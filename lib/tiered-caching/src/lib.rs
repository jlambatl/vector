//! Tiered caching library integration for Vector.
//!
//! Provides a pluggable, multi-tier cache abstraction with support for multiple backends
//! (Redis, Memcache) and a fast in-memory L1 tier.
//!
//! # Architecture
//!
//! - **L1 Tier**: In-memory cache - sub-millisecond latency
//! - **L2 Tier**: Distributed cache (Redis/Memcache) - persistent across instances
//! - **Compute-on-Miss**: Applications implement their own logic in VRL/transforms
//! - **No Invalidation (v1)**: TTL-based expiry only, no cross-instance invalidation

pub mod backends;
mod vrl_functions;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arc_swap::ArcSwap;
use tracing::warn;
use vrl::compiler::Function;
use vrl::value::Value;

pub use backends::memcache::MemcacheCacheConfig;
pub use backends::redis::RedisCacheConfig;
pub use backends::{MemcacheCacheTable, RedisCacheTable};
pub use multi_tier_cache::CacheStrategy;

pub(crate) fn map_ttl_to_strategy(ttl_secs: u64) -> CacheStrategy {
    if ttl_secs <= 10 {
        CacheStrategy::RealTime
    } else if ttl_secs <= 300 {
        CacheStrategy::ShortTerm
    } else if ttl_secs <= 3600 {
        CacheStrategy::MediumTerm
    } else if ttl_secs <= 10_800 {
        CacheStrategy::LongTerm
    } else {
        CacheStrategy::Custom(Duration::from_secs(ttl_secs))
    }
}

/// Result type for cache operations.
pub type CacheResult<T> = Result<T, CacheError>;

/// Errors that can occur in cache operations.
#[derive(Debug, Clone, PartialEq)]
pub enum CacheError {
    /// Cache not found with given name
    CacheNotFound(String),
    /// Connection error to backend
    ConnectionError(String),
    /// Serialization/deserialization error
    SerializationError(String),
    /// Key encoding error
    KeyEncodingError(String),
    /// Generic error with message
    Other(String),
}

impl std::fmt::Display for CacheError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheError::CacheNotFound(name) => write!(f, "Cache not found: {name}"),
            CacheError::ConnectionError(msg) => write!(f, "Connection error: {msg}"),
            CacheError::SerializationError(msg) => write!(f, "Serialization error: {msg}"),
            CacheError::KeyEncodingError(msg) => write!(f, "Key encoding error: {msg}"),
            CacheError::Other(msg) => write!(f, "Cache error: {msg}"),
        }
    }
}

impl std::error::Error for CacheError {}

/// Convert between `vrl::value::Value` and `serde_json::Value`.
///
/// The `multi-tier-cache` library operates on `serde_json::Value`, but Vector
/// standardizes on `vrl::value::Value`. These helpers handle the conversion.
pub(crate) fn vrl_to_json(value: &Value) -> CacheResult<serde_json::Value> {
    serde_json::to_value(value).map_err(|e| CacheError::SerializationError(e.to_string()))
}

pub(crate) fn json_to_vrl(value: serde_json::Value) -> CacheResult<Value> {
    Ok(Value::from(value))
}

/// Trait for cache backend implementations.
///
/// Implementers provide the actual caching mechanism (Redis, Memcache, etc.)
/// wrapped around the multi-tier-cache library. All values use `vrl::value::Value`
/// for direct compatibility with Vector's event model and VRL functions.
#[async_trait::async_trait]
pub trait CacheTable: Send + Sync {
    /// Get a value from the cache by key.
    ///
    /// Returns `Ok(Some(value))` if key exists, `Ok(None)` if miss, or error if operation fails.
    async fn get(&self, key: &str) -> CacheResult<Option<Value>>;

    /// Set a value in the cache with optional TTL in seconds.
    async fn set(&self, key: &str, value: Value, ttl_secs: Option<u64>) -> CacheResult<()>;

    /// Remove a value from the cache by key.
    async fn remove(&self, key: &str) -> CacheResult<()>;

    /// Check if the cache backend is healthy.
    async fn health_check(&self) -> bool;

    /// Get the name of this cache implementation (for debugging/monitoring).
    fn name(&self) -> &str;
}

/// Thread-safe registry for cache instances.
///
/// Supports two phases:
/// 1. **Loading Phase**: `Mutex`-protected mutable access for initialization
/// 2. **Runtime Phase**: Lock-free reads via `ArcSwap` after `finish_load()`
pub struct CacheRegistry {
    tables: Mutex<HashMap<String, Arc<dyn CacheTable>>>,
    readonly: ArcSwap<HashMap<String, Arc<dyn CacheTable>>>,
}

impl CacheRegistry {
    /// Create a new cache registry.
    pub fn new() -> Self {
        Self {
            tables: Mutex::new(HashMap::new()),
            readonly: ArcSwap::new(Arc::new(HashMap::new())),
        }
    }

    /// Load caches into the registry during initialization.
    ///
    /// This is called during the loading phase (before `finish_load()`).
    pub fn load(&self, caches: HashMap<String, Arc<dyn CacheTable>>) {
        let mut guard = self.tables.lock().unwrap_or_else(|poison| {
            warn!(message = "Cache registry mutex was poisoned, recovering.");
            poison.into_inner()
        });
        *guard = caches;
    }

    /// Transition from loading phase to runtime phase.
    ///
    /// After this is called, the registry switches to lock-free read mode.
    /// The mutable `Mutex` is swapped into read-only `ArcSwap`.
    pub fn finish_load(&self) {
        let guard = self.tables.lock().unwrap_or_else(|poison| {
            warn!(message = "Cache registry mutex was poisoned, recovering.");
            poison.into_inner()
        });
        let caches = guard.clone();
        self.readonly.store(Arc::new(caches));
    }

    /// Get list of all cache instance names.
    pub fn cache_ids(&self) -> Vec<String> {
        self.readonly.load().keys().cloned().collect()
    }

    /// Get a read-only search interface for accessing caches.
    pub fn as_readonly(&self) -> CacheSearch {
        CacheSearch {
            tables: self.readonly.load().clone(),
        }
    }
}

impl Default for CacheRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Read-only view for accessing caches at runtime.
///
/// Provides lock-free access to the cache registry after `finish_load()` has been called.
#[derive(Clone, Debug)]
pub struct CacheSearch {
    tables: Arc<HashMap<String, Arc<dyn CacheTable>>>,
}

impl CacheSearch {
    /// Get a cache instance by name.
    pub fn find_cache(&self, name: &str) -> CacheResult<CacheRecordRef> {
        if let Some(table) = self.tables.get(name) {
            Ok(CacheRecordRef {
                table: Arc::clone(table),
            })
        } else {
            Err(CacheError::CacheNotFound(name.to_string()))
        }
    }
}

impl std::fmt::Debug for dyn CacheTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheTable")
            .field("name", &self.name())
            .finish()
    }
}

/// Reference to a cache instance for performing operations.
pub struct CacheRecordRef {
    table: Arc<dyn CacheTable>,
}

impl std::fmt::Debug for CacheRecordRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheRecordRef")
            .field("name", &self.table.name())
            .finish()
    }
}

impl CacheRecordRef {
    /// Get a value from the cache.
    pub async fn get(&self, key: &str) -> CacheResult<Option<Value>> {
        self.table.get(key).await
    }

    /// Set a value in the cache.
    pub async fn set(&self, key: &str, value: Value, ttl_secs: Option<u64>) -> CacheResult<()> {
        self.table.set(key, value, ttl_secs).await
    }

    /// Remove a value from the cache.
    pub async fn remove(&self, key: &str) -> CacheResult<()> {
        self.table.remove(key).await
    }

    /// Check cache health.
    pub async fn health_check(&self) -> bool {
        self.table.health_check().await
    }

    /// Get cache name for debugging.
    pub fn name(&self) -> &str {
        self.table.name()
    }
}

/// Register VRL functions for cache access.
///
/// Returns VRL functions for `cache_get`, `cache_set`, and `cache_remove`.
pub fn vrl_functions() -> Vec<Box<dyn Function>> {
    vec![
        Box::new(vrl_functions::CacheGet) as _,
        Box::new(vrl_functions::CacheSet) as _,
        Box::new(vrl_functions::CacheRemove) as _,
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct MockCache {
        name: String,
        data: Arc<std::sync::Mutex<HashMap<String, Value>>>,
    }

    #[async_trait::async_trait]
    impl CacheTable for MockCache {
        async fn get(&self, key: &str) -> CacheResult<Option<Value>> {
            let data = self.data.lock().unwrap();
            Ok(data.get(key).cloned())
        }

        async fn set(&self, key: &str, value: Value, _ttl_secs: Option<u64>) -> CacheResult<()> {
            let mut data = self.data.lock().unwrap();
            data.insert(key.to_string(), value);
            Ok(())
        }

        async fn remove(&self, key: &str) -> CacheResult<()> {
            let mut data = self.data.lock().unwrap();
            data.remove(key);
            Ok(())
        }

        async fn health_check(&self) -> bool {
            true
        }

        fn name(&self) -> &str {
            &self.name
        }
    }

    #[test]
    fn test_registry_loading_and_readonly() {
        let registry = CacheRegistry::new();

        let mock = Arc::new(MockCache {
            name: "test_cache".to_string(),
            data: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }) as Arc<dyn CacheTable>;

        let mut caches = HashMap::new();
        caches.insert("test".to_string(), mock);

        registry.load(caches);
        registry.finish_load();

        let ids = registry.cache_ids();
        assert_eq!(ids.len(), 1);
        assert!(ids.contains(&"test".to_string()));
    }

    #[test]
    fn test_cache_not_found() {
        let registry = CacheRegistry::new();
        registry.finish_load();

        let search = registry.as_readonly();
        let result = search.find_cache("nonexistent");
        assert!(matches!(result, Err(CacheError::CacheNotFound(_))));
    }

    #[tokio::test]
    async fn test_cache_get_set() {
        let registry = CacheRegistry::new();

        let mock = Arc::new(MockCache {
            name: "test_cache".to_string(),
            data: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }) as Arc<dyn CacheTable>;

        let mut caches = HashMap::new();
        caches.insert("test".to_string(), mock);

        registry.load(caches);
        registry.finish_load();

        let search = registry.as_readonly();
        let cache_ref = search.find_cache("test").unwrap();

        let result = cache_ref.get("mykey").await.unwrap();
        assert!(result.is_none()); // Initially none
        assert_eq!(cache_ref.name(), "test_cache");
    }

    #[tokio::test]
    async fn test_cache_set_and_get() {
        let registry = CacheRegistry::new();

        let mock = Arc::new(MockCache {
            name: "test_cache".to_string(),
            data: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }) as Arc<dyn CacheTable>;

        let mut caches = HashMap::new();
        caches.insert("test".to_string(), mock);

        registry.load(caches);
        registry.finish_load();

        let search = registry.as_readonly();
        let cache_ref = search.find_cache("test").unwrap();

        // Test set with VRL Value
        cache_ref
            .set("key1", Value::from("value1"), None)
            .await
            .unwrap();

        // Test get returns VRL Value
        let result = cache_ref.get("key1").await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap(), Value::from("value1"));
    }

    #[tokio::test]
    async fn test_cache_remove() {
        let registry = CacheRegistry::new();

        let mock = Arc::new(MockCache {
            name: "test_cache".to_string(),
            data: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }) as Arc<dyn CacheTable>;

        let mut caches = HashMap::new();
        caches.insert("test".to_string(), mock);

        registry.load(caches);
        registry.finish_load();

        let search = registry.as_readonly();
        let cache_ref = search.find_cache("test").unwrap();

        // Set a value
        cache_ref
            .set("key1", Value::from("value1"), None)
            .await
            .unwrap();

        // Remove it
        cache_ref.remove("key1").await.unwrap();

        // Get should return None
        let result = cache_ref.get("key1").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_multiple_caches_in_registry() {
        let registry = CacheRegistry::new();

        let mock1 = Arc::new(MockCache {
            name: "cache_one".to_string(),
            data: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }) as Arc<dyn CacheTable>;

        let mock2 = Arc::new(MockCache {
            name: "cache_two".to_string(),
            data: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }) as Arc<dyn CacheTable>;

        let mut caches = HashMap::new();
        caches.insert("one".to_string(), mock1);
        caches.insert("two".to_string(), mock2);

        registry.load(caches);
        registry.finish_load();

        let ids = registry.cache_ids();
        assert_eq!(ids.len(), 2);

        let search = registry.as_readonly();

        // Set different values in different caches
        let cache1 = search.find_cache("one").unwrap();
        let cache2 = search.find_cache("two").unwrap();

        cache1
            .set("key", Value::from("from_one"), None)
            .await
            .unwrap();
        cache2
            .set("key", Value::from("from_two"), None)
            .await
            .unwrap();

        // Each cache has its own data
        assert_eq!(
            cache1.get("key").await.unwrap(),
            Some(Value::from("from_one"))
        );
        assert_eq!(
            cache2.get("key").await.unwrap(),
            Some(Value::from("from_two"))
        );

        // Cache names are correct
        assert_eq!(cache1.name(), "cache_one");
        assert_eq!(cache2.name(), "cache_two");
    }

    #[tokio::test]
    async fn test_cache_search_clone() {
        let registry = CacheRegistry::new();

        let mock = Arc::new(MockCache {
            name: "test_cache".to_string(),
            data: Arc::new(std::sync::Mutex::new(HashMap::new())),
        }) as Arc<dyn CacheTable>;

        let mut caches = HashMap::new();
        caches.insert("test".to_string(), mock);

        registry.load(caches);
        registry.finish_load();

        let search1 = registry.as_readonly();
        let search2 = search1.clone();

        // Set via search1
        let cache1 = search1.find_cache("test").unwrap();
        cache1.set("key", Value::from("hello"), None).await.unwrap();

        // Get via search2 - should see the same data
        let cache2 = search2.find_cache("test").unwrap();
        assert_eq!(cache2.get("key").await.unwrap(), Some(Value::from("hello")));
    }

    #[test]
    fn test_cache_error_display() {
        assert_eq!(
            CacheError::CacheNotFound("my_cache".into()).to_string(),
            "Cache not found: my_cache"
        );
        assert_eq!(
            CacheError::ConnectionError("refused".into()).to_string(),
            "Connection error: refused"
        );
        assert_eq!(
            CacheError::SerializationError("bad data".into()).to_string(),
            "Serialization error: bad data"
        );
    }

    #[test]
    fn test_cache_error_equality() {
        assert_eq!(
            CacheError::CacheNotFound("a".into()),
            CacheError::CacheNotFound("a".into())
        );
        assert_ne!(
            CacheError::CacheNotFound("a".into()),
            CacheError::CacheNotFound("b".into())
        );
    }

    #[test]
    fn test_map_ttl_to_strategy() {
        use multi_tier_cache::CacheStrategy;

        assert!(matches!(map_ttl_to_strategy(5), CacheStrategy::RealTime));
        assert!(matches!(map_ttl_to_strategy(10), CacheStrategy::RealTime));
        assert!(matches!(map_ttl_to_strategy(60), CacheStrategy::ShortTerm));
        assert!(matches!(map_ttl_to_strategy(300), CacheStrategy::ShortTerm));
        assert!(matches!(
            map_ttl_to_strategy(600),
            CacheStrategy::MediumTerm
        ));
        assert!(matches!(
            map_ttl_to_strategy(3600),
            CacheStrategy::MediumTerm
        ));
        assert!(matches!(map_ttl_to_strategy(7200), CacheStrategy::LongTerm));
        assert!(matches!(
            map_ttl_to_strategy(10800),
            CacheStrategy::LongTerm
        ));
        assert!(matches!(
            map_ttl_to_strategy(86400),
            CacheStrategy::Custom(_)
        ));
    }

    #[test]
    fn test_vrl_json_conversion() {
        // VRL to JSON
        let vrl_val = Value::from("hello");
        let json_val = vrl_to_json(&vrl_val).unwrap();
        assert_eq!(json_val, serde_json::json!("hello"));

        // JSON to VRL
        let json_val = serde_json::json!({"key": "value", "num": 42});
        let vrl_val = json_to_vrl(json_val).unwrap();
        assert!(vrl_val.is_object());
    }
}
