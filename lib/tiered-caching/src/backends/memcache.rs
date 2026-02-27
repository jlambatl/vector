//! Memcache-backed cache table implementation.
//!
//! Unlike the Redis backend, `MemcachedCache` does not implement the `L2CacheBackend`
//! trait (because Memcache does not support TTL introspection), so it cannot be used
//! with `CacheManager`. This backend manually coordinates L1 (in-memory) and L2
//! (Memcache) tiers.

use std::sync::Arc;
use std::time::Duration;

use multi_tier_cache::{CacheBackend, CacheStrategy, MemcachedCache, MokaCache, MokaCacheConfig};
use tracing::warn;
use vrl::value::Value;

use crate::{CacheError, CacheResult, CacheTable, json_to_vrl, map_ttl_to_strategy, vrl_to_json};

/// Global mutex to serialize Memcache cache construction.
///
/// `MemcachedCache::new()` reads from the `MEMCACHED_URL` environment variable.
/// Since environment variable manipulation is not thread-safe, we serialize
/// construction behind this mutex.
static MEMCACHE_INIT_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Configuration for a Memcache-backed cache.
#[derive(Clone, Debug)]
pub struct MemcacheCacheConfig {
    /// Memcache server addresses (e.g., "localhost:11211" or "server1:11211,server2:11211")
    pub servers: String,
    /// L1 in-memory cache capacity (number of entries)
    pub l1_capacity: u64,
    /// L1 in-memory cache time-to-live
    pub l1_ttl: Duration,
    /// Default cache strategy (TTL for new entries)
    pub cache_strategy: CacheStrategy,
}

impl Default for MemcacheCacheConfig {
    fn default() -> Self {
        Self {
            servers: "localhost:11211".to_string(),
            l1_capacity: 10_000,
            l1_ttl: Duration::from_secs(300), // 5 minutes
            cache_strategy: CacheStrategy::MediumTerm,
        }
    }
}

/// Memcache cache table with manual L1↔L2 coordination.
///
/// `MemcachedCache` does not implement `L2CacheBackend` (Memcache lacks TTL
/// introspection), so this backend cannot use `CacheManager`. Instead, it
/// manually coordinates the in-memory L1 and Memcache L2 tiers:
///
/// - **get**: Check L1 first; on miss, check L2 and backfill L1 on hit.
/// - **set**: Write to both L1 and L2.
/// - **remove**: Invalidate from both L1 and L2.
///
/// The `CacheBackend::get()` method on `MemcachedCache` returns `Option` (not
/// `Result`), so L2 connection errors are treated as cache misses. A warning is
/// logged when L2 appears unhealthy so operators can detect backend problems.
#[derive(Clone)]
pub struct MemcacheCacheTable {
    l1_cache: Arc<MokaCache>,
    l2_cache: Arc<MemcachedCache>,
    strategy: CacheStrategy,
}

impl MemcacheCacheTable {
    /// Create a new Memcache-backed cache table.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::ConnectionError` if no servers are configured or if
    /// the Memcache client fails to initialize.
    ///
    /// # Safety
    ///
    /// This method temporarily sets the `MEMCACHED_URL` environment variable
    /// because `MemcachedCache::new()` only supports configuration via env var.
    /// A global mutex serializes construction to prevent race conditions.
    pub async fn new(config: MemcacheCacheConfig) -> CacheResult<Self> {
        if config.servers.is_empty() {
            return Err(CacheError::ConnectionError(
                "No memcache servers configured".to_string(),
            ));
        }

        let moka_config = MokaCacheConfig {
            max_capacity: config.l1_capacity,
            time_to_live: config.l1_ttl,
            time_to_idle: config.l1_ttl,
        };

        let l1_cache =
            Arc::new(MokaCache::new(moka_config).map_err(|e| CacheError::Other(e.to_string()))?);

        // Serialize Memcache construction to avoid env var race conditions.
        let l2_cache = {
            let _guard = MEMCACHE_INIT_MUTEX
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());

            let previous_url = std::env::var("MEMCACHED_URL").ok();

            // SAFETY: Protected by MEMCACHE_INIT_MUTEX to prevent concurrent env var access.
            unsafe {
                std::env::set_var("MEMCACHED_URL", &config.servers);
            }

            let result =
                MemcachedCache::new().map_err(|e| CacheError::ConnectionError(e.to_string()));

            // Restore previous env var state.
            unsafe {
                if let Some(value) = previous_url {
                    std::env::set_var("MEMCACHED_URL", value);
                } else {
                    std::env::remove_var("MEMCACHED_URL");
                }
            }

            Arc::new(result?)
        };

        Ok(Self {
            l1_cache,
            l2_cache,
            strategy: config.cache_strategy,
        })
    }
}

#[async_trait::async_trait]
impl CacheTable for MemcacheCacheTable {
    async fn get(&self, key: &str) -> CacheResult<Option<Value>> {
        // Check L1 first (fast path).
        if let Some(json_val) = self.l1_cache.get(key).await {
            return Ok(Some(json_to_vrl(json_val)?));
        }

        // L1 miss — check L2. Note: `CacheBackend::get()` on MemcachedCache returns
        // `Option<Value>`, not `Result`. Connection errors are treated as misses.
        // We check health to log warnings about backend problems.
        let value = self.l2_cache.get(key).await;
        if value.is_none() && !self.l2_cache.health_check().await {
            warn!(message = "Memcache L2 backend may be unavailable.", key = %key);
        }

        if let Some(ref json_val) = value {
            // Backfill L1 on L2 hit.
            let ttl = self.strategy.to_duration();
            let _ = self
                .l1_cache
                .set_with_ttl(key, json_val.clone(), ttl)
                .await;
        }

        match value {
            Some(json_val) => Ok(Some(json_to_vrl(json_val)?)),
            None => Ok(None),
        }
    }

    async fn set(&self, key: &str, value: Value, ttl_secs: Option<u64>) -> CacheResult<()> {
        let strategy = ttl_secs
            .map(map_ttl_to_strategy)
            .unwrap_or_else(|| self.strategy.clone());
        let ttl = strategy.to_duration();

        let json_val = vrl_to_json(&value)?;
        self.l1_cache
            .set_with_ttl(key, json_val.clone(), ttl)
            .await
            .map_err(|e| CacheError::Other(e.to_string()))?;
        self.l2_cache
            .set_with_ttl(key, json_val, ttl)
            .await
            .map_err(|e| CacheError::Other(e.to_string()))?;

        Ok(())
    }

    async fn remove(&self, key: &str) -> CacheResult<()> {
        self.l1_cache
            .remove(key)
            .await
            .map_err(|e| CacheError::Other(e.to_string()))?;
        self.l2_cache
            .remove(key)
            .await
            .map_err(|e| CacheError::Other(e.to_string()))?;

        Ok(())
    }

    async fn health_check(&self) -> bool {
        let l1_ok = self.l1_cache.health_check().await;
        let l2_ok = self.l2_cache.health_check().await;
        l1_ok && l2_ok
    }

    fn name(&self) -> &str {
        "memcache"
    }
}
