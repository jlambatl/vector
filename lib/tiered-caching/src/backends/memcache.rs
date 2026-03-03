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

        let l2_cache = Arc::new(
            MemcachedCache::with_url(&config.servers)
                .map_err(|e| CacheError::ConnectionError(e.to_string()))?,
        );

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
        if let Some(bytes) = self.l1_cache.get(key).await {
            let json_val: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|e| CacheError::Other(format!("L1 Deserialization error: {}", e)))?;
            return Ok(Some(json_to_vrl(json_val)?));
        }

        // L1 miss — check L2. Note: `CacheBackend::get()` on MemcachedCache returns
        // `Option<Value>`, not `Result`. Connection errors are treated as misses.
        // We check health to log warnings about backend problems.
        let value_bytes = self.l2_cache.get(key).await;
        if value_bytes.is_none() && !self.l2_cache.health_check().await {
            warn!(message = "Memcache L2 backend may be unavailable.", key = %key);
        }

        if let Some(ref bytes) = value_bytes {
            // Backfill L1 on L2 hit.
            let ttl = self.strategy.to_duration();
            let _ = self.l1_cache.set_with_ttl(key, bytes, ttl).await;
        }

        match value_bytes {
            Some(bytes) => {
                let json_val: serde_json::Value = serde_json::from_slice(&bytes)
                    .map_err(|e| CacheError::Other(format!("L2 Deserialization error: {}", e)))?;
                Ok(Some(json_to_vrl(json_val)?))
            }
            None => Ok(None),
        }
    }

    async fn set(&self, key: &str, value: Value, ttl_secs: Option<u64>) -> CacheResult<()> {
        let strategy = ttl_secs
            .map(map_ttl_to_strategy)
            .unwrap_or_else(|| self.strategy.clone());
        let ttl = strategy.to_duration();

        let json_val = vrl_to_json(&value)?;
        let bytes = serde_json::to_vec(&json_val)
            .map_err(|e| CacheError::Other(format!("Serialization error: {}", e)))?;

        self.l1_cache
            .set_with_ttl(key, &bytes, ttl)
            .await
            .map_err(|e| CacheError::Other(e.to_string()))?;
        self.l2_cache
            .set_with_ttl(key, &bytes, ttl)
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
