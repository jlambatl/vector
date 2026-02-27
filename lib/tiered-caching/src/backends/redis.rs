//! Redis-backed cache table implementation.

use std::sync::Arc;
use std::time::Duration;

use multi_tier_cache::{
    CacheBackend, CacheManager, CacheStrategy, MokaCache, MokaCacheConfig, RedisCache,
};
use vrl::value::Value;

use crate::{CacheError, CacheResult, CacheTable, json_to_vrl, map_ttl_to_strategy, vrl_to_json};

/// Configuration for a Redis-backed cache.
#[derive(Clone, Debug)]
pub struct RedisCacheConfig {
    /// Redis URL (e.g., "redis://localhost:6379")
    pub redis_url: String,
    /// L1 in-memory cache capacity (number of entries)
    pub l1_capacity: u64,
    /// L1 in-memory cache time-to-live
    pub l1_ttl: Duration,
    /// Default cache strategy (TTL for new entries)
    pub cache_strategy: CacheStrategy,
}

impl Default for RedisCacheConfig {
    fn default() -> Self {
        Self {
            redis_url: "redis://localhost:6379".to_string(),
            l1_capacity: 10_000,
            l1_ttl: Duration::from_secs(300), // 5 minutes
            cache_strategy: CacheStrategy::MediumTerm,
        }
    }
}

/// Redis cache table wrapping multi-tier-cache.
///
/// Data operations (`get`, `set`, `remove`) are delegated to the `CacheManager`
/// which handles L1↔L2 coordination including read-through, write-through,
/// and invalidation across tiers. Health checks use the underlying backends
/// directly since `CacheManager` does not expose a health check API.
#[derive(Clone)]
pub struct RedisCacheTable {
    cache: Arc<CacheManager>,
    l1_cache: Arc<MokaCache>,
    l2_cache: Arc<RedisCache>,
    strategy: CacheStrategy,
}

impl RedisCacheTable {
    /// Create a new Redis-backed cache table.
    ///
    /// # Errors
    ///
    /// Returns `CacheError::ConnectionError` if the Redis URL is invalid or
    /// connection to Redis fails.
    pub async fn new(config: RedisCacheConfig) -> CacheResult<Self> {
        if !config.redis_url.starts_with("redis://") && !config.redis_url.starts_with("rediss://") {
            return Err(CacheError::ConnectionError(
                "Redis URL must start with redis:// or rediss://".to_string(),
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
            RedisCache::with_url(&config.redis_url)
                .await
                .map_err(|e| CacheError::ConnectionError(e.to_string()))?,
        );

        let cache = Arc::new(
            CacheManager::new(Arc::clone(&l1_cache), Arc::clone(&l2_cache))
                .await
                .map_err(|e| CacheError::Other(e.to_string()))?,
        );

        Ok(Self {
            cache,
            l1_cache,
            l2_cache,
            strategy: config.cache_strategy,
        })
    }
}

#[async_trait::async_trait]
impl CacheTable for RedisCacheTable {
    async fn get(&self, key: &str) -> CacheResult<Option<Value>> {
        let result = self
            .cache
            .get(key)
            .await
            .map_err(|e| CacheError::Other(e.to_string()))?;

        match result {
            Some(json_val) => Ok(Some(json_to_vrl(json_val)?)),
            None => Ok(None),
        }
    }

    async fn set(&self, key: &str, value: Value, ttl_secs: Option<u64>) -> CacheResult<()> {
        let strategy = ttl_secs
            .map(map_ttl_to_strategy)
            .unwrap_or_else(|| self.strategy.clone());

        let json_val = vrl_to_json(&value)?;
        self.cache
            .set_with_strategy(key, json_val, strategy)
            .await
            .map_err(|e| CacheError::Other(e.to_string()))
    }

    async fn remove(&self, key: &str) -> CacheResult<()> {
        self.cache
            .invalidate(key)
            .await
            .map_err(|e| CacheError::Other(e.to_string()))
    }

    async fn health_check(&self) -> bool {
        let l1_ok = self.l1_cache.health_check().await;
        let l2_ok = self.l2_cache.health_check().await;
        l1_ok && l2_ok
    }

    fn name(&self) -> &str {
        "redis"
    }
}
