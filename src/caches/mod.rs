//! Cache instances configuration.
//!
//! Provides pluggable cache backends (Redis, Memcache) that can be accessed from transforms
//! and other components via VRL functions (`cache_get`, `cache_set`, `cache_remove`).

use std::time::Duration;

use vector_lib::configurable::configurable_component;

use crate::config::GenerateConfig;

pub mod strategy;

pub use strategy::CacheStrategyConfig;

/// The type of cache backend to use.
#[configurable_component]
#[derive(Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub enum CacheBackendType {
    /// Redis-backed distributed cache with in-memory L1 and Redis L2 tiers.
    Redis,
    /// Memcache-backed distributed cache with in-memory L1 and Memcache L2 tiers.
    Memcache,
}

/// Configuration for a cache instance.
///
/// Caches provide multi-tier caching with a fast in-memory L1 layer
/// and a distributed L2 backend (Redis or Memcache). Caches are accessed
/// from VRL using the `cache_get`, `cache_set`, and `cache_remove` functions.
#[configurable_component(global_option("caches"))]
#[derive(Clone, Debug)]
pub struct CacheInstanceConfig {
    /// The cache backend type.
    #[serde(rename = "type")]
    pub backend: CacheBackendType,

    /// Backend connection URL.
    ///
    /// The format depends on the selected backend type:
    ///
    /// For Redis, the URL format is `redis://[username:password@]host[:port]/[database]`.
    /// For Memcache, provide one or more server addresses, comma-separated.
    ///
    /// Redis examples:
    /// - `redis://localhost:6379` - local Redis without authentication
    /// - `redis://:password@localhost:6379` - local Redis with password
    /// - `rediss://redis.example.com:6380` - Redis with TLS/SSL
    ///
    /// Memcache examples:
    /// - `localhost:11211` - single local Memcache server
    /// - `server1:11211,server2:11211` - multiple Memcache servers
    #[serde(default = "default_url")]
    pub url: String,

    /// L1 in-memory cache capacity in number of entries.
    ///
    /// Determines the maximum number of entries kept in the fast in-memory layer.
    #[serde(default = "default_l1_capacity")]
    pub l1_capacity: u64,

    /// L1 in-memory cache time-to-live for entries in seconds.
    ///
    /// Duration before entries in the fast in-memory layer expire.
    #[serde(default = "default_l1_ttl")]
    pub l1_ttl_secs: u64,

    /// Default cache strategy controlling TTL behavior.
    ///
    /// Determines the TTL applied to entries stored via `cache_set` when no
    /// explicit `ttl_secs` is provided.
    #[serde(default)]
    pub cache_strategy: CacheStrategyConfig,
}

impl CacheInstanceConfig {
    /// Builds the cache backend from this configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache backend fails to initialize (e.g.,
    /// connection failure, invalid URL).
    pub async fn build(&self) -> crate::Result<Box<dyn tiered_caching::CacheTable>> {
        let ttl_duration = Duration::from_secs(self.l1_ttl_secs);
        let strategy: tiered_caching::CacheStrategy = self.cache_strategy.clone().into();

        match &self.backend {
            CacheBackendType::Redis => {
                let config = tiered_caching::RedisCacheConfig {
                    redis_url: self.url.clone(),
                    l1_capacity: self.l1_capacity,
                    l1_ttl: ttl_duration,
                    cache_strategy: strategy,
                };

                let table = tiered_caching::RedisCacheTable::new(config)
                    .await
                    .map_err(|e| {
                        Box::new(std::io::Error::other(e.to_string()))
                            as Box<dyn std::error::Error + Send + Sync>
                    })?;

                Ok(Box::new(table) as Box<dyn tiered_caching::CacheTable>)
            }
            CacheBackendType::Memcache => {
                let config = tiered_caching::MemcacheCacheConfig {
                    servers: self.url.clone(),
                    l1_capacity: self.l1_capacity,
                    l1_ttl: ttl_duration,
                    cache_strategy: strategy,
                };

                let table = tiered_caching::MemcacheCacheTable::new(config)
                    .await
                    .map_err(|e| {
                        Box::new(std::io::Error::other(e.to_string()))
                            as Box<dyn std::error::Error + Send + Sync>
                    })?;

                Ok(Box::new(table) as Box<dyn tiered_caching::CacheTable>)
            }
        }
    }
}

impl Default for CacheInstanceConfig {
    fn default() -> Self {
        Self {
            backend: CacheBackendType::Redis,
            url: default_url(),
            l1_capacity: default_l1_capacity(),
            l1_ttl_secs: default_l1_ttl(),
            cache_strategy: CacheStrategyConfig::default(),
        }
    }
}

impl GenerateConfig for CacheInstanceConfig {
    fn generate_config() -> toml::Value {
        toml::Value::try_from(Self::default())
            .unwrap_or_else(|_| toml::Value::Table(toml::map::Map::new()))
    }
}

const fn default_url() -> String {
    String::new()
}

const DEFAULT_L1_CAPACITY: u64 = 10_000;

const DEFAULT_L1_TTL: u64 = 3600; // 1 hour

const fn default_l1_capacity() -> u64 {
    DEFAULT_L1_CAPACITY
}

const fn default_l1_ttl() -> u64 {
    DEFAULT_L1_TTL
}
