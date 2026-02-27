//! Cache backend implementations.
//!
//! Provides concrete implementations of `CacheTable` that wrap the `multi-tier-cache` library
//! with different L2 backends (Redis, Memcache).

pub mod memcache;
pub mod redis;

pub use memcache::MemcacheCacheTable;
pub use redis::RedisCacheTable;
