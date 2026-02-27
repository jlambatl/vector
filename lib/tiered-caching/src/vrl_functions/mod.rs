//! VRL functions for cache access.
//!
//! Provides `cache_get`, `cache_set`, and `cache_remove` VRL functions that allow
//! users to interact with configured caches from within VRL transforms.

mod cache_get;
mod cache_remove;
mod cache_set;

pub use cache_get::CacheGet;
pub use cache_remove::CacheRemove;
pub use cache_set::CacheSet;
