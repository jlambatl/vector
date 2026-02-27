use vector_lib::configurable::configurable_component;

use crate::caches::CacheInstanceConfig;

/// Fully resolved cache component.
///
/// Unlike enrichment tables, caches are not part of the data flow graph.
/// They are accessed on-demand from VRL functions.
///
/// On configuration reload, all caches are rebuilt. The in-memory L1 layer is
/// reset (warm data is lost), while L2 data (Redis/Memcache) persists on the
/// server.
#[configurable_component]
#[derive(Clone, Debug)]
pub struct CacheOuter {
    #[serde(flatten)]
    pub inner: CacheInstanceConfig,
}

impl CacheOuter {
    /// Create a new cache outer wrapper.
    pub const fn new(inner: CacheInstanceConfig) -> Self {
        Self { inner }
    }
}
