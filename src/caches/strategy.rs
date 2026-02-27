//! Cache strategy configuration shared across all cache backends.

use std::time::Duration;

use vector_lib::configurable::configurable_component;

use tiered_caching::CacheStrategy;

/// Cache strategy for controlling TTL behavior.
#[configurable_component]
#[derive(Clone, Debug, Default)]
#[serde(rename_all = "snake_case")]
pub enum CacheStrategyConfig {
    /// Real-time: TTL ≤ 10 seconds.
    RealTime,
    /// Short-term: TTL ≤ 5 minutes.
    ShortTerm,
    /// Medium-term: TTL ≤ 1 hour. This is the default.
    #[default]
    MediumTerm,
    /// Long-term: TTL ≤ 3 hours.
    LongTerm,
    /// Custom TTL in seconds.
    Custom {
        /// TTL duration in seconds.
        ttl_secs: u64,
    },
}

impl From<CacheStrategyConfig> for CacheStrategy {
    fn from(config: CacheStrategyConfig) -> Self {
        match config {
            CacheStrategyConfig::RealTime => CacheStrategy::RealTime,
            CacheStrategyConfig::ShortTerm => CacheStrategy::ShortTerm,
            CacheStrategyConfig::MediumTerm => CacheStrategy::MediumTerm,
            CacheStrategyConfig::LongTerm => CacheStrategy::LongTerm,
            CacheStrategyConfig::Custom { ttl_secs } => {
                CacheStrategy::Custom(Duration::from_secs(ttl_secs))
            }
        }
    }
}
