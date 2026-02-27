//! VRL function: `cache_get`
//!
//! Retrieves a value from a named cache by key.

use std::sync::LazyLock;

use vrl::diagnostic::Label;
use vrl::prelude::*;

use crate::CacheSearch;

static PARAMETERS: LazyLock<Vec<Parameter>> = LazyLock::new(|| {
    vec![
        Parameter::required(
            "cache",
            kind::BYTES,
            "The name of the cache to retrieve the value from.",
        ),
        Parameter::required("key", kind::BYTES, "The key to look up in the cache."),
    ]
});

/// VRL function to get a value from a named cache.
///
/// # Usage
///
/// ```vrl
/// result = cache_get!("my_cache", "request_id_123")
/// ```
#[derive(Clone, Copy, Debug)]
pub struct CacheGet;

impl Function for CacheGet {
    fn identifier(&self) -> &'static str {
        "cache_get"
    }

    fn usage(&self) -> &'static str {
        "Retrieves a value from a named cache by key. Returns the cached value if found, or null if the key is not present."
    }

    fn internal_failure_reasons(&self) -> &'static [&'static str] {
        &[
            "The named cache does not exist.",
            "A connection error occurred with the cache backend.",
        ]
    }

    fn return_kind(&self) -> u16 {
        kind::ANY
    }

    fn parameters(&self) -> &'static [Parameter] {
        &PARAMETERS
    }

    fn examples(&self) -> &'static [Example] {
        &[
            example! {
                title: "Cache hit",
                source: r#"cache_get!("my_cache", "user_123")"#,
                result: Ok(r#"{"name": "Alice"}"#),
            },
            example! {
                title: "Cache miss",
                source: r#"cache_get!("my_cache", "nonexistent")"#,
                result: Ok("null"),
            },
        ]
    }

    fn category(&self) -> &'static str {
        "cache"
    }

    fn compile(
        &self,
        _state: &TypeState,
        ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        let caches = ctx
            .get_external_context_mut::<CacheSearch>()
            .ok_or(Box::new(CacheVrlError::CachesNotLoaded) as Box<dyn DiagnosticMessage>)?
            .clone();

        let cache = arguments.required("cache");
        let key = arguments.required("key");

        Ok(CacheGetFn { cache, key, caches }.as_expr())
    }
}

#[derive(Debug, Clone)]
struct CacheGetFn {
    cache: Box<dyn Expression>,
    key: Box<dyn Expression>,
    caches: CacheSearch,
}

impl FunctionExpression for CacheGetFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let cache_name = self.cache.resolve(ctx)?;
        let cache_name = cache_name
            .try_bytes_utf8_lossy()
            .map_err(|_| ExpressionError::from("cache name must be a valid UTF-8 string"))?;

        let key = self.key.resolve(ctx)?;
        let key = key
            .try_bytes_utf8_lossy()
            .map_err(|_| ExpressionError::from("cache key must be a valid UTF-8 string"))?;

        let cache_ref = self
            .caches
            .find_cache(&cache_name)
            .map_err(|e| ExpressionError::from(e.to_string()))?;

        // Use block_on for synchronous VRL execution.
        // The tiered-caching library handles L1 (Moka, in-process) and L2 (Redis/Memcache)
        // coordination. L1 hits are fast and do not actually block.
        let result = futures::executor::block_on(cache_ref.get(&key))
            .map_err(|e| ExpressionError::from(e.to_string()))?;

        Ok(result.unwrap_or(Value::Null))
    }

    fn type_def(&self, _: &TypeState) -> TypeDef {
        TypeDef::any().fallible()
    }
}

#[derive(Debug)]
pub(crate) struct CacheVrlError {
    variant: &'static str,
}

impl CacheVrlError {
    #[allow(non_upper_case_globals)]
    pub(crate) const CachesNotLoaded: Self = Self {
        variant: "CachesNotLoaded",
    };
}

impl std::fmt::Display for CacheVrlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.variant {
            "CachesNotLoaded" => write!(
                f,
                "Cache tables are not loaded. This is an internal Vector error."
            ),
            _ => write!(f, "Unknown cache VRL error."),
        }
    }
}

impl std::error::Error for CacheVrlError {}

impl DiagnosticMessage for CacheVrlError {
    fn code(&self) -> usize {
        901
    }

    fn message(&self) -> String {
        self.to_string()
    }

    fn labels(&self) -> Vec<Label> {
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use vrl::{
        compiler::{TargetValue, prelude::TimeZone, state::RuntimeState},
        value,
        value::Secrets,
    };

    use super::*;
    use crate::{CacheRegistry, CacheResult, CacheTable};

    #[derive(Clone)]
    struct MockCache {
        data: Arc<std::sync::Mutex<HashMap<String, Value>>>,
    }

    #[async_trait::async_trait]
    impl CacheTable for MockCache {
        async fn get(&self, key: &str) -> CacheResult<Option<Value>> {
            let data = self.data.lock().unwrap();
            Ok(data.get(key).cloned())
        }
        async fn set(&self, key: &str, value: Value, _ttl: Option<u64>) -> CacheResult<()> {
            self.data.lock().unwrap().insert(key.to_string(), value);
            Ok(())
        }
        async fn remove(&self, key: &str) -> CacheResult<()> {
            self.data.lock().unwrap().remove(key);
            Ok(())
        }
        async fn health_check(&self) -> bool {
            true
        }
        fn name(&self) -> &str {
            "mock"
        }
    }

    fn setup_cache_search() -> CacheSearch {
        let registry = CacheRegistry::new();
        let mut data = HashMap::new();
        data.insert("existing_key".to_string(), Value::from("cached_value"));

        let mock = Arc::new(MockCache {
            data: Arc::new(std::sync::Mutex::new(data)),
        }) as Arc<dyn CacheTable>;

        let mut caches = HashMap::new();
        caches.insert("test_cache".to_string(), mock);
        registry.load(caches);
        registry.finish_load();
        registry.as_readonly()
    }

    #[test]
    fn cache_get_hit() {
        let caches = setup_cache_search();
        let func = CacheGetFn {
            cache: Box::new(expression::Literal::from("test_cache")),
            key: Box::new(expression::Literal::from("existing_key")),
            caches,
        };

        let tz = TimeZone::default();
        let mut target = TargetValue {
            value: value!({}),
            metadata: value!({}),
            secrets: Secrets::new(),
        };
        let mut runtime_state = RuntimeState::default();
        let mut ctx = Context::new(&mut target, &mut runtime_state, &tz);

        let result = func.resolve(&mut ctx).unwrap();
        assert_eq!(result, Value::from("cached_value"));
    }

    #[test]
    fn cache_get_miss() {
        let caches = setup_cache_search();
        let func = CacheGetFn {
            cache: Box::new(expression::Literal::from("test_cache")),
            key: Box::new(expression::Literal::from("nonexistent_key")),
            caches,
        };

        let tz = TimeZone::default();
        let mut target = TargetValue {
            value: value!({}),
            metadata: value!({}),
            secrets: Secrets::new(),
        };
        let mut runtime_state = RuntimeState::default();
        let mut ctx = Context::new(&mut target, &mut runtime_state, &tz);

        let result = func.resolve(&mut ctx).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn cache_get_not_found() {
        let caches = setup_cache_search();
        let func = CacheGetFn {
            cache: Box::new(expression::Literal::from("no_such_cache")),
            key: Box::new(expression::Literal::from("key")),
            caches,
        };

        let tz = TimeZone::default();
        let mut target = TargetValue {
            value: value!({}),
            metadata: value!({}),
            secrets: Secrets::new(),
        };
        let mut runtime_state = RuntimeState::default();
        let mut ctx = Context::new(&mut target, &mut runtime_state, &tz);

        let result = func.resolve(&mut ctx);
        assert!(result.is_err());
    }
}
