//! VRL function: `cache_set`
//!
//! Stores a value in a named cache by key with an optional TTL.

use std::sync::LazyLock;

use vrl::prelude::*;

use super::cache_get::CacheVrlError;
use crate::CacheSearch;

static PARAMETERS: LazyLock<Vec<Parameter>> = LazyLock::new(|| {
    vec![
        Parameter::required(
            "cache",
            kind::BYTES,
            "The name of the cache to store the value in.",
        ),
        Parameter::required(
            "key",
            kind::BYTES,
            "The key under which to store the value.",
        ),
        Parameter::required("value", kind::ANY, "The value to store in the cache."),
        Parameter::optional(
            "ttl_secs",
            kind::INTEGER,
            "Time-to-live in seconds for the cached entry. If not specified, the cache's default TTL is used.",
        ),
    ]
});

/// VRL function to set a value in a named cache.
///
/// # Usage
///
/// ```vrl
/// cache_set!("my_cache", "request_id_123", {"name": "Alice"}, ttl_secs: 300)
/// ```
#[derive(Clone, Copy, Debug)]
pub struct CacheSet;

impl Function for CacheSet {
    fn identifier(&self) -> &'static str {
        "cache_set"
    }

    fn usage(&self) -> &'static str {
        "Stores a value in a named cache by key, with an optional TTL in seconds."
    }

    fn internal_failure_reasons(&self) -> &'static [&'static str] {
        &[
            "The named cache does not exist.",
            "A connection error occurred with the cache backend.",
        ]
    }

    fn return_kind(&self) -> u16 {
        kind::NULL
    }

    fn parameters(&self) -> &'static [Parameter] {
        &PARAMETERS
    }

    fn examples(&self) -> &'static [Example] {
        &[
            example! {
                title: "Set a value",
                source: r#"cache_set!("my_cache", "user_123", {"name": "Alice"})"#,
                result: Ok("null"),
            },
            example! {
                title: "Set a value with TTL",
                source: r#"cache_set!("my_cache", "session_abc", "active", ttl_secs: 3600)"#,
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
        let value = arguments.required("value");
        let ttl_secs = arguments.optional("ttl_secs");

        Ok(CacheSetFn {
            cache,
            key,
            value,
            ttl_secs,
            caches,
        }
        .as_expr())
    }
}

#[derive(Debug, Clone)]
struct CacheSetFn {
    cache: Box<dyn Expression>,
    key: Box<dyn Expression>,
    value: Box<dyn Expression>,
    ttl_secs: Option<Box<dyn Expression>>,
    caches: CacheSearch,
}

impl FunctionExpression for CacheSetFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let cache_name = self.cache.resolve(ctx)?;
        let cache_name = cache_name
            .try_bytes_utf8_lossy()
            .map_err(|_| ExpressionError::from("cache name must be a valid UTF-8 string"))?;

        let key = self.key.resolve(ctx)?;
        let key = key
            .try_bytes_utf8_lossy()
            .map_err(|_| ExpressionError::from("cache key must be a valid UTF-8 string"))?;

        let value = self.value.resolve(ctx)?;

        let ttl_secs = self
            .ttl_secs
            .as_ref()
            .map(|expr| {
                let v = expr.resolve(ctx)?;
                v.try_integer()
                    .map(|i| i as u64)
                    .map_err(|_| ExpressionError::from("ttl_secs must be an integer"))
            })
            .transpose()?;

        let cache_ref = self
            .caches
            .find_cache(&cache_name)
            .map_err(|e| ExpressionError::from(e.to_string()))?;

        futures::executor::block_on(cache_ref.set(&key, value, ttl_secs))
            .map_err(|e| ExpressionError::from(e.to_string()))?;

        Ok(Value::Null)
    }

    fn type_def(&self, _: &TypeState) -> TypeDef {
        TypeDef::null().fallible()
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

    #[test]
    fn cache_set_and_verify() {
        let registry = CacheRegistry::new();
        let shared_data = Arc::new(std::sync::Mutex::new(HashMap::new()));
        let mock = Arc::new(MockCache {
            data: Arc::clone(&shared_data),
        }) as Arc<dyn CacheTable>;

        let mut caches = HashMap::new();
        caches.insert("test_cache".to_string(), mock);
        registry.load(caches);
        registry.finish_load();

        let cache_search = registry.as_readonly();

        let func = CacheSetFn {
            cache: Box::new(expression::Literal::from("test_cache")),
            key: Box::new(expression::Literal::from("my_key")),
            value: Box::new(expression::Literal::from("my_value")),
            ttl_secs: None,
            caches: cache_search,
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

        // Verify value was stored
        let data = shared_data.lock().unwrap();
        assert_eq!(data.get("my_key"), Some(&Value::from("my_value")));
    }
}
