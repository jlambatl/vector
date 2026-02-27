//! VRL function: `cache_remove`
//!
//! Removes a value from a named cache by key.

use std::sync::LazyLock;

use vrl::prelude::*;

use super::cache_get::CacheVrlError;
use crate::CacheSearch;

static PARAMETERS: LazyLock<Vec<Parameter>> = LazyLock::new(|| {
    vec![
        Parameter::required(
            "cache",
            kind::BYTES,
            "The name of the cache to remove the value from.",
        ),
        Parameter::required("key", kind::BYTES, "The key to remove from the cache."),
    ]
});

/// VRL function to remove a value from a named cache.
///
/// # Usage
///
/// ```vrl
/// cache_remove!("my_cache", "request_id_123")
/// ```
#[derive(Clone, Copy, Debug)]
pub struct CacheRemove;

impl Function for CacheRemove {
    fn identifier(&self) -> &'static str {
        "cache_remove"
    }

    fn usage(&self) -> &'static str {
        "Removes a value from a named cache by key."
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
        &[example! {
            title: "Remove a cached value",
            source: r#"cache_remove!("my_cache", "user_123")"#,
            result: Ok("null"),
        }]
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

        Ok(CacheRemoveFn { cache, key, caches }.as_expr())
    }
}

#[derive(Debug, Clone)]
struct CacheRemoveFn {
    cache: Box<dyn Expression>,
    key: Box<dyn Expression>,
    caches: CacheSearch,
}

impl FunctionExpression for CacheRemoveFn {
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

        futures::executor::block_on(cache_ref.remove(&key))
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
    fn cache_remove_existing_key() {
        let registry = CacheRegistry::new();
        let mut initial_data = HashMap::new();
        initial_data.insert("to_remove".to_string(), Value::from("goodbye"));

        let shared_data = Arc::new(std::sync::Mutex::new(initial_data));
        let mock = Arc::new(MockCache {
            data: Arc::clone(&shared_data),
        }) as Arc<dyn CacheTable>;

        let mut caches = HashMap::new();
        caches.insert("test_cache".to_string(), mock);
        registry.load(caches);
        registry.finish_load();

        let cache_search = registry.as_readonly();

        let func = CacheRemoveFn {
            cache: Box::new(expression::Literal::from("test_cache")),
            key: Box::new(expression::Literal::from("to_remove")),
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

        // Verify value was removed
        let data = shared_data.lock().unwrap();
        assert!(data.get("to_remove").is_none());
    }
}
