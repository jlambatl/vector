use async_trait::async_trait;
use criterion::{Criterion, criterion_group, criterion_main};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tiered_caching::{CacheRegistry, CacheTable};
use vrl::value::Value;

#[derive(Clone)]
struct MockCache {
    name: String,
    data: Arc<Mutex<HashMap<String, Value>>>,
}

#[async_trait]
impl CacheTable for MockCache {
    async fn get(&self, key: &str) -> tiered_caching::CacheResult<Option<Value>> {
        let data = self.data.lock().unwrap();
        Ok(data.get(key).cloned())
    }

    async fn set(
        &self,
        key: &str,
        value: Value,
        _ttl_secs: Option<u64>,
    ) -> tiered_caching::CacheResult<()> {
        let mut data = self.data.lock().unwrap();
        data.insert(key.to_string(), value);
        Ok(())
    }

    async fn remove(&self, key: &str) -> tiered_caching::CacheResult<()> {
        let mut data = self.data.lock().unwrap();
        data.remove(key);
        Ok(())
    }

    async fn health_check(&self) -> bool {
        true
    }

    fn name(&self) -> &str {
        &self.name
    }
}

fn bench_cache_get(c: &mut Criterion) {
    let registry = CacheRegistry::new();
    let mock = Arc::new(MockCache {
        name: "bench_cache".to_string(),
        data: Arc::new(Mutex::new(HashMap::new())),
    }) as Arc<dyn CacheTable>;

    let mut caches = HashMap::new();
    caches.insert("bench".to_string(), mock);
    registry.load(caches);
    registry.finish_load();

    let search = registry.as_readonly();
    let cache_ref = search.find_cache("bench").unwrap();

    // Pre-populate with some data
    for i in 0..1000 {
        let key = format!("key{i}");
        let value = Value::from(format!("value{i}"));
        futures::executor::block_on(cache_ref.set(&key, value, None)).unwrap();
    }

    c.bench_function("cache_get_hit", |b| {
        b.iter(|| {
            futures::executor::block_on(cache_ref.get("key500")).unwrap();
        })
    });

    c.bench_function("cache_get_miss", |b| {
        b.iter(|| {
            futures::executor::block_on(cache_ref.get("nonexistent")).unwrap();
        })
    });
}

fn bench_cache_set(c: &mut Criterion) {
    let registry = CacheRegistry::new();
    let mock = Arc::new(MockCache {
        name: "bench_cache".to_string(),
        data: Arc::new(Mutex::new(HashMap::new())),
    }) as Arc<dyn CacheTable>;

    let mut caches = HashMap::new();
    caches.insert("bench".to_string(), mock);
    registry.load(caches);
    registry.finish_load();

    let search = registry.as_readonly();
    let cache_ref = search.find_cache("bench").unwrap();

    c.bench_function("cache_set", |b| {
        let mut i = 0;
        b.iter(|| {
            let key = format!("set_key{i}");
            let value = Value::from(format!("set_value{i}"));
            futures::executor::block_on(cache_ref.set(&key, value, None)).unwrap();
            i += 1;
        })
    });
}

fn bench_cache_remove(c: &mut Criterion) {
    let registry = CacheRegistry::new();
    let mock = Arc::new(MockCache {
        name: "bench_cache".to_string(),
        data: Arc::new(Mutex::new(HashMap::new())),
    }) as Arc<dyn CacheTable>;

    let mut caches = HashMap::new();
    caches.insert("bench".to_string(), mock);
    registry.load(caches);
    registry.finish_load();

    let search = registry.as_readonly();
    let cache_ref = search.find_cache("bench").unwrap();

    // Pre-populate
    for i in 0..1000 {
        let key = format!("rem_key{i}");
        let value = Value::from(format!("rem_value{i}"));
        futures::executor::block_on(cache_ref.set(&key, value, None)).unwrap();
    }

    c.bench_function("cache_remove", |b| {
        let mut i = 0;
        b.iter(|| {
            let key = format!("rem_key{}", i % 1000);
            futures::executor::block_on(cache_ref.remove(&key)).unwrap();
            i += 1;
        })
    });
}

criterion_group!(
    benches,
    bench_cache_get,
    bench_cache_set,
    bench_cache_remove
);
criterion_main!(benches);
