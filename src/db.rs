use std::{collections::HashMap, sync::{Arc}};

use anyhow::Error;
use tokio::sync::Mutex;

struct dbstate {
    kv: HashMap<String, String>
}

#[derive(Clone)]
pub struct db {
    state: Arc<Mutex<dbstate>>
}

impl db {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(dbstate { kv: HashMap::new() }))
        }
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        let lock = self.state.lock().await;

        lock.kv.get(key).cloned()
    }

    pub async fn set(&self, key: &str, value: &str) -> Result<(), Error>{
        let mut lock = self.state.lock().await;

        lock.kv.insert(String::from(key), String::from(value));
        Ok(())
    }
}