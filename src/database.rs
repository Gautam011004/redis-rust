use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Error;
use tokio::{sync::Mutex, time::sleep};


#[derive(Clone)]
pub enum key_value {
    String(String),
    List(Vec<String>)
}
pub struct dbstate {
    pub kv: HashMap<String, key_value>,
}

#[derive(Clone)]
pub struct db {
    pub state: Arc<Mutex<dbstate>>
}

impl db {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(dbstate { kv: HashMap::new() }))
        }
    }

    pub async fn get(&self, key: &str) -> Option<String> {
        let lock = self.state.lock().await;

        let v = match lock.kv.get(key).cloned() {
            Some(key_value::String(s)) => s,
            _ => panic!("Usupported args")
        };
        Some(v)
    }

    pub async fn set(&self, key: String, value: String, ttl: Option<u64>) -> Result<(), Error>{
        let mut lock = self.state.lock().await;

        lock.kv.insert(key.clone(), key_value::String(String::from(value)));

        let temp = self.clone();
        if let Some(ttl) = ttl {
            tokio::spawn(async move {
                sleep(Duration::from_millis(ttl)).await;
                let mut locking = temp.state.lock().await;
                locking.kv.remove(&key);
            });
        }     
        Ok(())
    } 
}