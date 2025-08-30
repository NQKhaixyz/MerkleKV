// src/store/sled_engine.rs
use anyhow::{Result, anyhow};
use sled::{Db, Tree, IVec};
use super::kv_trait::KVEngineStoreTrait;

pub struct SledEngine {
    db: Db,
    tree: Tree,
}

impl SledEngine {
    pub fn new(storage_path: &str) -> Result<Self> {
        let db = sled::open(storage_path)?;
        let tree = db.open_tree(b"merkle_kv")?;
        Ok(Self { db, tree })
    }

    fn to_string_opt(v: Option<IVec>) -> Option<String> {
        v.map(|ivec| String::from_utf8_lossy(&ivec).to_string())
    }
}

impl KVEngineStoreTrait for SledEngine {
    fn get(&self, key: &str) -> Option<String> {
        match self.tree.get(key) {
            Ok(opt) => Self::to_string_opt(opt),
            Err(_) => None,
        }
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        self.tree.insert(key.as_bytes(), value.as_bytes())?;
        Ok(())
    }

    fn delete(&self, key: &str) -> bool {
        match self.tree.remove(key) {
            Ok(opt) => opt.is_some(),
            Err(_) => false,
        }
    }

    fn keys(&self) -> Vec<String> {
        let iter = self.tree.iter();
        iter.keys()
            .filter_map(|r| r.ok())
            .filter_map(|k| String::from_utf8(k.to_vec()).ok())
            .collect()
    }

    fn len(&self) -> usize {
        self.tree.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn increment(&self, key: &str, amount: Option<i64>) -> Result<i64> {
        let amt = amount.unwrap_or(1);
        // get current
        let current = match self.tree.get(key) {
            Ok(Some(v)) => {
                let s = String::from_utf8_lossy(&v).to_string();
                s.parse::<i64>().map_err(|e| anyhow!("parse int error: {}", e))?
            }
            Ok(None) => 0,
            Err(e) => return Err(anyhow!(e)),
        };
        let new = current + amt;
        self.tree.insert(key.as_bytes(), new.to_string().as_bytes())?;
        Ok(new)
    }

    fn decrement(&self, key: &str, amount: Option<i64>) -> Result<i64> {
        let dec = amount.unwrap_or(1);
        self.increment(key, Some(-dec))
    }

    fn append(&self, key: &str, value: &str) -> Result<String> {
        let current = match self.tree.get(key) {
            Ok(Some(v)) => String::from_utf8_lossy(&v).to_string(),
            Ok(None) => String::new(),
            Err(e) => return Err(anyhow!(e)),
        };
        let new = format!("{}{}", current, value);
        self.tree.insert(key.as_bytes(), new.as_bytes())?;
        Ok(new)
    }

    fn prepend(&self, key: &str, value: &str) -> Result<String> {
        let current = match self.tree.get(key) {
            Ok(Some(v)) => String::from_utf8_lossy(&v).to_string(),
            Ok(None) => String::new(),
            Err(e) => return Err(anyhow!(e)),
        };
        let new = format!("{}{}", value, current);
        self.tree.insert(key.as_bytes(), new.as_bytes())?;
        Ok(new)
    }

    fn truncate(&self) -> Result<()> {
        self.tree.clear()?;
        Ok(())
    }

    fn count_keys(&self) -> Result<u64> {
        Ok(self.tree.len() as u64)
    }

    fn sync(&self) -> Result<()> {
        self.db.flush()?;
        Ok(())
    }
}
