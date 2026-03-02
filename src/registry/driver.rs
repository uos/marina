use std::path::Path;

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

use crate::model::bag_ref::BagRef;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteDescriptor {
    pub registry_name: String,
    pub bag: BagRef,
    pub original_bytes: u64,
    pub packed_bytes: u64,
}

pub trait RegistryDriver: Send + Sync {
    fn push(
        &self,
        registry_name: &str,
        bag: &BagRef,
        packed_file: &Path,
        original_bytes: u64,
        packed_bytes: u64,
    ) -> Result<()>;

    fn pull(&self, bag: &BagRef, out_packed_file: &Path) -> Result<RemoteDescriptor>;

    fn list(&self, filter: &str) -> Result<Vec<BagRef>>;

    fn remove(&self, bag: &BagRef) -> Result<()>;

    fn write_http_index(&self) -> Result<()> {
        Err(anyhow!(
            "http index generation is not supported for this registry type"
        ))
    }

    fn check_connection(&self) -> Result<()> {
        Ok(())
    }
}
