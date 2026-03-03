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

/// Metadata written alongside the bundle when pushing.
#[derive(Debug, Clone)]
pub struct PushMeta {
    pub original_bytes: u64,
    pub packed_bytes: u64,
    /// Short SHA-256 of the packed bundle (first 12 hex chars).
    pub bundle_hash: String,
    /// Pointcloud encoding: "lossless", "lossy", or "disabled".
    pub pointcloud: String,
    /// MCAP chunk compression: "none", "zstd", or "lz4".
    pub mcap_compression: String,
}

/// Per-bag information readable from a remote registry.
#[derive(Debug, Clone)]
pub struct BagInfo {
    pub bundle_hash: Option<String>,
    pub original_bytes: u64,
    pub packed_bytes: u64,
    pub pointcloud: Option<String>,
    pub mcap_compression: Option<String>,
}

pub trait RegistryDriver: Send + Sync {
    fn push(
        &self,
        registry_name: &str,
        bag: &BagRef,
        packed_file: &Path,
        meta: &PushMeta,
    ) -> Result<()>;

    fn pull(&self, bag: &BagRef, out_packed_file: &Path) -> Result<RemoteDescriptor>;

    fn list(&self, filter: &str) -> Result<Vec<BagRef>>;

    fn remove(&self, bag: &BagRef) -> Result<()>;

    /// Fetch lightweight metadata for a specific bag. Returns `None` if unsupported.
    fn bag_info(&self, _bag: &BagRef) -> Result<Option<BagInfo>> {
        Ok(None)
    }

    /// List all matching bags together with their metadata in one operation.
    /// Drivers that can batch this more efficiently should override the default.
    fn list_with_info(&self, filter: &str) -> Result<Vec<(BagRef, Option<BagInfo>)>> {
        let bags = self.list(filter)?;
        bags.into_iter()
            .map(|bag| {
                let info = self.bag_info(&bag).ok().flatten();
                Ok((bag, info))
            })
            .collect()
    }

    fn write_http_index(&self) -> Result<()> {
        Err(anyhow!(
            "http index generation is not supported for this registry type"
        ))
    }

    fn check_connection(&self) -> Result<()> {
        Ok(())
    }
}
