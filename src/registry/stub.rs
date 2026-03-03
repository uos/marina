use std::path::Path;

use anyhow::{Result, anyhow};

use crate::model::bag_ref::BagRef;
use crate::registry::driver::{PushMeta, RegistryDriver, RemoteDescriptor};

pub struct StubRegistry {
    kind: String,
    uri: String,
    auth_env: Option<String>,
}

impl StubRegistry {
    pub fn new(kind: &str, uri: &str, auth_env: Option<String>) -> Self {
        Self {
            kind: kind.to_string(),
            uri: uri.to_string(),
            auth_env,
        }
    }

    fn unsupported(&self) -> anyhow::Error {
        let mut msg = format!(
            "registry kind '{}' ({}) is configured but not implemented yet",
            self.kind, self.uri
        );
        if let Some(var) = &self.auth_env {
            msg.push_str(&format!("; auth env configured: {}", var));
        }
        anyhow!(msg)
    }
}

impl RegistryDriver for StubRegistry {
    fn push(
        &self,
        _registry_name: &str,
        _bag: &BagRef,
        _packed_file: &Path,
        _meta: &PushMeta,
    ) -> Result<()> {
        Err(self.unsupported())
    }

    fn pull(&self, _bag: &BagRef, _out_packed_file: &Path) -> Result<RemoteDescriptor> {
        Err(self.unsupported())
    }

    fn list(&self, _filter: &str) -> Result<Vec<BagRef>> {
        Err(self.unsupported())
    }

    fn remove(&self, _bag: &BagRef) -> Result<()> {
        Err(self.unsupported())
    }
}
