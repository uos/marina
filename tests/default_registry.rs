use anyhow::Result;
use std::env;

use marina::core::marina::connection_warning;
use marina::model::bag_ref::BagRef;
use marina::registry::driver::RegistryDriver;
use marina::storage::config;
use tempfile::tempdir;

struct FailingDriver;

impl RegistryDriver for FailingDriver {
    fn push(
        &self,
        _registry_name: &str,
        _bag: &BagRef,
        _packed_file: &std::path::Path,
        _original_bytes: u64,
        _packed_bytes: u64,
    ) -> Result<()> {
        Err(anyhow::anyhow!("unexpected"))
    }

    fn pull(
        &self,
        _bag: &BagRef,
        _out_packed_file: &std::path::Path,
    ) -> Result<marina::registry::driver::RemoteDescriptor> {
        Err(anyhow::anyhow!("unexpected"))
    }

    fn list(&self, _filter: &str) -> Result<Vec<BagRef>> {
        Err(anyhow::anyhow!("unreachable"))
    }

    fn remove(&self, _bag: &BagRef) -> Result<()> {
        Err(anyhow::anyhow!("unexpected"))
    }

    fn check_connection(&self) -> Result<()> {
        Err(anyhow::anyhow!("connection failed"))
    }
}

#[test]
fn default_registry_present_in_empty_config() -> Result<()> {
    // point configuration at an empty temporary directory so we don't touch the
    // user's real config.
    let tmp = tempdir()?;
    unsafe {
        env::set_var("MARINA_CONFIG_DIR", tmp.path());
    }

    let regs = config::load_registries()?;
    assert!(
        regs.registry
            .iter()
            .any(|r| r.name == config::DEFAULT_REGISTRY_NAME)
    );
    Ok(())
}

#[test]
fn connection_warning_helper_returns_message() {
    // use a non-default name to emphasize the helper is generic
    let msg = connection_warning("my-registry", "some://uri", &FailingDriver);
    assert!(msg.is_some(), "expected a warning string");
    let text = msg.unwrap();
    assert!(text.contains("my-registry"));
    assert!(text.contains("unreachable") || text.contains("connection failed"));
}
