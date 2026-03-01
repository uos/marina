use anyhow::Result;
use std::env;
use std::fs;

use marina::storage::config;

#[test]
fn no_config_file_yields_default() -> Result<()> {
    let tmp = tempfile::tempdir()?;
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
fn existing_empty_config_does_not_add_default() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    unsafe {
        env::set_var("MARINA_CONFIG_DIR", tmp.path());
    }

    // create an explicit empty registries file
    let cfg = tmp.path().join("registries.toml");
    fs::write(&cfg, "registry = []")?;

    let regs = config::load_registries()?;
    assert!(
        !regs
            .registry
            .iter()
            .any(|r| r.name == config::DEFAULT_REGISTRY_NAME)
    );
    Ok(())
}
