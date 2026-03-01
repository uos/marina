use anyhow::Result;
use std::env;
use std::fs;
use std::sync::Mutex;

use marina::storage::config;

static ENV_LOCK: Mutex<()> = Mutex::new(());

#[test]
fn no_config_file_yields_default() -> Result<()> {
    let _guard = ENV_LOCK.lock().expect("env test mutex poisoned");
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
    assert!(matches!(
        regs.compression.pointcloud_mode,
        config::ConfigPointcloudMode::Lossy
    ));
    assert!((regs.compression.pointcloud_accuracy_mm - 1.0).abs() < f64::EPSILON);
    assert!(matches!(
        regs.compression.packed_mcap_compression,
        config::ConfigMcapCompression::Zstd
    ));
    assert!(matches!(
        regs.compression.packed_archive_compression,
        config::ConfigArchiveCompression::Gzip
    ));
    assert!(matches!(
        regs.compression.unpacked_mcap_compression,
        config::ConfigMcapCompression::Zstd
    ));
    Ok(())
}

#[test]
fn existing_empty_config_does_not_add_default() -> Result<()> {
    let _guard = ENV_LOCK.lock().expect("env test mutex poisoned");
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
    assert!(matches!(
        regs.compression.pointcloud_mode,
        config::ConfigPointcloudMode::Lossy
    ));
    assert!((regs.compression.pointcloud_accuracy_mm - 1.0).abs() < f64::EPSILON);
    assert!(matches!(
        regs.compression.packed_mcap_compression,
        config::ConfigMcapCompression::Zstd
    ));
    assert!(matches!(
        regs.compression.packed_archive_compression,
        config::ConfigArchiveCompression::Gzip
    ));
    assert!(matches!(
        regs.compression.unpacked_mcap_compression,
        config::ConfigMcapCompression::Zstd
    ));
    Ok(())
}

#[test]
fn custom_compression_config_is_loaded() -> Result<()> {
    let _guard = ENV_LOCK.lock().expect("env test mutex poisoned");
    let tmp = tempfile::tempdir()?;
    unsafe {
        env::set_var("MARINA_CONFIG_DIR", tmp.path());
    }

    let cfg = tmp.path().join("registries.toml");
    fs::write(
        &cfg,
        r#"
registry = []

[compression]
pointcloud_mode = "off"
pointcloud_accuracy_mm = 2.5
packed_mcap_compression = "none"
packed_archive_compression = "none"
unpacked_mcap_compression = "lz4"
"#,
    )?;

    let regs = config::load_registries()?;
    assert!(matches!(
        regs.compression.pointcloud_mode,
        config::ConfigPointcloudMode::Off
    ));
    assert!((regs.compression.pointcloud_accuracy_mm - 2.5).abs() < f64::EPSILON);
    assert!(matches!(
        regs.compression.packed_mcap_compression,
        config::ConfigMcapCompression::None
    ));
    assert!(matches!(
        regs.compression.packed_archive_compression,
        config::ConfigArchiveCompression::None
    ));
    assert!(matches!(
        regs.compression.unpacked_mcap_compression,
        config::ConfigMcapCompression::Lz4
    ));

    Ok(())
}
