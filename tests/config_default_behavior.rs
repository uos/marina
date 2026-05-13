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
        config::ConfigArchiveCompression::None
    ));
    assert!(matches!(
        regs.compression.unpacked_mcap_compression,
        config::ConfigMcapCompression::Lz4
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
    let cfg = tmp.path().join("marina.rl");
    fs::write(&cfg, "registries {}")?;

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
        config::ConfigArchiveCompression::None
    ));
    assert!(matches!(
        regs.compression.unpacked_mcap_compression,
        config::ConfigMcapCompression::Lz4
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

    let cfg = tmp.path().join("marina.rl");
    fs::write(
        &cfg,
        r#"
compression {
  pointcloud_mode = "off"
  pointcloud_accuracy = 2.5mm
  packed_mcap_compression = "none"
  packed_archive_compression = "none"
  unpacked_mcap_compression = "lz4"
}

registries {}
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

#[test]
fn ssh_proxy_jump_config_is_loaded_and_saved() -> Result<()> {
    let _guard = ENV_LOCK.lock().expect("env test mutex poisoned");
    let tmp = tempfile::tempdir()?;
    unsafe {
        env::set_var("MARINA_CONFIG_DIR", tmp.path());
    }

    let cfg = tmp.path().join("marina.rl");
    fs::write(
        &cfg,
        r#"
registries {
  glumanda {
    uri = "ssh://marina@glumanda.example.org:/srv/marina"
    kind = "ssh"
    auth_env = "MARINA_SSH_KEY"
    proxy_jump = "ci@jump.example.org:2222"
    ssh_transport = "openssh"
  }
}
"#,
    )?;

    let regs = config::load_registries()?;
    let reg = regs
        .registry
        .iter()
        .find(|r| r.name == "glumanda")
        .expect("glumanda registry should be loaded");
    assert_eq!(reg.proxy_jump.as_deref(), Some("ci@jump.example.org:2222"));
    assert_eq!(reg.ssh_transport.as_deref(), Some("openssh"));

    config::save_registries(&regs)?;
    let saved = fs::read_to_string(&cfg)?;
    assert!(saved.contains("proxy_jump = \"ci@jump.example.org:2222\""));
    assert!(saved.contains("ssh_transport = \"openssh\""));

    Ok(())
}
