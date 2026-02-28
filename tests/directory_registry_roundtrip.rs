use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use marina::io::bag::discover_bag;
use marina::io::pack::{pack_bag, unpack_bag};
use marina::model::bag_ref::BagRef;
use marina::registry::driver::RegistryDriver;
use marina::registry::folder::FolderRegistry;

fn unique_temp(name: &str) -> PathBuf {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).expect("time");
    std::env::temp_dir().join(format!(
        "marina_{name}_{}_{}",
        std::process::id(),
        ts.as_nanos()
    ))
}

#[test]
fn directory_registry_pack_pull_unpack_roundtrip() -> Result<()> {
    let bag_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test/dlg_cut");
    let source = discover_bag(&bag_dir)?;

    let work = unique_temp("roundtrip");
    fs::create_dir_all(&work)?;

    let packed_local = work.join("bundle.push.tar.gz");
    let meta = pack_bag(&source, &packed_local)?;
    assert!(meta.packed_bytes > 0);

    let registry_root = work.join("registry_dir");
    let registry = FolderRegistry::from_uri("dirtest", &registry_root.display().to_string())?;

    let bag_ref: BagRef = "dlg_cut:ouster".parse()?;
    registry.push(
        "dirtest",
        &bag_ref,
        &packed_local,
        meta.original_bytes,
        meta.packed_bytes,
    )?;

    let listed = registry.list("dlg_cut*")?;
    assert!(listed.iter().any(|b| b == &bag_ref));

    let pulled_archive = work.join("bundle.pull.tar.gz");
    let descriptor = registry.pull(&bag_ref, &pulled_archive)?;
    assert_eq!(descriptor.bag, bag_ref);

    let unpacked = work.join("unpacked");
    fs::create_dir_all(&unpacked)?;
    unpack_bag(&pulled_archive, &unpacked)?;

    let unpacked_bag = discover_bag(&unpacked)?;
    assert!(unpacked_bag.mcap.exists());
    assert!(unpacked_bag.metadata_yaml.exists());

    Ok(())
}
