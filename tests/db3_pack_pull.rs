use std::fs;

use anyhow::Result;
use marina::io::bag;
use marina::io::pack;

#[test]
fn discovers_db3_directory_without_mcap() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bag_dir = tmp.path().join("bag_db3");
    fs::create_dir_all(&bag_dir)?;
    fs::write(
        bag_dir.join("metadata.yaml"),
        "rosbag2_bagfile_information: {}\n",
    )?;
    fs::write(bag_dir.join("data_0.db3"), b"sqlite-bytes")?;

    let source = bag::discover_bag(&bag_dir)?;
    assert!(source.has_db3);
    assert!(source.mcap.is_none());

    Ok(())
}

#[test]
fn pack_and_unpack_db3_skips_mcap_pipeline() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bag_dir = tmp.path().join("bag_db3");
    fs::create_dir_all(&bag_dir)?;
    fs::write(
        bag_dir.join("metadata.yaml"),
        "rosbag2_bagfile_information: {}\n",
    )?;
    fs::write(bag_dir.join("data_0.db3"), b"sqlite-bytes")?;

    let source = bag::discover_bag(&bag_dir)?;
    let archive = tmp.path().join("bundle.marina.tar.gz");
    let _meta = pack::pack_bag(&source, &archive)?;

    let out = tmp.path().join("unpacked");
    pack::unpack_bag(&archive, &out)?;

    assert!(out.join("data_0.db3").exists());
    assert!(out.join("metadata.yaml").exists());

    Ok(())
}
