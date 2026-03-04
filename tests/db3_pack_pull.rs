#![cfg(feature = "db3")]
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use marina::io::bag;
use marina::io::db3_transform::{self, Db3TransformOptions};
use marina::io::mcap_transform::PointCloudCompressionMode;
use marina::io::pack;
use marina::progress::ProgressReporter;
use rusqlite::{Connection, OptionalExtension};

// Minimal rosbag2 SQLite schema (topics + messages tables only).
const ROSBAG2_SCHEMA: &str = "
    CREATE TABLE topics (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        type TEXT NOT NULL,
        serialization_format TEXT NOT NULL,
        offered_qos_profiles TEXT NOT NULL
    );
    CREATE TABLE messages (
        id INTEGER PRIMARY KEY,
        topic_id INTEGER NOT NULL,
        timestamp INTEGER NOT NULL,
        data BLOB NOT NULL
    );
";

fn fixture_db3() -> PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("dlg_cut_db3/dlg_cut_db3.db3");
    assert!(path.exists(), "test fixture missing: {}", path.display());
    path
}

/// Creates a valid db3 with one non-PointCloud2 topic and a dummy message.
fn create_minimal_db3_no_pc2(path: &Path) -> Result<()> {
    let conn = Connection::open(path)?;
    conn.execute_batch(ROSBAG2_SCHEMA)?;
    conn.execute(
        "INSERT INTO topics VALUES (1, '/tf', 'tf2_msgs/msg/TFMessage', 'cdr', '')",
        [],
    )?;
    // Minimal 4-byte CDR encapsulation header for a tiny payload.
    conn.execute(
        "INSERT INTO messages VALUES (1, 1, 100000000, X'00010000')",
        [],
    )?;
    Ok(())
}

/// Copies `msg_limit` real PointCloud2 CDR blobs from the dlg_cut_db3 fixture
/// into a fresh synthetic db3.  Returns the number of messages inserted.
fn create_db3_with_real_pc2(path: &Path, msg_limit: usize) -> Result<usize> {
    let real_db3 = fixture_db3();

    let real_conn =
        Connection::open_with_flags(&real_db3, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;

    let topic_id: Option<i64> = real_conn
        .query_row(
            "SELECT id FROM topics WHERE type = 'sensor_msgs/msg/PointCloud2' LIMIT 1",
            [],
            |row| row.get(0),
        )
        .optional()?;

    let topic_id = topic_id.expect("dlg_cut_db3 fixture has no PointCloud2 topic");

    let mut stmt =
        real_conn.prepare("SELECT data FROM messages WHERE topic_id = ?1 ORDER BY id LIMIT ?2")?;
    let messages: Vec<Vec<u8>> = stmt
        .query_map(rusqlite::params![topic_id, msg_limit as i64], |row| {
            row.get(0)
        })?
        .collect::<Result<_, _>>()?;

    let conn = Connection::open(path)?;
    conn.execute_batch(ROSBAG2_SCHEMA)?;
    conn.execute(
        "INSERT INTO topics VALUES \
         (1, '/ouster/points', 'sensor_msgs/msg/PointCloud2', 'cdr', '')",
        [],
    )?;
    for (i, data) in messages.iter().enumerate() {
        conn.execute(
            "INSERT INTO messages VALUES (?1, 1, ?2, ?3)",
            rusqlite::params![i as i64 + 1, i as i64 * 100_000_000i64, data],
        )?;
    }

    Ok(messages.len())
}

// ---------------------------------------------------------------------------
// Discovery / pack-unpack tests (no PointCloud2 compression)
// ---------------------------------------------------------------------------

#[test]
fn discovers_db3_directory_without_mcap() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bag_dir = tmp.path().join("bag_db3");
    fs::create_dir_all(&bag_dir)?;
    fs::write(
        bag_dir.join("metadata.yaml"),
        "rosbag2_bagfile_information: {}\n",
    )?;
    create_minimal_db3_no_pc2(&bag_dir.join("data_0.db3"))?;

    let source = bag::discover_bag(&bag_dir)?;
    assert!(source.has_db3);
    assert!(source.mcap.is_none());

    Ok(())
}

#[test]
fn pack_and_unpack_db3_roundtrip() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let bag_dir = tmp.path().join("bag_db3");
    fs::create_dir_all(&bag_dir)?;
    fs::write(
        bag_dir.join("metadata.yaml"),
        "rosbag2_bagfile_information: {}\n",
    )?;
    create_minimal_db3_no_pc2(&bag_dir.join("data_0.db3"))?;

    let source = bag::discover_bag(&bag_dir)?;
    let archive = tmp.path().join("bundle.marina.tar.gz");
    let _meta = pack::pack_bag(&source, &archive)?;

    let out = tmp.path().join("unpacked");
    pack::unpack_bag(&archive, &out)?;

    assert!(out.join("data_0.db3").exists());
    assert!(out.join("metadata.yaml").exists());

    Ok(())
}

// ---------------------------------------------------------------------------
// db3 PointCloud2 compression / decompression  (mirrors mcap_transform_read)
// ---------------------------------------------------------------------------

#[test]
fn compress_and_decompress_db3_pointcloud_lossless() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let test_db3 = tmp.path().join("test.db3");
    let msg_count = create_db3_with_real_pc2(&test_db3, 3)?;

    let mut progress = ProgressReporter::silent();

    let compress_stats = db3_transform::compress_db3_for_push(
        &test_db3,
        &Db3TransformOptions {
            pointcloud_mode: PointCloudCompressionMode::Lossless,
            pointcloud_precision_m: 0.001,
        },
        &mut progress,
    )?;

    assert_eq!(
        compress_stats.pointcloud_messages, msg_count,
        "all PC2 messages should have been compressed"
    );
    assert!(
        db3_transform::has_marina_pointcloud_metadata(&test_db3)?,
        "marina_metadata should be present after compression"
    );

    let decompress_stats = db3_transform::decompress_db3_after_pull(&test_db3, &mut progress)?;

    assert_eq!(
        decompress_stats.pointcloud_messages, msg_count,
        "all PC2 messages should have been restored"
    );
    assert!(
        !db3_transform::has_marina_pointcloud_metadata(&test_db3)?,
        "marina_metadata should be removed after decompression"
    );

    Ok(())
}

#[test]
fn compress_and_decompress_db3_pointcloud_lossy() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let test_db3 = tmp.path().join("test.db3");
    let msg_count = create_db3_with_real_pc2(&test_db3, 3)?;

    let mut progress = ProgressReporter::silent();

    let compress_stats = db3_transform::compress_db3_for_push(
        &test_db3,
        &Db3TransformOptions {
            pointcloud_mode: PointCloudCompressionMode::Lossy,
            pointcloud_precision_m: 0.001,
        },
        &mut progress,
    )?;

    assert_eq!(compress_stats.pointcloud_messages, msg_count);
    assert!(db3_transform::has_marina_pointcloud_metadata(&test_db3)?);

    // For lossy mode we only verify the decompress succeeds and produces valid
    // CDR (the point positions are approximate so byte-equality is not expected).
    let decompress_stats = db3_transform::decompress_db3_after_pull(&test_db3, &mut progress)?;

    assert_eq!(decompress_stats.pointcloud_messages, msg_count);
    assert!(!db3_transform::has_marina_pointcloud_metadata(&test_db3)?);

    Ok(())
}

#[test]
fn compress_db3_noop_on_no_pointcloud_topics() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let test_db3 = tmp.path().join("test.db3");
    create_minimal_db3_no_pc2(&test_db3)?;

    let mut progress = ProgressReporter::silent();

    let stats = db3_transform::compress_db3_for_push(
        &test_db3,
        &Db3TransformOptions {
            pointcloud_mode: PointCloudCompressionMode::Lossless,
            pointcloud_precision_m: 0.001,
        },
        &mut progress,
    )?;

    assert_eq!(
        stats.pointcloud_messages, 0,
        "no PC2 topics → nothing to compress"
    );
    assert!(
        !db3_transform::has_marina_pointcloud_metadata(&test_db3)?,
        "marina_metadata must not be written when there is nothing to compress"
    );

    Ok(())
}

#[test]
fn compress_and_decompress_db3_message_count_matches() -> Result<()> {
    let tmp = tempfile::tempdir()?;
    let test_db3 = tmp.path().join("test.db3");
    let msg_count = create_db3_with_real_pc2(&test_db3, 3)?;

    let mut progress = ProgressReporter::silent();

    let compress_stats = db3_transform::compress_db3_for_push(
        &test_db3,
        &Db3TransformOptions {
            pointcloud_mode: PointCloudCompressionMode::Lossless,
            pointcloud_precision_m: 0.001,
        },
        &mut progress,
    )?;
    let decompress_stats = db3_transform::decompress_db3_after_pull(&test_db3, &mut progress)?;

    assert_eq!(
        compress_stats.pointcloud_messages, decompress_stats.pointcloud_messages,
        "compress and decompress should process the same number of messages (mirrors the MCAP test)"
    );
    assert_eq!(
        compress_stats.pointcloud_messages, msg_count,
        "expected {} messages to be processed",
        msg_count
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Direct fixture tests (operate on a copy of dlg_cut_db3/dlg_cut_db3.db3)
// ---------------------------------------------------------------------------

#[test]
fn compress_and_decompress_real_fixture_lossless() -> Result<()> {
    let src = fixture_db3();
    let tmp = tempfile::tempdir()?;
    let test_db3 = tmp.path().join("dlg_cut_db3.db3");
    fs::copy(&src, &test_db3)?;

    let mut progress = ProgressReporter::silent();

    let compress_stats = db3_transform::compress_db3_for_push(
        &test_db3,
        &Db3TransformOptions {
            pointcloud_mode: PointCloudCompressionMode::Lossless,
            pointcloud_precision_m: 0.001,
        },
        &mut progress,
    )?;

    assert!(
        compress_stats.pointcloud_messages > 0,
        "expected at least one PC2 message in fixture"
    );
    assert!(
        compress_stats.total_messages > 0,
        "expected at least one message total in fixture"
    );
    assert!(db3_transform::has_marina_pointcloud_metadata(&test_db3)?);

    let decompress_stats = db3_transform::decompress_db3_after_pull(&test_db3, &mut progress)?;

    assert_eq!(
        compress_stats.pointcloud_messages, decompress_stats.pointcloud_messages,
        "compress and decompress should process the same number of messages"
    );
    assert!(!db3_transform::has_marina_pointcloud_metadata(&test_db3)?);

    Ok(())
}

#[test]
fn compress_and_decompress_real_fixture_lossy() -> Result<()> {
    let src = fixture_db3();
    let tmp = tempfile::tempdir()?;
    let test_db3 = tmp.path().join("dlg_cut_db3.db3");
    fs::copy(&src, &test_db3)?;

    let mut progress = ProgressReporter::silent();

    let compress_stats = db3_transform::compress_db3_for_push(
        &test_db3,
        &Db3TransformOptions {
            pointcloud_mode: PointCloudCompressionMode::Lossy,
            pointcloud_precision_m: 0.001,
        },
        &mut progress,
    )?;

    assert!(compress_stats.pointcloud_messages > 0);
    assert!(db3_transform::has_marina_pointcloud_metadata(&test_db3)?);

    let decompress_stats = db3_transform::decompress_db3_after_pull(&test_db3, &mut progress)?;

    assert_eq!(
        compress_stats.pointcloud_messages,
        decompress_stats.pointcloud_messages
    );
    assert!(!db3_transform::has_marina_pointcloud_metadata(&test_db3)?);

    Ok(())
}
