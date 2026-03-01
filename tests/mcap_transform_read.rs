use std::path::PathBuf;

use anyhow::Result;
use marina::io::mcap_transform::{
    McapChunkCompression, PointCloudCompressionMode, PullTransformOptions, PushTransformOptions,
    compress_mcap_for_push_with_progress, decompress_mcap_after_pull_with_progress,
};
use marina::progress::ProgressReporter;

#[test]
fn reads_and_rewrites_dlg_cut_mcap_via_chunked_indexed_reader() -> Result<()> {
    let input = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("dlg_cut/dlg_cut_0.mcap");
    assert!(input.exists(), "test fixture missing: {}", input.display());

    let tmp = tempfile::tempdir()?;
    let transformed = tmp.path().join("transformed_push.mcap");
    let restored = tmp.path().join("restored_pull.mcap");

    let mut progress = ProgressReporter::silent();

    let push_stats = compress_mcap_for_push_with_progress(
        &input,
        &transformed,
        PushTransformOptions {
            pointcloud_mode: PointCloudCompressionMode::Disabled,
            pointcloud_precision_m: 0.001,
            output_mcap_compression: McapChunkCompression::None,
        },
        &mut progress,
    )?;

    assert!(transformed.exists(), "output mcap was not created");
    assert!(
        push_stats.total_messages > 0,
        "expected to process at least one MCAP message"
    );

    let pull_stats = decompress_mcap_after_pull_with_progress(
        &transformed,
        &restored,
        PullTransformOptions {
            output_mcap_compression: McapChunkCompression::None,
        },
        &mut progress,
    )?;

    assert!(restored.exists(), "restored mcap was not created");
    assert_eq!(
        push_stats.total_messages, pull_stats.total_messages,
        "push and pull transform should process the same number of messages"
    );

    Ok(())
}
