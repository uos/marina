use std::borrow::Cow;
use std::ffi::OsStr;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use cloudini::ros::{CompressedPointCloud2, CompressionConfig};
use mcap::{Compression, Message, MessageStream, WriteOptions, Writer};
use memmap2::Mmap;

use crate::progress::ProgressReporter;

ros_pointcloud2::impl_pointcloud2_for_ros2_interfaces_jazzy_serde!();

const POINTCLOUD2_SCHEMA: &str = "sensor_msgs/msg/PointCloud2";
const CDR_ENCODING: &str = "cdr";
const MARINA_CODEC_KEY: &str = "marina.pointcloud.codec";
const MARINA_CODEC_VAL: &str = "cloudini";

fn map_mcap_file(path: &Path) -> Result<Mmap> {
    let file = File::open(path)
        .with_context(|| format!("failed to open input mcap {}", path.display()))?;
    let mapped = unsafe { Mmap::map(&file) }
        .with_context(|| format!("failed to mmap input mcap {}", path.display()))?;
    Ok(mapped)
}

#[cfg(unix)]
fn advise_sequential(mapped: &Mmap) {
    let _ = mapped.advise(memmap2::Advice::Sequential);
}

#[cfg(not(unix))]
fn advise_sequential(_mapped: &Mmap) {}

#[cfg(unix)]
fn advise_release(_mapped: &Mmap) {}

#[cfg(not(unix))]
fn advise_release(_mapped: &Mmap) {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McapChunkCompression {
    None,
    Zstd,
    Lz4,
}

impl Default for McapChunkCompression {
    fn default() -> Self {
        Self::Zstd
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PointCloudCompressionMode {
    Disabled,
    Lossy,
    Lossless,
}

impl Default for PointCloudCompressionMode {
    fn default() -> Self {
        Self::Lossy
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PushTransformOptions {
    pub pointcloud_mode: PointCloudCompressionMode,
    pub pointcloud_precision_m: f64,
    pub output_mcap_compression: McapChunkCompression,
}

impl Default for PushTransformOptions {
    fn default() -> Self {
        Self {
            pointcloud_mode: PointCloudCompressionMode::Lossy,
            pointcloud_precision_m: 0.001,
            output_mcap_compression: McapChunkCompression::Zstd,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PullTransformOptions {
    pub output_mcap_compression: McapChunkCompression,
}

impl Default for PullTransformOptions {
    fn default() -> Self {
        Self {
            output_mcap_compression: McapChunkCompression::Zstd,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct TransformStats {
    pub pointcloud_messages: usize,
    pub total_messages: usize,
}

pub fn compress_mcap_for_push(input: &Path, output: &Path) -> Result<TransformStats> {
    let mut reporter = ProgressReporter::silent();
    compress_mcap_for_push_with_progress(
        input,
        output,
        PushTransformOptions::default(),
        &mut reporter,
    )
}

pub fn compress_mcap_for_push_with_progress(
    input: &Path,
    output: &Path,
    options: PushTransformOptions,
    progress: &mut ProgressReporter<'_>,
) -> Result<TransformStats> {
    progress.emit(
        "pack",
        format!(
            "reading MCAP file {}",
            input
                .file_name()
                .unwrap_or_else(|| OsStr::new("<unknown>"))
                .to_string_lossy()
        ),
    );

    let mapped = map_mcap_file(input)?;
    advise_sequential(&mapped);

    let writer_file = File::create(output)
        .with_context(|| format!("failed to create output mcap {}", output.display()))?;
    let mut writer = make_writer(BufWriter::new(writer_file), options.output_mcap_compression)?;

    let mut stats = TransformStats::default();

    for msg in MessageStream::new(&mapped)? {
        let msg = msg?;
        stats.total_messages += 1;
        match options.pointcloud_mode {
            PointCloudCompressionMode::Disabled => writer.write(&msg)?,
            PointCloudCompressionMode::Lossy | PointCloudCompressionMode::Lossless => {
                if should_transform_channel(&msg) {
                    let transformed = compress_pointcloud_message(
                        msg,
                        options.pointcloud_mode,
                        options.pointcloud_precision_m,
                    )?;
                    writer.write(&transformed)?;
                    stats.pointcloud_messages += 1;
                } else {
                    writer.write(&msg)?;
                }
            }
        }
    }

    advise_release(&mapped);

    writer.finish()?;
    let mode = match options.pointcloud_mode {
        PointCloudCompressionMode::Disabled => "disabled",
        PointCloudCompressionMode::Lossy => "lossy",
        PointCloudCompressionMode::Lossless => "lossless",
    };
    progress.emit(
        "pack",
        format!(
            "transformed {} PointCloud2 messages out of {} total MCAP messages (mode: {}, precision: {:.3} mm)",
            stats.pointcloud_messages,
            stats.total_messages,
            mode,
            options.pointcloud_precision_m * 1000.0
        ),
    );
    Ok(stats)
}

pub fn decompress_mcap_after_pull(input: &Path, output: &Path) -> Result<TransformStats> {
    let mut reporter = ProgressReporter::silent();
    decompress_mcap_after_pull_with_progress(
        input,
        output,
        PullTransformOptions::default(),
        &mut reporter,
    )
}

pub fn decompress_mcap_after_pull_with_progress(
    input: &Path,
    output: &Path,
    options: PullTransformOptions,
    progress: &mut ProgressReporter<'_>,
) -> Result<TransformStats> {
    progress.emit(
        "unpack",
        format!(
            "reading MCAP file {}",
            input
                .file_name()
                .unwrap_or_else(|| OsStr::new("<unknown>"))
                .to_string_lossy()
        ),
    );

    let mapped = map_mcap_file(input)?;
    advise_sequential(&mapped);

    let writer_file = File::create(output)
        .with_context(|| format!("failed to create output mcap {}", output.display()))?;
    let mut writer = make_writer(BufWriter::new(writer_file), options.output_mcap_compression)?;

    let mut stats = TransformStats::default();

    for msg in MessageStream::new(&mapped)? {
        let msg = msg?;
        stats.total_messages += 1;
        if is_cloudini_encoded_channel(&msg) {
            let transformed = decompress_pointcloud_message(msg)?;
            writer.write(&transformed)?;
            stats.pointcloud_messages += 1;
        } else {
            writer.write(&msg)?;
        }
    }

    advise_release(&mapped);

    writer.finish()?;
    progress.emit(
        "unpack",
        format!(
            "restored {} PointCloud2 messages out of {} total MCAP messages",
            stats.pointcloud_messages, stats.total_messages
        ),
    );
    Ok(stats)
}

fn make_writer(
    writer: BufWriter<File>,
    compression: McapChunkCompression,
) -> Result<Writer<BufWriter<File>>> {
    let mcap_compression = match compression {
        McapChunkCompression::None => None,
        McapChunkCompression::Zstd => Some(Compression::Zstd),
        McapChunkCompression::Lz4 => Some(Compression::Lz4),
    };
    Writer::with_options(writer, WriteOptions::new().compression(mcap_compression))
        .context("failed creating mcap writer")
}

fn should_transform_channel(msg: &Message<'_>) -> bool {
    msg.channel.message_encoding == CDR_ENCODING
        && msg
            .channel
            .schema
            .as_ref()
            .is_some_and(|s| s.name == POINTCLOUD2_SCHEMA)
}

fn is_cloudini_encoded_channel(msg: &Message<'_>) -> bool {
    msg.channel
        .metadata
        .get(MARINA_CODEC_KEY)
        .is_some_and(|v| v.starts_with(MARINA_CODEC_VAL))
}

fn compress_pointcloud_message(
    msg: Message<'static>,
    mode: PointCloudCompressionMode,
    precision_m: f64,
) -> Result<Message<'static>> {
    let pointcloud: ros2_interfaces_jazzy_serde::sensor_msgs::msg::PointCloud2 =
        cdr::deserialize(&msg.data)
            .context("failed to CDR-decode PointCloud2 while preparing push")?;

    let cloud = impl_ros2_interfaces_jazzy_serde::to_pointcloud2_msg(pointcloud);
    let compression = match mode {
        PointCloudCompressionMode::Disabled => {
            unreachable!("disabled mode should not reach compress_pointcloud_message")
        }
        PointCloudCompressionMode::Lossy => CompressionConfig::lossy_zstd(precision_m as f32),
        PointCloudCompressionMode::Lossless => CompressionConfig::lossless_zstd(),
    };
    let compressed = CompressedPointCloud2::compress(cloud, compression)
        .context("cloudini compression failed")?;

    let payload = cdr::serialize::<_, _, cdr::CdrLe>(&compressed, cdr::Infinite)
        .context("failed to CDR-encode compressed pointcloud")?;

    let mut metadata = msg.channel.metadata.clone();
    metadata.insert(
        MARINA_CODEC_KEY.to_string(),
        match mode {
            PointCloudCompressionMode::Disabled => "cloudini/disabled".to_string(),
            PointCloudCompressionMode::Lossy => {
                format!("cloudini/lossy-zstd/{:.9}m", precision_m)
            }
            PointCloudCompressionMode::Lossless => "cloudini/lossless-zstd".to_string(),
        },
    );

    let channel = Arc::new(mcap::Channel {
        id: msg.channel.id,
        topic: msg.channel.topic.clone(),
        schema: msg.channel.schema.clone(),
        message_encoding: msg.channel.message_encoding.clone(),
        metadata,
    });

    Ok(Message {
        channel,
        sequence: msg.sequence,
        log_time: msg.log_time,
        publish_time: msg.publish_time,
        data: Cow::Owned(payload),
    })
}

fn decompress_pointcloud_message(msg: Message<'static>) -> Result<Message<'static>> {
    let compressed: CompressedPointCloud2 = cdr::deserialize(&msg.data)
        .context("failed to CDR-decode compressed pointcloud while pulling")?;
    let restored = compressed
        .decompress()
        .context("cloudini decompression failed")?;

    let ros_pointcloud = impl_ros2_interfaces_jazzy_serde::from_pointcloud2_msg(restored);
    let payload = cdr::serialize::<_, _, cdr::CdrLe>(&ros_pointcloud, cdr::Infinite)
        .context("failed to CDR-encode restored PointCloud2")?;

    let mut metadata = msg.channel.metadata.clone();
    metadata.remove(MARINA_CODEC_KEY);

    let channel = Arc::new(mcap::Channel {
        id: msg.channel.id,
        topic: msg.channel.topic.clone(),
        schema: msg.channel.schema.clone(),
        message_encoding: msg.channel.message_encoding.clone(),
        metadata,
    });

    Ok(Message {
        channel,
        sequence: msg.sequence,
        log_time: msg.log_time,
        publish_time: msg.publish_time,
        data: Cow::Owned(payload),
    })
}
