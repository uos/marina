use std::borrow::Cow;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use cloudini::ros::{CompressedPointCloud2, CompressionConfig};
use mcap::{Message, MessageStream, Writer};

ros_pointcloud2::impl_pointcloud2_for_ros2_interfaces_jazzy_serde!();

const POINTCLOUD2_SCHEMA: &str = "sensor_msgs/msg/PointCloud2";
const CDR_ENCODING: &str = "cdr";
const MARINA_CODEC_KEY: &str = "marina.pointcloud.codec";
const MARINA_CODEC_VAL: &str = "cloudini-lossy-zstd-1mm";

#[derive(Debug, Default, Clone, Copy)]
pub struct TransformStats {
    pub transformed_messages: usize,
}

pub fn compress_mcap_for_push(input: &Path, output: &Path) -> Result<TransformStats> {
    let bytes = std::fs::read(input)
        .with_context(|| format!("failed to read input mcap {}", input.display()))?;

    let writer_file = File::create(output)
        .with_context(|| format!("failed to create output mcap {}", output.display()))?;
    let mut writer = Writer::new(BufWriter::new(writer_file))?;

    let mut stats = TransformStats::default();

    for msg in MessageStream::new(&bytes)? {
        let msg = msg?;
        if should_transform_channel(&msg) {
            let transformed = compress_pointcloud_message(msg)?;
            writer.write(&transformed)?;
            stats.transformed_messages += 1;
        } else {
            writer.write(&msg)?;
        }
    }

    writer.finish()?;
    Ok(stats)
}

pub fn decompress_mcap_after_pull(input: &Path, output: &Path) -> Result<TransformStats> {
    let bytes = std::fs::read(input)
        .with_context(|| format!("failed to read input mcap {}", input.display()))?;

    let writer_file = File::create(output)
        .with_context(|| format!("failed to create output mcap {}", output.display()))?;
    let mut writer = Writer::new(BufWriter::new(writer_file))?;

    let mut stats = TransformStats::default();

    for msg in MessageStream::new(&bytes)? {
        let msg = msg?;
        if is_cloudini_encoded_channel(&msg) {
            let transformed = decompress_pointcloud_message(msg)?;
            writer.write(&transformed)?;
            stats.transformed_messages += 1;
        } else {
            writer.write(&msg)?;
        }
    }

    writer.finish()?;
    Ok(stats)
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
        .is_some_and(|v| v == MARINA_CODEC_VAL)
}

fn compress_pointcloud_message(msg: Message<'static>) -> Result<Message<'static>> {
    let pointcloud: ros2_interfaces_jazzy_serde::sensor_msgs::msg::PointCloud2 =
        cdr::deserialize(&msg.data)
            .context("failed to CDR-decode PointCloud2 while preparing push")?;

    let cloud = impl_ros2_interfaces_jazzy_serde::to_pointcloud2_msg(pointcloud);
    let compressed = CompressedPointCloud2::compress(cloud, CompressionConfig::lossy_zstd(0.001))
        .context("cloudini compression failed")?;

    let payload = cdr::serialize::<_, _, cdr::CdrLe>(&compressed, cdr::Infinite)
        .context("failed to CDR-encode compressed pointcloud")?;

    let mut metadata = msg.channel.metadata.clone();
    metadata.insert(MARINA_CODEC_KEY.to_string(), MARINA_CODEC_VAL.to_string());

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
