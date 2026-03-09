# Compression

Marina applies several layers of compression when pushing datasets to registries. The defaults are chosen to maximise space savings while keeping pull times fast and data quality high.

## PointCloud2 Compression

For ROS 2 `sensor_msgs/PointCloud2` messages, Marina embeds the [cloudini](https://github.com/facontidavide/cloudini) library, a specialised point cloud codec that achieves dramatically better compression ratios than general-purpose compressors.

Marina uses **lossy compression with 1 mm accuracy** by default. At this setting, individual point coordinates are rounded to the nearest millimetre before encoding. For virtually all outdoor LiDAR use cases (Velodyne, Ouster, Livox, etc.) this is imperceptible since 1 mm is well below sensor noise.

| Mode | Description |
|---|---|
| `off` | No PointCloud2 compression; messages are stored as-is |
| `lossy` | Coordinate rounding to `pointcloud_accuracy` (default: `1mm`) |
| `lossless` | Lossless cloudini encoding; larger files than lossy |

## MCAP Chunk Compression

MCAP files store messages in compressed chunks. Marina recompresses these chunks when packing a dataset.

Two contexts are configurable separately:

| Setting | Description |
|---|---|
| `packed_mcap_compression` | Compression applied to chunks inside the uploaded archive |
| `unpacked_mcap_compression` | Compression applied to chunks in the locally cached copy after pull |

Available codecs: `none`, `lz4`, `zstd`.

- **LZ4**: fast decompression, good for local cache where pull speed matters.
- **Zstd**: higher compression ratio, good for remote storage where bandwidth matters.

## Archive Compression

The outer archive wrapping the MCAP files can be compressed as well for when folders contain much more than bags:

| Setting | Description |
|---|---|
| `packed_archive_compression` | Compression of the `.tar` archive uploaded to the registry |

Available modes: `none`, `gzip`.

## Configuration File

Compression defaults live in `~/.config/marina/marina.rl` under the `compression` block.
The format is [ratslang](https://codeberg.org/stelzo/ratslang) — physical units are written directly (e.g. `1mm`, `0.5mm`).

~~~awk
compression {
  pointcloud_mode = lossy
  pointcloud_accuracy = 1mm      # any length unit: mm, cm, m
  packed_mcap_compression = zstd
  packed_archive_compression = none
  unpacked_mcap_compression = lz4
}
~~~

All fields are optional. Omitting a field uses the built-in default shown above.

## Per-Command Overrides

Compression settings provided on the command line override the config for that invocation only. They do not modify the config file.

~~~bash
# Push with lossless point cloud compression
marina push outdoor-run:v2 ./bag/ --pointcloud-mode lossless

# Push with higher accuracy (0.1 mm)
marina push outdoor-run:v2 ./bag/ \
  --pointcloud-mode lossy \
  --pointcloud-accuracy-mm 0.1

# Push without any point cloud compression
marina push outdoor-run:v2 ./bag/ --pointcloud-mode off

# Change MCAP chunk compression
marina push outdoor-run:v2 ./bag/ \
  --packed-mcap-compression lz4 \
  --packed-archive-compression gzip

# Pull with no recompression of local cache
marina pull outdoor-run:v2 --unpacked-mcap-compression none
~~~

## Plain Folder Support

For datasets that are not MCAP bagfiles (plain folders, non-ROS data), PointCloud2 compression is not applied. Archive and MCAP-level settings have no effect on non-MCAP content, and the data is stored as-is inside the archive.
