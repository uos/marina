use std::fs::{self, File};
use std::io::{self, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use tar::{Archive, Builder};

use crate::io::bag::{BagSource, MCAP_CLI_URL, MCAP_ROS2_URL};
use crate::io::mcap_transform;
use crate::io::mcap_transform::{McapChunkCompression, PullTransformOptions, PushTransformOptions};
use crate::progress::ProgressReporter;

#[derive(Debug, Clone, Copy, Default)]
pub struct PackOptions {
    pub transform: PushTransformOptions,
    pub archive_compression: ArchiveCompression,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct UnpackOptions {
    pub transform: PullTransformOptions,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ArchiveCompression {
    #[default]
    Gzip,
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackedMeta {
    pub original_bytes: u64,
    pub packed_bytes: u64,
}

struct ProgressReader<R: Read> {
    inner: R,
    progress: ProgressBar,
}

impl<R: Read> ProgressReader<R> {
    fn new(inner: R, progress: ProgressBar) -> Self {
        Self { inner, progress }
    }
}

impl<R: Read> Read for ProgressReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        if read > 0 {
            self.progress.inc(read as u64);
        }
        Ok(read)
    }
}

fn is_gzip_archive(path: &Path) -> Result<bool> {
    let mut file =
        File::open(path).with_context(|| format!("cannot open archive {}", path.display()))?;
    let mut magic = [0u8; 2];
    let read = file
        .read(&mut magic)
        .with_context(|| format!("cannot read archive header {}", path.display()))?;
    if read < 2 {
        return Ok(false);
    }
    Ok(magic == [0x1f, 0x8b])
}

fn append_staging_bundle<W: Write>(
    builder: &mut Builder<W>,
    staging_dir: &Path,
    pb: &ProgressBar,
    total_bytes: u64,
    total_files: u64,
    progress: &mut ProgressReporter<'_>,
) -> Result<u64> {
    builder.append_dir("bundle", staging_dir)?;

    let mut packed_files = 0u64;
    for entry in walkdir::WalkDir::new(staging_dir) {
        let entry = entry?;
        let path = entry.path();
        let rel = path.strip_prefix(staging_dir).with_context(|| {
            format!(
                "failed to strip staging prefix {} from {}",
                staging_dir.display(),
                path.display()
            )
        })?;

        if rel.as_os_str().is_empty() {
            continue;
        }

        let archive_path = Path::new("bundle").join(rel);
        if path.is_dir() {
            builder.append_dir(&archive_path, path)?;
            continue;
        }

        let metadata = fs::metadata(path)?;
        let mut header = tar::Header::new_gnu();
        header.set_metadata(&metadata);
        header.set_cksum();

        let mut file = File::open(path)?;
        if !pb.is_hidden() && total_bytes > 0 {
            let mut reader = ProgressReader::new(file, pb.clone());
            builder.append_data(&mut header, &archive_path, &mut reader)?;
        } else {
            builder.append_data(&mut header, &archive_path, &mut file)?;
        }

        packed_files += 1;
        if !pb.is_hidden() {
            if total_bytes == 0 {
                pb.tick();
            }
        }

        if pb.is_hidden()
            && (packed_files == 1 || packed_files % 512 == 0 || packed_files == total_files)
        {
            progress.emit(
                "pack",
                format!(
                    "archive packing progress: {}/{} file(s)",
                    packed_files, total_files
                ),
            );
        }
    }

    Ok(packed_files)
}

pub fn pack_bag(source: &BagSource, out_file: &Path) -> Result<PackedMeta> {
    let mut reporter = ProgressReporter::silent();
    pack_bag_with_progress_and_options(source, out_file, PackOptions::default(), &mut reporter)
}

pub fn pack_bag_with_progress(
    source: &BagSource,
    out_file: &Path,
    progress: &mut ProgressReporter<'_>,
) -> Result<PackedMeta> {
    pack_bag_with_progress_and_options(source, out_file, PackOptions::default(), progress)
}

pub fn pack_bag_with_progress_and_options(
    source: &BagSource,
    out_file: &Path,
    options: PackOptions,
    progress: &mut ProgressReporter<'_>,
) -> Result<PackedMeta> {
    progress.emit(
        "pack",
        format!(
            "staging bag directory from {}",
            source.root.as_path().display()
        ),
    );
    let parent = out_file
        .parent()
        .context("packed output file has no parent dir")?;
    fs::create_dir_all(parent)?;
    let staging_dir = parent.join("staging_push_bundle");
    if staging_dir.exists() {
        fs::remove_dir_all(&staging_dir)?;
    }
    fs::create_dir_all(&staging_dir)?;
    if source.root.is_file() {
        let file_name = source
            .root
            .file_name()
            .ok_or_else(|| anyhow!("source mcap file has no file name"))?;
        fs::copy(&source.root, staging_dir.join(file_name)).with_context(|| {
            format!(
                "failed to stage source mcap file {}",
                source.root.as_path().display()
            )
        })?;
    } else {
        copy_dir(&source.root, &staging_dir)?;
    }

    if let Some(source_mcap) = source.mcap.as_ref() {
        let mcap_rel = if source.root.is_file() {
            PathBuf::from(
                source_mcap
                    .file_name()
                    .ok_or_else(|| anyhow!("mcap file has no file name"))?,
            )
        } else {
            source_mcap
                .strip_prefix(&source.root)
                .with_context(|| {
                    format!(
                        "{} is outside {}",
                        source_mcap.display(),
                        source.root.display()
                    )
                })?
                .to_path_buf()
        };
        let staged_mcap = staging_dir.join(mcap_rel);
        let transformed_mcap = staging_dir.join(".marina_transform.mcap");
        let skip_push_transform = options.transform.pointcloud_mode
            == mcap_transform::PointCloudCompressionMode::Disabled
            && options.transform.output_mcap_compression == McapChunkCompression::None;

        if skip_push_transform {
            progress.emit(
                "pack",
                "skipping MCAP rewrite (pointcloud + chunk compression disabled)",
            );
        } else {
            progress.emit("pack", "rewriting MCAP messages");
            mcap_transform::compress_mcap_for_push_with_progress(
                &staged_mcap,
                &transformed_mcap,
                options.transform,
                progress,
            )?;
            fs::rename(&transformed_mcap, &staged_mcap)?;
        }
    } else if source.has_db3 {
        progress.emit(
            "pack",
            format!(
                "WARNING: ROS2 .db3 source detected; skipping MCAP optimization pipeline. This is supported but significantly less efficient than MCAP for transfer/storage/playback. Strongly recommended: convert to MCAP first ({} / {}).",
                MCAP_CLI_URL, MCAP_ROS2_URL
            ),
        );
    }

    progress.emit("pack", "compressing staged folder to marina archive");

    let mut total_bytes = 0u64;
    let mut total_files = 0u64;
    for entry in walkdir::WalkDir::new(&staging_dir) {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            total_files += 1;
            total_bytes += fs::metadata(path)?.len();
        }
    }

    let pb = if std::io::stderr().is_terminal() {
        let pb = if total_bytes > 0 {
            ProgressBar::new(total_bytes)
        } else {
            ProgressBar::new_spinner()
        };
        pb.set_style(
            ProgressStyle::with_template(
                "packing archive [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})",
            )
            .unwrap_or_else(|_| ProgressStyle::default_bar()),
        );
        pb.set_message(format!("packing {} file(s)", total_files));
        if total_bytes == 0 {
            pb.enable_steady_tick(std::time::Duration::from_millis(100));
        }
        pb
    } else {
        ProgressBar::hidden()
    };

    let packed_files = match options.archive_compression {
        ArchiveCompression::Gzip => {
            let tar_gz = File::create(out_file)?;
            let encoder = GzEncoder::new(tar_gz, Compression::best());
            let mut builder = Builder::new(encoder);
            let packed_files = append_staging_bundle(
                &mut builder,
                &staging_dir,
                &pb,
                total_bytes,
                total_files,
                progress,
            )?;
            let encoder = builder.into_inner()?;
            encoder.finish()?;
            packed_files
        }
        ArchiveCompression::None => {
            let tar_file = File::create(out_file)?;
            let mut builder = Builder::new(tar_file);
            let packed_files = append_staging_bundle(
                &mut builder,
                &staging_dir,
                &pb,
                total_bytes,
                total_files,
                progress,
            )?;
            let _file = builder.into_inner()?;
            packed_files
        }
    };

    if !pb.is_hidden() {
        if total_bytes > 0 {
            pb.finish_with_message(format!(
                "packing archive complete: {}/{} file(s)",
                packed_files, total_files
            ));
        } else {
            pb.finish_with_message("packing archive complete".to_string());
        }
    }

    progress.emit("pack", "cleaning temporary staging files");
    fs::remove_dir_all(staging_dir)?;

    let packed_bytes = fs::metadata(out_file)?.len();
    Ok(PackedMeta {
        original_bytes: source.original_bytes,
        packed_bytes,
    })
}

pub fn unpack_bag(archive_path: &Path, out_dir: &Path) -> Result<()> {
    let mut reporter = ProgressReporter::silent();
    unpack_bag_with_progress_and_options(
        archive_path,
        out_dir,
        UnpackOptions::default(),
        &mut reporter,
    )
}

pub fn unpack_bag_with_progress(
    archive_path: &Path,
    out_dir: &Path,
    progress: &mut ProgressReporter<'_>,
) -> Result<()> {
    unpack_bag_with_progress_and_options(archive_path, out_dir, UnpackOptions::default(), progress)
}

pub fn unpack_bag_with_progress_and_options(
    archive_path: &Path,
    out_dir: &Path,
    options: UnpackOptions,
    progress: &mut ProgressReporter<'_>,
) -> Result<()> {
    progress.emit(
        "unpack",
        format!("extracting archive {}", archive_path.display()),
    );
    fs::create_dir_all(out_dir)?;
    let archive_is_gzip = is_gzip_archive(archive_path)?;
    if archive_is_gzip {
        let tar_gz = File::open(archive_path)
            .with_context(|| format!("cannot open archive {}", archive_path.display()))?;
        let decoder = GzDecoder::new(tar_gz);
        let mut archive = Archive::new(decoder);
        archive.unpack(out_dir)?;
    } else {
        let tar_file = File::open(archive_path)
            .with_context(|| format!("cannot open archive {}", archive_path.display()))?;
        let mut archive = Archive::new(tar_file);
        archive.unpack(out_dir)?;
    }

    let bundle = out_dir.join("bundle");
    if bundle.exists() {
        for entry in fs::read_dir(&bundle)? {
            let entry = entry?;
            let src = entry.path();
            let dst = out_dir.join(entry.file_name());
            if dst.exists() {
                if dst.is_dir() {
                    fs::remove_dir_all(&dst)?;
                } else {
                    fs::remove_file(&dst)?;
                }
            }
            move_path(&src, &dst)?;
        }
        fs::remove_dir_all(bundle)?;
    }

    if let Some(mcap_file) = find_first_mcap(out_dir)? {
        progress.emit("unpack", "restoring PointCloud2 messages");
        let has_cloudini_channels = mcap_transform::has_cloudini_pointcloud_metadata(&mcap_file)?;
        let skip_pull_transform = options.transform.output_mcap_compression
            == McapChunkCompression::None
            && !has_cloudini_channels;

        if skip_pull_transform {
            progress.emit("unpack", "skipping MCAP restore");
        } else {
            let decoded = out_dir.join(".marina_restored.mcap");
            mcap_transform::decompress_mcap_after_pull_with_progress(
                &mcap_file,
                &decoded,
                options.transform,
                progress,
            )?;
            fs::rename(decoded, mcap_file)?;
        }
    } else if has_any_db3(out_dir)? {
        progress.emit(
            "unpack",
            format!(
                "WARNING: pulled .db3 bag. Skipping restore and compression. MCAP is strongly recommended! ({} / {}).",
                MCAP_CLI_URL, MCAP_ROS2_URL
            ),
        );
    } else {
        return Err(anyhow!(
            "no .mcap or .db3 file found in {}",
            out_dir.display()
        ));
    }
    progress.emit("unpack", "unpack complete");

    Ok(())
}

fn move_path(src: &PathBuf, dst: &PathBuf) -> Result<()> {
    match fs::rename(src, dst) {
        Ok(_) => Ok(()),
        Err(_) => {
            if src.is_dir() {
                fs::create_dir_all(dst)?;
                for entry in fs::read_dir(src)? {
                    let entry = entry?;
                    let src_child = entry.path();
                    let dst_child = dst.join(entry.file_name());
                    move_path(&src_child, &dst_child)?;
                }
                fs::remove_dir_all(src)?;
            } else {
                let mut in_file = File::open(src)?;
                let mut out_file = File::create(dst)?;
                io::copy(&mut in_file, &mut out_file)?;
                fs::remove_file(src)?;
            }
            Ok(())
        }
    }
}

fn copy_dir(src: &Path, dst: &Path) -> Result<()> {
    if !src.exists() {
        return Err(anyhow!("source does not exist: {}", src.display()));
    }
    fs::create_dir_all(dst)?;
    for entry in walkdir::WalkDir::new(src) {
        let entry = entry?;
        let path = entry.path();
        let rel = path.strip_prefix(src).with_context(|| {
            format!(
                "failed to strip prefix {} from {}",
                src.display(),
                path.display()
            )
        })?;
        let target = dst.join(rel);
        if path.is_dir() {
            fs::create_dir_all(&target)?;
        } else {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(path, &target)?;
        }
    }
    Ok(())
}

fn find_first_mcap(root: &Path) -> Result<Option<PathBuf>> {
    for entry in walkdir::WalkDir::new(root) {
        let entry = entry?;
        let p = entry.path();
        if p.is_file() && p.extension().and_then(|e| e.to_str()) == Some("mcap") {
            return Ok(Some(p.to_path_buf()));
        }
    }
    Ok(None)
}

fn has_any_db3(root: &Path) -> Result<bool> {
    for entry in walkdir::WalkDir::new(root) {
        let entry = entry?;
        let p = entry.path();
        if p.is_file() && p.extension().and_then(|e| e.to_str()) == Some("db3") {
            return Ok(true);
        }
    }
    Ok(false)
}
