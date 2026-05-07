use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

#[cfg(unix)]
fn available_space(path: &Path) -> Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    let c_path = CString::new(path.as_os_str().as_bytes()).context("path contains null byte")?;
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    let ret = unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) };
    if ret != 0 {
        return Err(anyhow!("statvfs failed for {}", path.display()));
    }
    #[allow(clippy::unnecessary_cast)]
    Ok((stat.f_bavail as u64) * (stat.f_frsize as u64))
}

#[cfg(not(unix))]
fn available_space(_path: &Path) -> Result<u64> {
    // Not implemented on non-Unix; skip the check.
    Ok(u64::MAX)
}

fn format_bytes(bytes: u64) -> String {
    const GB: u64 = 1 << 30;
    const MB: u64 = 1 << 20;
    const KB: u64 = 1 << 10;
    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

use anyhow::{Context, Result, anyhow};
use flate2::read::GzDecoder;
use flate2::{Compression, GzBuilder};
use indicatif::ProgressBar;
use serde::{Deserialize, Serialize};
use tar::{Archive, Builder, EntryType};

use crate::io::bag::BagSource;
#[cfg(feature = "db3")]
use crate::io::db3_transform::{self, Db3TransformOptions};
use crate::io::mcap_transform;
#[cfg(feature = "db3")]
use crate::io::mcap_transform::PointCloudCompressionMode;
use crate::io::mcap_transform::{McapChunkCompression, PullTransformOptions, PushTransformOptions};
use crate::io::transform_progress::make_byte_progress_bar;
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

fn inspect_downloaded_archive(path: &Path) -> Result<()> {
    let mut file =
        File::open(path).with_context(|| format!("cannot open archive {}", path.display()))?;
    let mut buf = vec![0u8; 4096];
    let read = file
        .read(&mut buf)
        .with_context(|| format!("cannot read archive header {}", path.display()))?;
    buf.truncate(read);
    if buf.is_empty() {
        return Err(anyhow!("downloaded archive is empty: {}", path.display()));
    }

    let head = String::from_utf8_lossy(&buf).to_ascii_lowercase();
    let looks_like_html = head.contains("<!doctype html")
        || head.contains("<html")
        || head.contains("<head>")
        || head.contains("<body");
    if !looks_like_html {
        return Ok(());
    }

    if head.contains("google drive") && head.contains("quota exceeded") {
        return Err(anyhow!(
            "downloaded file is a Google Drive quota page, not an archive. \
             The registry file likely exceeded public download limits; try again later \
             or pull with authenticated Google Drive access (`marina registry auth <registry>`)."
        ));
    }

    Err(anyhow!(
        "downloaded file looks like HTML, not a tar archive: {}",
        path.display()
    ))
}

fn append_staging_bundle<W: Write>(
    builder: &mut Builder<W>,
    staging_dir: &Path,
    pb: &ProgressBar,
    total_bytes: u64,
    total_files: u64,
    progress: &mut ProgressReporter<'_>,
) -> Result<u64> {
    append_dir_header(builder, Path::new("bundle"))?;

    let mut rel_dirs: Vec<PathBuf> = Vec::new();
    let mut rel_files: Vec<PathBuf> = Vec::new();

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

        if path.is_dir() {
            rel_dirs.push(rel.to_path_buf());
        } else {
            rel_files.push(rel.to_path_buf());
        }
    }

    rel_dirs.sort();
    rel_files.sort();

    for rel in &rel_dirs {
        let archive_path = Path::new("bundle").join(rel);
        append_dir_header(builder, &archive_path)?;
    }

    let mut packed_files = 0u64;
    for rel in &rel_files {
        let path = staging_dir.join(rel);
        let archive_path = Path::new("bundle").join(rel);

        let metadata = fs::metadata(&path)?;
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(EntryType::Regular);
        header.set_mode(normalized_file_mode(&metadata));
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_size(metadata.len());
        header.set_cksum();

        let mut file = File::open(&path)?;
        if !pb.is_hidden() && total_bytes > 0 {
            let mut reader = ProgressReader::new(file, pb.clone());
            builder.append_data(&mut header, &archive_path, &mut reader)?;
        } else {
            builder.append_data(&mut header, &archive_path, &mut file)?;
        }

        packed_files += 1;
        if !pb.is_hidden() && total_bytes == 0 {
            pb.tick();
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

fn append_dir_header<W: Write>(builder: &mut Builder<W>, archive_path: &Path) -> Result<()> {
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(EntryType::Directory);
    header.set_mode(0o755);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_size(0);
    header.set_cksum();
    builder.append_data(&mut header, archive_path, io::empty())?;
    Ok(())
}

fn normalized_file_mode(metadata: &fs::Metadata) -> u32 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        metadata.permissions().mode() & 0o777
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        0o644
    }
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
    let parent = out_file
        .parent()
        .context("packed output file has no parent dir")?;
    fs::create_dir_all(parent)?;

    let skip_push_transform = options.transform.pointcloud_mode
        == mcap_transform::PointCloudCompressionMode::Disabled
        && options.transform.output_mcap_compression == McapChunkCompression::None;

    // Staging (copying source to a temp dir) is only needed when transforms must be applied
    // in-place, or when the source is a single file (so the archive root contains the file,
    // not the parent directory).
    #[cfg(feature = "db3")]
    let needs_staging = (source.mcap.is_some() && !skip_push_transform)
        || source.root.is_file()
        || (source.has_db3
            && options.transform.pointcloud_mode
                != mcap_transform::PointCloudCompressionMode::Disabled);
    #[cfg(not(feature = "db3"))]
    let needs_staging = (source.mcap.is_some() && !skip_push_transform) || source.root.is_file();

    // Check available space: bundle ≈ bag size (assuming no compression benefit).
    // If staging is needed, the staging copy and the bundle coexist on the same filesystem.
    let required_space = if needs_staging {
        source.original_bytes * 2
    } else {
        source.original_bytes
    };
    let avail = available_space(parent)?;
    if avail < required_space {
        let missing = required_space - avail;
        return Err(anyhow!(
            "not enough disk space in {}: need {} more ({} required, {} available)",
            parent.display(),
            format_bytes(missing),
            format_bytes(required_space),
            format_bytes(avail),
        ));
    }

    let (pack_dir, staging_is_temp) = if needs_staging {
        progress.emit(
            "pack",
            format!(
                "staging dataset directory from {}",
                source.root.as_path().display()
            ),
        );
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
            if skip_push_transform {
                progress.emit(
                    "pack",
                    "skipping MCAP rewrite (pointcloud + chunk compression disabled)",
                );
            } else {
                mcap_transform::compress_mcap_for_push_with_progress(
                    &staged_mcap,
                    &transformed_mcap,
                    options.transform,
                    progress,
                )?;
                fs::rename(&transformed_mcap, &staged_mcap)?;
            }
        }
        #[cfg(feature = "db3")]
        if source.has_db3
            && options.transform.pointcloud_mode != PointCloudCompressionMode::Disabled
        {
            for db3_path in find_all_db3(&staging_dir)? {
                db3_transform::compress_db3_for_push(
                    &db3_path,
                    &Db3TransformOptions {
                        pointcloud_mode: options.transform.pointcloud_mode,
                        pointcloud_precision_m: options.transform.pointcloud_precision_m,
                    },
                    progress,
                )?;
            }
        }

        (staging_dir, true)
    } else {
        (source.root.clone(), false)
    };

    progress.emit("pack", "compressing staged folder to marina archive");

    let mut total_bytes = 0u64;
    let mut total_files = 0u64;
    for entry in walkdir::WalkDir::new(&pack_dir) {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            total_files += 1;
            total_bytes += fs::metadata(path)?.len();
        }
    }

    let pb = make_byte_progress_bar(total_bytes, format!("packing {} file(s)", total_files));

    match options.archive_compression {
        ArchiveCompression::Gzip => {
            let tar_gz = File::create(out_file)?;
            let encoder = GzBuilder::new().mtime(0).write(tar_gz, Compression::best());
            let mut builder = Builder::new(encoder);
            append_staging_bundle(
                &mut builder,
                &pack_dir,
                &pb,
                total_bytes,
                total_files,
                progress,
            )?;
            let encoder = builder.into_inner()?;
            encoder.finish()?;
        }
        ArchiveCompression::None => {
            let tar_file = File::create(out_file)?;
            let mut builder = Builder::new(tar_file);
            append_staging_bundle(
                &mut builder,
                &pack_dir,
                &pb,
                total_bytes,
                total_files,
                progress,
            )?;
            let _file = builder.into_inner()?;
        }
    };

    if !pb.is_hidden() {
        pb.finish_and_clear();
    }

    if staging_is_temp {
        progress.emit("pack", "cleaning temporary staging files");
        fs::remove_dir_all(&pack_dir)?;
    }

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
    inspect_downloaded_archive(archive_path)?;
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
    fs::remove_file(archive_path)
        .with_context(|| format!("failed to remove archive {}", archive_path.display()))?;

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
        #[cfg(feature = "db3")]
        {
            let db3_files = find_all_db3(out_dir)?;
            if let Some(first) = db3_files.first() {
                if db3_transform::has_marina_pointcloud_metadata(first)? {
                    for db3 in &db3_files {
                        db3_transform::decompress_db3_after_pull(db3, progress)?;
                    }
                }
            }
        }
    } else {
        progress.emit(
            "unpack",
            "no MCAP/DB3 detected, skipping ROS-specific restore pipeline",
        );
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

#[cfg(feature = "db3")]
fn find_all_db3(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in walkdir::WalkDir::new(root) {
        let entry = entry?;
        let p = entry.path();
        if p.is_file() && p.extension().and_then(|e| e.to_str()) == Some("db3") {
            out.push(p.to_path_buf());
        }
    }
    Ok(out)
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
