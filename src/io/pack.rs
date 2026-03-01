use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};
use tar::{Archive, Builder};

use crate::io::bag::BagSource;
use crate::io::mcap_transform;
use crate::io::mcap_transform::{PullTransformOptions, PushTransformOptions};
use crate::progress::ProgressReporter;

#[derive(Debug, Clone, Copy, Default)]
pub struct PackOptions {
    pub transform: PushTransformOptions,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct UnpackOptions {
    pub transform: PullTransformOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackedMeta {
    pub original_bytes: u64,
    pub packed_bytes: u64,
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

    let mcap_rel = if source.root.is_file() {
        PathBuf::from(
            source
                .mcap
                .file_name()
                .ok_or_else(|| anyhow!("mcap file has no file name"))?,
        )
    } else {
        source
            .mcap
            .strip_prefix(&source.root)
            .with_context(|| {
                format!(
                    "{} is outside {}",
                    source.mcap.display(),
                    source.root.display()
                )
            })?
            .to_path_buf()
    };
    let staged_mcap = staging_dir.join(mcap_rel);
    let transformed_mcap = staging_dir.join(".marina_transform.mcap");
    progress.emit("pack", "rewriting MCAP messages");
    mcap_transform::compress_mcap_for_push_with_progress(
        &staged_mcap,
        &transformed_mcap,
        options.transform,
        progress,
    )?;
    fs::rename(&transformed_mcap, &staged_mcap)?;

    progress.emit("pack", "compressing staged folder to marina archive");
    let tar_gz = File::create(out_file)?;
    let encoder = GzEncoder::new(tar_gz, Compression::best());
    let mut builder = Builder::new(encoder);
    builder.append_dir_all("bundle", &staging_dir)?;
    let encoder = builder.into_inner()?;
    encoder.finish()?;

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
    let tar_gz = File::open(archive_path)
        .with_context(|| format!("cannot open archive {}", archive_path.display()))?;
    let decoder = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(decoder);
    archive.unpack(out_dir)?;

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

    progress.emit("unpack", "restoring PointCloud2 messages");
    let mcap_file = find_first_mcap(out_dir)?;
    let decoded = out_dir.join(".marina_restored.mcap");
    mcap_transform::decompress_mcap_after_pull_with_progress(
        &mcap_file,
        &decoded,
        options.transform,
        progress,
    )?;
    fs::rename(decoded, mcap_file)?;
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

fn find_first_mcap(root: &Path) -> Result<PathBuf> {
    for entry in walkdir::WalkDir::new(root) {
        let entry = entry?;
        let p = entry.path();
        if p.is_file() && p.extension().and_then(|e| e.to_str()) == Some("mcap") {
            return Ok(p.to_path_buf());
        }
    }
    Err(anyhow!("no .mcap file found in {}", root.display()))
}
