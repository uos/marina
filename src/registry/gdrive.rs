use std::fs;
use std::io::{Read, Write};
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use glob::Pattern;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::blocking::{Client, multipart};
use serde::{Deserialize, Serialize};

use crate::model::bag_ref::BagRef;
use crate::registry::driver::{RegistryDriver, RemoteDescriptor};

const DRIVE_FILES_API: &str = "https://www.googleapis.com/drive/v3/files";
const DRIVE_UPLOAD_API: &str = "https://www.googleapis.com/upload/drive/v3/files";

#[derive(Debug, Clone)]
pub struct GDriveRegistry {
    pub name: String,
    folder_id: String,
    token_env: Option<String>,
    client: Client,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetaFile {
    bag: BagRef,
    original_bytes: u64,
    packed_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct FilesListResponse {
    files: Vec<DriveFile>,
}

#[derive(Debug, Deserialize)]
struct DriveFile {
    id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct DriveCreateResponse {
    id: String,
}

impl GDriveRegistry {
    pub fn from_uri(name: &str, uri: &str, auth_env: Option<String>) -> Result<Self> {
        let folder_id = uri
            .strip_prefix("gdrive://")
            .ok_or_else(|| anyhow!("gdrive registry URI must start with gdrive://"))?
            .trim()
            .to_string();
        if folder_id.is_empty() {
            return Err(anyhow!(
                "gdrive URI must include a folder id: gdrive://<folder_id>"
            ));
        }
        Ok(Self {
            name: name.to_string(),
            folder_id,
            token_env: auth_env,
            client: Client::builder().build()?,
        })
    }

    fn object_stem(&self, bag: &BagRef) -> String {
        bag.object_path().replace('/', "__")
    }

    fn bundle_name(&self, bag: &BagRef) -> String {
        format!("{}.bundle.marina.tar.gz", self.object_stem(bag))
    }

    fn metadata_name(&self, bag: &BagRef) -> String {
        format!("{}.metadata.json", self.object_stem(bag))
    }

    fn token(&self) -> Result<String> {
        if let Some(var) = &self.token_env {
            return std::env::var(var)
                .with_context(|| format!("missing gdrive token env var '{}'", var));
        }
        std::env::var("GOOGLE_DRIVE_TOKEN")
            .context("missing GOOGLE_DRIVE_TOKEN env var (or set registry auth_env)")
    }

    fn auth_header(&self) -> Result<String> {
        Ok(format!("Bearer {}", self.token()?))
    }

    fn query_files(&self, query: &str) -> Result<Vec<DriveFile>> {
        let auth = self.auth_header()?;
        let resp = self
            .client
            .get(DRIVE_FILES_API)
            .header("Authorization", auth)
            .query(&[("q", query), ("fields", "files(id,name)")])
            .send()
            .context("failed querying Google Drive files")?
            .error_for_status()
            .context("Google Drive list query failed")?
            .json::<FilesListResponse>()
            .context("failed decoding Google Drive file list")?;
        Ok(resp.files)
    }

    fn delete_by_name(&self, name: &str) -> Result<()> {
        let q = format!(
            "'{}' in parents and trashed = false and name = '{}'",
            self.folder_id, name
        );
        for file in self.query_files(&q)? {
            self.delete_file(&file.id)?;
        }
        Ok(())
    }

    fn delete_file(&self, id: &str) -> Result<()> {
        let auth = self.auth_header()?;
        self.client
            .delete(format!("{}/{}", DRIVE_FILES_API, id))
            .header("Authorization", auth)
            .send()
            .with_context(|| format!("failed deleting gdrive file {}", id))?
            .error_for_status()
            .with_context(|| format!("Google Drive delete failed for {}", id))?;
        Ok(())
    }

    fn upload_named_bytes(&self, name: &str, mime: &str, bytes: Vec<u8>) -> Result<String> {
        let auth = self.auth_header()?;
        let metadata = serde_json::json!({
            "name": name,
            "parents": [self.folder_id],
        });

        let pb = spinner(&format!("uploading {} to Google Drive", name));
        let form = multipart::Form::new()
            .part(
                "metadata",
                multipart::Part::text(metadata.to_string())
                    .mime_str("application/json; charset=UTF-8")?,
            )
            .part(
                "file",
                multipart::Part::bytes(bytes)
                    .file_name(name.to_string())
                    .mime_str(mime)?,
            );

        let created = self
            .client
            .post(DRIVE_UPLOAD_API)
            .header("Authorization", auth)
            .query(&[("uploadType", "multipart"), ("fields", "id")])
            .multipart(form)
            .send()
            .with_context(|| format!("failed uploading {} to Google Drive", name))?
            .error_for_status()
            .with_context(|| format!("Google Drive upload failed for {}", name))?
            .json::<DriveCreateResponse>()
            .with_context(|| format!("failed decoding upload response for {}", name))?;
        pb.finish_and_clear();

        Ok(created.id)
    }

    fn download_file_to_path(&self, id: &str, out: &Path, title: &str) -> Result<()> {
        let auth = self.auth_header()?;
        let mut resp = self
            .client
            .get(format!("{}/{}", DRIVE_FILES_API, id))
            .header("Authorization", auth)
            .query(&[("alt", "media")])
            .send()
            .with_context(|| format!("failed downloading {} from Google Drive", id))?
            .error_for_status()
            .with_context(|| format!("Google Drive download failed for {}", id))?;

        let total = resp.content_length().unwrap_or(0);
        let pb = if total > 0 {
            let pb = ProgressBar::new(total);
            pb.set_style(
                ProgressStyle::with_template("{msg} [{bar:40.cyan/blue}] {bytes}/{total_bytes}")
                    .unwrap_or_else(|_| ProgressStyle::default_bar()),
            );
            pb.set_message(title.to_string());
            Some(pb)
        } else {
            Some(spinner(title))
        };

        let mut file = fs::File::create(out)
            .with_context(|| format!("failed creating output file {}", out.display()))?;
        let mut buf = [0u8; 1024 * 64];
        loop {
            let n = resp.read(&mut buf)?;
            if n == 0 {
                break;
            }
            file.write_all(&buf[..n])?;
            if let Some(pb) = &pb {
                pb.inc(n as u64);
            }
        }
        if let Some(pb) = pb {
            pb.finish_and_clear();
        }
        Ok(())
    }

    fn download_file_bytes(&self, id: &str) -> Result<Vec<u8>> {
        let auth = self.auth_header()?;
        let bytes = self
            .client
            .get(format!("{}/{}", DRIVE_FILES_API, id))
            .header("Authorization", auth)
            .query(&[("alt", "media")])
            .send()
            .with_context(|| format!("failed downloading {} from Google Drive", id))?
            .error_for_status()
            .with_context(|| format!("Google Drive download failed for {}", id))?
            .bytes()
            .context("failed reading Google Drive response bytes")?;
        Ok(bytes.to_vec())
    }

    fn find_single_by_name(&self, name: &str) -> Result<DriveFile> {
        let q = format!(
            "'{}' in parents and trashed = false and name = '{}'",
            self.folder_id, name
        );
        let mut files = self.query_files(&q)?;
        files
            .drain(..)
            .next()
            .ok_or_else(|| anyhow!("file '{}' not found in gdrive registry", name))
    }
}

impl RegistryDriver for GDriveRegistry {
    fn push(
        &self,
        _registry_name: &str,
        bag: &BagRef,
        packed_file: &Path,
        original_bytes: u64,
        packed_bytes: u64,
    ) -> Result<()> {
        let bundle_name = self.bundle_name(bag);
        let metadata_name = self.metadata_name(bag);

        self.delete_by_name(&bundle_name)?;
        self.delete_by_name(&metadata_name)?;

        let bundle_bytes = fs::read(packed_file)
            .with_context(|| format!("failed to read {}", packed_file.display()))?;
        self.upload_named_bytes(&bundle_name, "application/gzip", bundle_bytes)?;

        let metadata = MetaFile {
            bag: bag.clone().without_attachment(),
            original_bytes,
            packed_bytes,
        };
        let metadata_bytes = serde_json::to_vec_pretty(&metadata)?;
        self.upload_named_bytes(&metadata_name, "application/json", metadata_bytes)?;

        Ok(())
    }

    fn pull(&self, bag: &BagRef, out_packed_file: &Path) -> Result<RemoteDescriptor> {
        let bundle_name = self.bundle_name(bag);
        let metadata_name = self.metadata_name(bag);

        let bundle = self.find_single_by_name(&bundle_name)?;
        let metadata_file = self.find_single_by_name(&metadata_name)?;

        if let Some(parent) = out_packed_file.parent() {
            fs::create_dir_all(parent)?;
        }
        self.download_file_to_path(
            &bundle.id,
            out_packed_file,
            &format!("downloading {}", bundle.name),
        )?;

        let meta_bytes = self.download_file_bytes(&metadata_file.id)?;
        let meta: MetaFile = serde_json::from_slice(&meta_bytes)
            .context("failed parsing metadata.json from gdrive")?;

        Ok(RemoteDescriptor {
            registry_name: self.name.clone(),
            bag: meta.bag,
            original_bytes: meta.original_bytes,
            packed_bytes: meta.packed_bytes,
        })
    }

    fn list(&self, filter: &str) -> Result<Vec<BagRef>> {
        let pattern = Pattern::new(filter).or_else(|_| Pattern::new("*"))?;
        let q = format!(
            "'{}' in parents and trashed = false and name contains '.metadata.json'",
            self.folder_id
        );
        let files = self.query_files(&q)?;

        let mut out = Vec::new();
        for file in files {
            let bytes = self.download_file_bytes(&file.id)?;
            let meta: MetaFile = serde_json::from_slice(&bytes)
                .with_context(|| format!("failed parsing metadata file {}", file.name))?;
            let bag = meta.bag.without_attachment();
            if pattern.matches(&bag.to_string()) {
                out.push(bag);
            }
        }

        Ok(out)
    }

    fn remove(&self, bag: &BagRef) -> Result<()> {
        self.delete_by_name(&self.bundle_name(bag))?;
        self.delete_by_name(&self.metadata_name(bag))?;
        Ok(())
    }
}

fn spinner(message: &str) -> ProgressBar {
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{spinner} {msg}")
            .unwrap_or_else(|_| ProgressStyle::default_spinner())
            .tick_chars("|/-\\ "),
    );
    pb.set_message(message.to_string());
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb
}
