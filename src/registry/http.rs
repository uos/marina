use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use glob::Pattern;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::StatusCode;
use reqwest::blocking::Client;
use serde::Deserialize;

use crate::model::bag_ref::BagRef;
use crate::registry::driver::{PushMeta, RegistryDriver, RemoteDescriptor};

#[derive(Debug, Clone)]
pub struct HttpRegistry {
    pub name: String,
    base_url: String,
    client: Client,
}

#[derive(Debug, Clone, Deserialize)]
struct MetaFile {
    bag: BagRef,
    original_bytes: u64,
    packed_bytes: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct HttpIndexEntry {
    bag: BagRef,
    #[serde(default)]
    original_bytes: Option<u64>,
    #[serde(default)]
    packed_bytes: Option<u64>,
    #[serde(default)]
    bundle_url: Option<String>,
    #[serde(default)]
    metadata_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum HttpIndexFile {
    Array(Vec<HttpIndexEntry>),
    Object { bags: Vec<HttpIndexEntry> },
}

impl HttpRegistry {
    pub fn from_uri(name: &str, uri: &str) -> Result<Self> {
        if !uri.starts_with("http://") && !uri.starts_with("https://") {
            return Err(anyhow!(
                "http registry URI must start with http:// or https://"
            ));
        }
        let base_url = uri.trim_end_matches('/').to_string();
        Ok(Self {
            name: name.to_string(),
            base_url,
            client: Client::builder()
                .connect_timeout(Duration::from_secs(30))
                .timeout(Duration::from_secs(60 * 60))
                .build()?,
        })
    }

    fn index_url(&self) -> String {
        format!("{}/index.json", self.base_url)
    }

    fn object_base_url(&self, bag: &BagRef) -> String {
        format!("{}/{}", self.base_url, bag.object_path())
    }

    fn default_bundle_url(&self, bag: &BagRef) -> String {
        format!("{}/bundle.marina.tar.gz", self.object_base_url(bag))
    }

    fn default_metadata_url(&self, bag: &BagRef) -> String {
        format!("{}/metadata.json", self.object_base_url(bag))
    }

    fn fetch_index(&self) -> Result<Vec<HttpIndexEntry>> {
        let resp = self
            .client
            .get(self.index_url())
            .send()
            .context("failed to fetch http registry index.json")?;

        if resp.status() == StatusCode::NOT_FOUND {
            return Ok(Vec::new());
        }

        let text = resp
            .error_for_status()
            .context("http registry index request failed")?
            .text()
            .context("failed reading http registry index response")?;

        let parsed: HttpIndexFile =
            serde_json::from_str(&text).context("failed parsing http registry index.json")?;
        Ok(match parsed {
            HttpIndexFile::Array(items) => items,
            HttpIndexFile::Object { bags } => bags,
        })
    }

    fn find_index_entry(&self, bag: &BagRef) -> Result<Option<HttpIndexEntry>> {
        let target = bag.without_attachment();
        for item in self.fetch_index()? {
            if item.bag.without_attachment() == target {
                return Ok(Some(item));
            }
        }
        Ok(None)
    }

    fn download_file_with_progress(&self, url: &str, out: &Path, title: &str) -> Result<u64> {
        let mut resp = self
            .client
            .get(url)
            .send()
            .with_context(|| format!("failed downloading {}", title))?
            .error_for_status()
            .with_context(|| format!("download failed for {}", title))?;

        let total = resp.content_length().unwrap_or(0);
        let pb = if total > 0 {
            let pb = ProgressBar::new(total);
            pb.set_style(
                ProgressStyle::with_template(
                    "{msg} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})",
                )
                .unwrap_or_else(|_| ProgressStyle::default_bar()),
            );
            pb.set_message(title.to_string());
            Some(pb)
        } else {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::with_template("{spinner} {msg}")
                    .unwrap_or_else(|_| ProgressStyle::default_spinner())
                    .tick_chars("|/-\\ "),
            );
            pb.set_message(title.to_string());
            pb.enable_steady_tick(std::time::Duration::from_millis(100));
            Some(pb)
        };

        if let Some(parent) = out.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = fs::File::create(out)
            .with_context(|| format!("failed creating output file {}", out.display()))?;

        let mut downloaded = 0u64;
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = resp.read(&mut buf)?;
            if n == 0 {
                break;
            }
            file.write_all(&buf[..n])?;
            downloaded += n as u64;
            if let Some(pb) = &pb {
                pb.inc(n as u64);
            }
        }
        if let Some(pb) = pb {
            pb.finish_and_clear();
        }
        Ok(downloaded)
    }

    fn fetch_metadata(&self, url: &str) -> Result<MetaFile> {
        let text = self
            .client
            .get(url)
            .send()
            .with_context(|| format!("failed downloading metadata {}", url))?
            .error_for_status()
            .with_context(|| format!("metadata request failed for {}", url))?
            .text()
            .context("failed reading metadata response")?;
        let meta: MetaFile = serde_json::from_str(&text)
            .with_context(|| format!("failed parsing metadata json from {}", url))?;
        Ok(meta)
    }
}

impl RegistryDriver for HttpRegistry {
    fn push(
        &self,
        _registry_name: &str,
        _bag: &BagRef,
        _packed_file: &Path,
        _meta: &PushMeta,
    ) -> Result<()> {
        Err(anyhow!(
            "http registry '{}' is read-only: push is not supported",
            self.name
        ))
    }

    fn pull(&self, bag: &BagRef, out_packed_file: &Path) -> Result<RemoteDescriptor> {
        let target = bag.without_attachment();
        let entry = self.find_index_entry(&target)?;

        let bundle_url = entry
            .as_ref()
            .and_then(|e| e.bundle_url.clone())
            .unwrap_or_else(|| self.default_bundle_url(&target));
        let metadata_url = entry
            .as_ref()
            .and_then(|e| e.metadata_url.clone())
            .unwrap_or_else(|| self.default_metadata_url(&target));

        let downloaded =
            self.download_file_with_progress(&bundle_url, out_packed_file, &format!("{}", target))?;

        let descriptor = match self.fetch_metadata(&metadata_url) {
            Ok(meta) => RemoteDescriptor {
                registry_name: self.name.clone(),
                bag: meta.bag.without_attachment(),
                original_bytes: meta.original_bytes,
                packed_bytes: meta.packed_bytes,
            },
            Err(_) => RemoteDescriptor {
                registry_name: self.name.clone(),
                bag: target,
                original_bytes: entry
                    .as_ref()
                    .and_then(|e| e.original_bytes)
                    .unwrap_or(downloaded),
                packed_bytes: entry
                    .as_ref()
                    .and_then(|e| e.packed_bytes)
                    .unwrap_or(downloaded),
            },
        };

        Ok(descriptor)
    }

    fn list(&self, filter: &str) -> Result<Vec<BagRef>> {
        let pattern = Pattern::new(filter).or_else(|_| Pattern::new("*"))?;
        let items = self.fetch_index()?;
        if items.is_empty() {
            return Err(anyhow!(
                "http registry '{}' has no index.json; list/search is unavailable (pull by exact bag still works)",
                self.name
            ));
        }

        let mut out = Vec::new();
        for item in items {
            let bag = item.bag.without_attachment();
            if pattern.matches(&bag.to_string()) {
                out.push(bag);
            }
        }
        out.sort_by_key(|b| b.to_string());
        out.dedup();
        Ok(out)
    }

    fn remove(&self, _bag: &BagRef) -> Result<()> {
        Err(anyhow!(
            "http registry '{}' is read-only: remove is not supported",
            self.name
        ))
    }

    fn check_connection(&self) -> Result<()> {
        self.client
            .get(&self.base_url)
            .timeout(Duration::from_secs(5))
            .send()
            .context("failed checking http registry connectivity")?
            .error_for_status()
            .context("http registry connectivity check returned error")?;
        Ok(())
    }

    fn check_write_access(&self) -> Result<()> {
        Err(anyhow!(
            "http registry '{}' is read-only: push is not supported",
            self.name
        ))
    }
}
