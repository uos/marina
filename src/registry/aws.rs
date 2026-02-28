use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, anyhow};
use glob::Pattern;
use serde::{Deserialize, Serialize};

use crate::model::bag_ref::BagRef;
use crate::registry::driver::{RegistryDriver, RemoteDescriptor};

#[derive(Debug, Clone)]
pub struct AwsRegistry {
    pub name: String,
    bucket: String,
    prefix: String,
    auth_env: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetaFile {
    bag: BagRef,
    original_bytes: u64,
    packed_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct S3ListResponse {
    #[serde(rename = "Contents", default)]
    contents: Vec<S3Object>,
}

#[derive(Debug, Deserialize)]
struct S3Object {
    #[serde(rename = "Key")]
    key: String,
}

impl AwsRegistry {
    pub fn from_uri(name: &str, uri: &str, auth_env: Option<String>) -> Result<Self> {
        let raw = uri
            .strip_prefix("aws://")
            .or_else(|| uri.strip_prefix("s3://"))
            .ok_or_else(|| anyhow!("aws registry URI must start with aws:// or s3://"))?;

        let (bucket, prefix) = if let Some((b, p)) = raw.split_once('/') {
            (b.trim().to_string(), p.trim_matches('/').to_string())
        } else {
            (raw.trim().to_string(), String::new())
        };

        if bucket.is_empty() {
            return Err(anyhow!("aws/s3 URI must include bucket name"));
        }

        Ok(Self {
            name: name.to_string(),
            bucket,
            prefix,
            auth_env,
        })
    }

    fn key_base(&self, bag: &BagRef) -> String {
        if self.prefix.is_empty() {
            bag.object_path()
        } else {
            format!("{}/{}", self.prefix, bag.object_path())
        }
    }

    fn bundle_key(&self, bag: &BagRef) -> String {
        format!("{}/bundle.marina.tar.gz", self.key_base(bag))
    }

    fn metadata_key(&self, bag: &BagRef) -> String {
        format!("{}/metadata.json", self.key_base(bag))
    }

    fn s3_uri(&self, key: &str) -> String {
        format!("s3://{}/{}", self.bucket, key)
    }

    fn run_aws(&self, args: &[&str]) -> Result<()> {
        let mut cmd = Command::new("aws");
        cmd.args(args);
        if let Some(var) = &self.auth_env
            && let Ok(value) = std::env::var(var)
        {
            cmd.env(var, value);
        }
        let output = cmd.output().context("failed to execute aws cli")?;
        if !output.status.success() {
            return Err(anyhow!(
                "aws cli failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Ok(())
    }

    fn run_aws_capture(&self, args: &[&str]) -> Result<String> {
        let mut cmd = Command::new("aws");
        cmd.args(args);
        if let Some(var) = &self.auth_env
            && let Ok(value) = std::env::var(var)
        {
            cmd.env(var, value);
        }
        let output = cmd.output().context("failed to execute aws cli")?;
        if !output.status.success() {
            return Err(anyhow!(
                "aws cli failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    fn download_metadata_file(&self, key: &str) -> Result<MetaFile> {
        let tmp = std::env::temp_dir().join(format!(
            "marina_aws_meta_{}_{}.json",
            std::process::id(),
            key.replace('/', "_")
        ));

        let src = self.s3_uri(key);
        let dst = tmp
            .to_str()
            .ok_or_else(|| anyhow!("invalid temp path for metadata download"))?
            .to_string();
        self.run_aws(&["s3", "cp", &src, &dst, "--only-show-errors"])?;

        let text = fs::read_to_string(&tmp)
            .with_context(|| format!("failed reading {}", tmp.display()))?;
        let _ = fs::remove_file(&tmp);
        let meta: MetaFile = serde_json::from_str(&text)
            .with_context(|| format!("failed parsing metadata object {}", key))?;
        Ok(meta)
    }

    fn remove_prefix(&self, bag: &BagRef) -> Result<()> {
        let prefix = self.key_base(bag);
        self.run_aws(&[
            "s3",
            "rm",
            &self.s3_uri(&prefix),
            "--recursive",
            "--only-show-errors",
        ])
    }
}

impl RegistryDriver for AwsRegistry {
    fn push(
        &self,
        _registry_name: &str,
        bag: &BagRef,
        packed_file: &Path,
        original_bytes: u64,
        packed_bytes: u64,
    ) -> Result<()> {
        self.remove_prefix(bag)?;

        let local_bundle = packed_file
            .to_str()
            .ok_or_else(|| anyhow!("invalid local packed path"))?;
        let remote_bundle = self.s3_uri(&self.bundle_key(bag));
        self.run_aws(&[
            "s3",
            "cp",
            local_bundle,
            &remote_bundle,
            "--only-show-errors",
        ])?;

        let meta = MetaFile {
            bag: bag.clone().without_attachment(),
            original_bytes,
            packed_bytes,
        };
        let tmp_meta = std::env::temp_dir().join(format!(
            "marina_aws_meta_upload_{}_{}.json",
            std::process::id(),
            bag.cache_key()
        ));
        fs::write(&tmp_meta, serde_json::to_vec_pretty(&meta)?)?;

        let local_meta = tmp_meta
            .to_str()
            .ok_or_else(|| anyhow!("invalid local metadata temp path"))?
            .to_string();
        let remote_meta = self.s3_uri(&self.metadata_key(bag));
        self.run_aws(&["s3", "cp", &local_meta, &remote_meta, "--only-show-errors"])?;
        let _ = fs::remove_file(tmp_meta);

        Ok(())
    }

    fn pull(&self, bag: &BagRef, out_packed_file: &Path) -> Result<RemoteDescriptor> {
        if let Some(parent) = out_packed_file.parent() {
            fs::create_dir_all(parent)?;
        }
        let dst = out_packed_file
            .to_str()
            .ok_or_else(|| anyhow!("invalid local output path"))?
            .to_string();
        let src = self.s3_uri(&self.bundle_key(bag));
        self.run_aws(&["s3", "cp", &src, &dst, "--only-show-errors"])?;

        let meta = self.download_metadata_file(&self.metadata_key(bag))?;
        Ok(RemoteDescriptor {
            registry_name: self.name.clone(),
            bag: meta.bag,
            original_bytes: meta.original_bytes,
            packed_bytes: meta.packed_bytes,
        })
    }

    fn list(&self, filter: &str) -> Result<Vec<BagRef>> {
        let pattern = Pattern::new(filter).or_else(|_| Pattern::new("*"))?;
        let prefix = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("{}/", self.prefix)
        };

        let output = self.run_aws_capture(&[
            "s3api",
            "list-objects-v2",
            "--bucket",
            &self.bucket,
            "--prefix",
            &prefix,
            "--output",
            "json",
        ])?;

        let listed: S3ListResponse = serde_json::from_str(&output)
            .context("failed parsing aws s3api list-objects-v2 output")?;

        let mut out = Vec::new();
        for obj in listed.contents {
            if !obj.key.ends_with("/metadata.json") {
                continue;
            }
            let meta = self.download_metadata_file(&obj.key)?;
            let bag = meta.bag.without_attachment();
            if pattern.matches(&bag.to_string()) {
                out.push(bag);
            }
        }

        out.sort_by_key(|b| b.to_string());
        Ok(out)
    }

    fn remove(&self, bag: &BagRef) -> Result<()> {
        self.remove_prefix(bag)
    }
}
