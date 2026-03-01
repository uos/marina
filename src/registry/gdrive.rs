use std::fs;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use glob::Pattern;
use indicatif::{ProgressBar, ProgressStyle};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use regex::Regex;
use reqwest::blocking::{Body, Client};
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PublicManifest {
    bag: BagRef,
    original_bytes: u64,
    packed_bytes: u64,
    bundle_file_id: String,
    metadata_file_id: String,
    bundle_url: String,
    metadata_url: String,
}

#[derive(Debug, Deserialize)]
struct FilesListResponse {
    files: Vec<DriveFile>,
}

#[derive(Debug, Deserialize, Clone)]
struct DriveFile {
    id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct DriveCreateResponse {
    id: String,
}

#[derive(Debug, Serialize)]
struct DriveCreateRequest {
    name: String,
    parents: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "mimeType")]
    mime_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ServiceAccountKey {
    client_email: String,
    private_key: String,
    #[serde(default = "default_token_uri")]
    token_uri: String,
}

#[derive(Debug, Serialize)]
struct ServiceAccountClaims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    exp: u64,
    iat: u64,
}

#[derive(Debug, Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
}

fn default_token_uri() -> String {
    "https://oauth2.googleapis.com/token".to_string()
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
            client: Client::builder()
                .connect_timeout(Duration::from_secs(30))
                .timeout(Duration::from_secs(60 * 60))
                .build()?,
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

    fn public_manifest_name(&self, bag: &BagRef) -> String {
        format!("{}.public.json", self.object_stem(bag))
    }

    fn auth_header_optional(&self) -> Result<Option<String>> {
        let raw = if let Some(var) = &self.token_env {
            std::env::var(var).ok()
        } else {
            std::env::var("GOOGLE_DRIVE_TOKEN").ok()
        };

        match raw {
            Some(secret) => Ok(Some(format!(
                "Bearer {}",
                self.resolve_access_token(secret.trim())?
            ))),
            None => Ok(None),
        }
    }

    fn auth_header_required(&self) -> Result<String> {
        self.auth_header_optional()?.ok_or_else(|| {
            anyhow!(
                "gdrive auth missing: set registry auth_env (token/service-account json) or GOOGLE_DRIVE_TOKEN"
            )
        })
    }

    fn api_key_optional(&self) -> Option<String> {
        std::env::var("GOOGLE_DRIVE_API_KEY").ok()
    }

    fn resolve_access_token(&self, secret: &str) -> Result<String> {
        if secret.is_empty() {
            return Err(anyhow!("empty gdrive auth secret"));
        }

        if let Some(sa) = self.try_load_service_account(secret)? {
            return self.service_account_access_token(&sa);
        }

        Ok(secret.to_string())
    }

    fn try_load_service_account(&self, secret: &str) -> Result<Option<ServiceAccountKey>> {
        let path = Path::new(secret);
        if path.exists() && path.is_file() {
            let text = fs::read_to_string(path).with_context(|| {
                format!("failed reading service-account json {}", path.display())
            })?;
            return self.parse_service_account_json(&text);
        }

        if secret.trim_start().starts_with('{') {
            return self.parse_service_account_json(secret);
        }

        Ok(None)
    }

    fn parse_service_account_json(&self, text: &str) -> Result<Option<ServiceAccountKey>> {
        let parsed: serde_json::Value =
            serde_json::from_str(text).context("failed parsing gdrive auth json value")?;
        let typ = parsed.get("type").and_then(|v| v.as_str());
        if typ != Some("service_account") {
            return Ok(None);
        }
        let key: ServiceAccountKey = serde_json::from_value(parsed)
            .context("invalid service-account json fields for gdrive auth")?;
        Ok(Some(key))
    }

    fn service_account_access_token(&self, key: &ServiceAccountKey) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock before unix epoch")?
            .as_secs();
        let claims = ServiceAccountClaims {
            iss: &key.client_email,
            scope: "https://www.googleapis.com/auth/drive",
            aud: &key.token_uri,
            exp: now + 3600,
            iat: now,
        };
        let assertion = encode(
            &Header::new(Algorithm::RS256),
            &claims,
            &EncodingKey::from_rsa_pem(key.private_key.as_bytes())
                .context("invalid service-account private_key PEM")?,
        )
        .context("failed creating service-account JWT assertion")?;

        let token = self
            .client
            .post(&key.token_uri)
            .form(&[
                ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                ("assertion", assertion.as_str()),
            ])
            .send()
            .context("failed requesting OAuth token from Google")?
            .error_for_status()
            .context("Google OAuth token request failed")?
            .json::<OAuthTokenResponse>()
            .context("failed decoding Google OAuth token response")?;
        Ok(token.access_token)
    }

    fn query_files(&self, query: &str) -> Result<Vec<DriveFile>> {
        let auth = self.auth_header_optional()?;
        let api_key = self.api_key_optional();

        if auth.is_none() && api_key.is_none() {
            let files = self.list_public_folder_files()?;
            return Ok(filter_public_files(&files, query));
        }

        let mut req = self.client.get(DRIVE_FILES_API).query(&[
            ("q", query),
            ("fields", "files(id,name)"),
            ("supportsAllDrives", "true"),
            ("includeItemsFromAllDrives", "true"),
        ]);

        if let Some(auth) = auth {
            req = req.header("Authorization", auth);
        } else if let Some(key) = api_key.as_deref() {
            req = req.query(&[("key", key)]);
        }

        let resp = req
            .send()
            .context("failed querying Google Drive files")?
            .error_for_status()
            .context("Google Drive list query failed")?
            .json::<FilesListResponse>()
            .context("failed decoding Google Drive file list")?;
        Ok(resp.files)
    }

    fn list_public_folder_files(&self) -> Result<Vec<DriveFile>> {
        let html = self
            .client
            .get(format!(
                "https://drive.google.com/embeddedfolderview?id={}#list",
                self.folder_id
            ))
            .send()
            .context("failed loading public gdrive folder page")?
            .error_for_status()
            .context("public gdrive folder page request failed")?
            .text()
            .context("failed reading public gdrive folder page")?;

        let mut files = Vec::new();
        let re = public_file_regex();
        for cap in re.captures_iter(&html) {
            let id = cap
                .get(1)
                .map(|m| m.as_str().to_string())
                .unwrap_or_default();
            let body = cap.get(2).map(|m| m.as_str()).unwrap_or_default();
            let name = strip_html_tags(body).trim().to_string();
            if !id.is_empty() && !name.is_empty() {
                files.push(DriveFile { id, name });
            }
        }

        if files.is_empty() {
            return Err(anyhow!(
                "public folder page did not expose files; ensure folder and files are shared for anyone with link"
            ));
        }

        Ok(files)
    }

    fn delete_by_name(&self, name: &str) -> Result<()> {
        let q = format!(
            "'{}' in parents and trashed = false and name = '{}'",
            self.folder_id, name
        );
        for file in self.query_files(&q)? {
            let _ = self.delete_file(&file.id);
        }
        Ok(())
    }

    fn delete_file(&self, id: &str) -> Result<()> {
        let auth = self.auth_header_required()?;
        self.client
            .delete(format!("{}/{}", DRIVE_FILES_API, id))
            .header("Authorization", auth)
            .query(&[("supportsAllDrives", "true")])
            .send()
            .with_context(|| format!("failed deleting gdrive file {}", id))?
            .error_for_status()
            .with_context(|| format!("Google Drive delete failed for {}", id))?;
        Ok(())
    }

    fn find_files_by_name_authenticated(&self, name: &str) -> Result<Vec<DriveFile>> {
        let auth = self.auth_header_required()?;
        let q = format!(
            "'{}' in parents and trashed = false and name = '{}'",
            self.folder_id, name
        );
        let resp = self
            .client
            .get(DRIVE_FILES_API)
            .header("Authorization", auth)
            .query(&[
                ("q", q.as_str()),
                ("fields", "files(id,name)"),
                ("supportsAllDrives", "true"),
                ("includeItemsFromAllDrives", "true"),
            ])
            .send()
            .context("failed querying existing gdrive files by name")?
            .error_for_status()
            .context("Google Drive list by name failed")?
            .json::<FilesListResponse>()
            .context("failed decoding Google Drive file list by name")?;
        Ok(resp.files)
    }

    fn create_drive_file(&self, name: &str, mime: &str) -> Result<String> {
        let mut existing = self.find_files_by_name_authenticated(name)?;
        existing.sort_by(|a, b| a.id.cmp(&b.id));
        if let Some(first) = existing.first() {
            return Ok(first.id.clone());
        }

        let auth = self.auth_header_required()?;
        let created = self
            .client
            .post(DRIVE_FILES_API)
            .header("Authorization", auth)
            .query(&[("fields", "id"), ("supportsAllDrives", "true")])
            .json(&DriveCreateRequest {
                name: name.to_string(),
                parents: vec![self.folder_id.clone()],
                mime_type: Some(mime.to_string()),
            })
            .send()
            .with_context(|| format!("failed creating Google Drive file {}", name))?
            .error_for_status()
            .with_context(|| format!("Google Drive create failed for {}", name))?
            .json::<DriveCreateResponse>()
            .with_context(|| format!("failed decoding create response for {}", name))?;
        Ok(created.id)
    }

    fn upload_media(&self, file_id: &str, mime: &str, body: Body, name: &str) -> Result<()> {
        let auth = self.auth_header_required()?;
        self.client
            .patch(format!("{}/{}", DRIVE_UPLOAD_API, file_id))
            .header("Authorization", auth)
            .header("Content-Type", mime)
            .query(&[("uploadType", "media"), ("supportsAllDrives", "true")])
            .body(body)
            .send()
            .with_context(|| format!("failed uploading {} to Google Drive", name))?
            .error_for_status()
            .with_context(|| format!("Google Drive upload failed for {}", name))?;
        Ok(())
    }

    fn upload_named_bytes(&self, name: &str, mime: &str, bytes: Vec<u8>) -> Result<String> {
        let file_id = self.create_drive_file(name, mime)?;
        let total = bytes.len() as u64;
        let pb = transfer_bar(total, &format!("gdrive upload {}", name));
        let reader = ProgressReader::new(std::io::Cursor::new(bytes), Some(pb.clone()));
        self.upload_media(&file_id, mime, Body::new(reader), name)?;
        pb.finish_and_clear();
        Ok(file_id)
    }

    fn upload_named_file(&self, name: &str, mime: &str, path: &Path) -> Result<String> {
        let file_id = self.create_drive_file(name, mime)?;
        let size = fs::metadata(path)
            .with_context(|| format!("failed to stat {}", path.display()))?
            .len();
        let pb = transfer_bar(size, &format!("gdrive upload {}", name));
        let file =
            fs::File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        let reader = ProgressReader::new(file, Some(pb.clone()));
        self.upload_media(&file_id, mime, Body::new(reader), name)?;
        pb.finish_and_clear();
        Ok(file_id)
    }

    fn download_file_to_path(&self, id: &str, out: &Path, title: &str) -> Result<()> {
        let auth = self.auth_header_optional()?;
        let api_key = self.api_key_optional();

        let mut req = if auth.is_none() && api_key.is_none() {
            self.client.get(public_download_url(id))
        } else {
            self.client
                .get(format!("{}/{}", DRIVE_FILES_API, id))
                .query(&[("alt", "media"), ("supportsAllDrives", "true")])
        };

        if let Some(auth) = auth {
            req = req.header("Authorization", auth);
        } else if let Some(key) = api_key.as_deref() {
            req = req.query(&[("key", key)]);
        }

        self.stream_response_to_path(req, out, title)
    }

    fn download_public_url_to_path(&self, url: &str, out: &Path, title: &str) -> Result<()> {
        let req = self.client.get(url);
        self.stream_response_to_path(req, out, title)
    }

    fn stream_response_to_path(
        &self,
        req: reqwest::blocking::RequestBuilder,
        out: &Path,
        title: &str,
    ) -> Result<()> {
        let mut resp = req
            .send()
            .with_context(|| format!("failed downloading {}", title))?
            .error_for_status()
            .with_context(|| format!("download failed: {}", title))?;

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
        let auth = self.auth_header_optional()?;
        let api_key = self.api_key_optional();

        let mut req = if auth.is_none() && api_key.is_none() {
            self.client.get(public_download_url(id))
        } else {
            self.client
                .get(format!("{}/{}", DRIVE_FILES_API, id))
                .query(&[("alt", "media"), ("supportsAllDrives", "true")])
        };

        if let Some(auth) = auth {
            req = req.header("Authorization", auth);
        } else if let Some(key) = api_key.as_deref() {
            req = req.query(&[("key", key)]);
        }

        let bytes = req
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
        let manifest_name = self.public_manifest_name(bag);

        let bundle_id = self.upload_named_file(&bundle_name, "application/gzip", packed_file)?;

        let metadata = MetaFile {
            bag: bag.clone().without_attachment(),
            original_bytes,
            packed_bytes,
        };
        let metadata_bytes = serde_json::to_vec_pretty(&metadata)?;
        let metadata_id =
            self.upload_named_bytes(&metadata_name, "application/json", metadata_bytes)?;

        let manifest = PublicManifest {
            bag: bag.clone().without_attachment(),
            original_bytes,
            packed_bytes,
            bundle_file_id: bundle_id.clone(),
            metadata_file_id: metadata_id.clone(),
            bundle_url: public_download_url(&bundle_id),
            metadata_url: public_download_url(&metadata_id),
        };
        let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
        self.upload_named_bytes(&manifest_name, "application/json", manifest_bytes)?;

        Ok(())
    }

    fn pull(&self, bag: &BagRef, out_packed_file: &Path) -> Result<RemoteDescriptor> {
        let auth = self.auth_header_optional()?;
        let api_key = self.api_key_optional();

        if auth.is_none() && api_key.is_none() {
            let manifest_name = self.public_manifest_name(bag);
            let manifest_file = self.find_single_by_name(&manifest_name)?;
            let manifest_bytes = self.download_file_bytes(&manifest_file.id)?;
            let manifest: PublicManifest = serde_json::from_slice(&manifest_bytes)
                .context("failed parsing public manifest from gdrive")?;

            if let Some(parent) = out_packed_file.parent() {
                fs::create_dir_all(parent)?;
            }
            self.download_public_url_to_path(
                &manifest.bundle_url,
                out_packed_file,
                &format!("downloading {}", self.bundle_name(bag)),
            )?;

            return Ok(RemoteDescriptor {
                registry_name: self.name.clone(),
                bag: manifest.bag,
                original_bytes: manifest.original_bytes,
                packed_bytes: manifest.packed_bytes,
            });
        }

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

        let manifest_query = format!(
            "'{}' in parents and trashed = false and name contains '.public.json'",
            self.folder_id
        );
        let manifest_files = self.query_files(&manifest_query)?;

        let mut out = Vec::new();
        for file in manifest_files {
            let bytes = self.download_file_bytes(&file.id)?;
            let manifest: PublicManifest = match serde_json::from_slice(&bytes) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let bag = manifest.bag.without_attachment();
            if pattern.matches(&bag.to_string()) {
                out.push(bag);
            }
        }

        if !out.is_empty() {
            out.sort_by_key(|b| b.to_string());
            out.dedup();
            return Ok(out);
        }

        let q = format!(
            "'{}' in parents and trashed = false and name contains '.metadata.json'",
            self.folder_id
        );
        let files = self.query_files(&q)?;

        for file in files {
            let bytes = self.download_file_bytes(&file.id)?;
            let meta: MetaFile = serde_json::from_slice(&bytes)
                .with_context(|| format!("failed parsing metadata file {}", file.name))?;
            let bag = meta.bag.without_attachment();
            if pattern.matches(&bag.to_string()) {
                out.push(bag);
            }
        }

        out.sort_by_key(|b| b.to_string());
        out.dedup();
        Ok(out)
    }

    fn remove(&self, bag: &BagRef) -> Result<()> {
        self.delete_by_name(&self.bundle_name(bag))?;
        self.delete_by_name(&self.metadata_name(bag))?;
        self.delete_by_name(&self.public_manifest_name(bag))?;
        Ok(())
    }

    fn check_connection(&self) -> Result<()> {
        if cfg!(test) {
            return Ok(());
        }

        let auth = self.auth_header_optional()?;
        let mut req = self
            .client
            .get(DRIVE_FILES_API)
            .timeout(Duration::from_secs(5))
            .query(&[
                (
                    "q",
                    format!("'{}' in parents and trashed=false", self.folder_id),
                ),
                ("pageSize", "1".to_string()),
                ("fields", "files(id)".to_string()),
                ("supportsAllDrives", "true".to_string()),
                ("includeItemsFromAllDrives", "true".to_string()),
            ]);

        if let Some(auth) = auth {
            req = req.header("Authorization", auth);
        } else if let Some(key) = self.api_key_optional().as_deref() {
            req = req.query(&[("key", key.to_string())]);
        }

        req.send()
            .context("failed checking gdrive connectivity")?
            .error_for_status()
            .context("drive connectivity check returned error")?;
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

fn transfer_bar(total: u64, message: &str) -> ProgressBar {
    let pb = if total > 0 {
        ProgressBar::new(total)
    } else {
        ProgressBar::new_spinner()
    };
    pb.set_style(
        ProgressStyle::with_template("{msg} [{bar:40.green/blue}] {bytes}/{total_bytes} ({eta})")
            .unwrap_or_else(|_| ProgressStyle::default_bar()),
    );
    pb.set_message(message.to_string());
    pb
}

struct ProgressReader<R> {
    inner: R,
    pb: Option<ProgressBar>,
}

impl<R> ProgressReader<R> {
    fn new(inner: R, pb: Option<ProgressBar>) -> Self {
        Self { inner, pb }
    }
}

impl<R: Read> Read for ProgressReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        if let Some(pb) = &self.pb {
            pb.inc(n as u64);
        }
        Ok(n)
    }
}

fn public_file_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(
            r#"href=\"https://drive\.google\.com/file/d/([^/\"]+)/view[^\"]*\"[^>]*>(.*?)</a>"#,
        )
        .expect("valid public drive file regex")
    })
}

fn strip_html_tags(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_tag = false;
    for ch in s.chars() {
        match ch {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(ch),
            _ => {}
        }
    }
    html_unescape_basic(&out)
}

fn html_unescape_basic(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
}

fn filter_public_files(files: &[DriveFile], query: &str) -> Vec<DriveFile> {
    if let Some(name) = parse_query_name_eq(query) {
        return files
            .iter()
            .filter(|f| f.name == name)
            .cloned()
            .collect::<Vec<_>>();
    }
    if let Some(needle) = parse_query_name_contains(query) {
        return files
            .iter()
            .filter(|f| f.name.contains(&needle))
            .cloned()
            .collect::<Vec<_>>();
    }
    files.to_vec()
}

fn parse_query_name_eq(query: &str) -> Option<String> {
    let marker = "name = '";
    let start = query.find(marker)? + marker.len();
    let rest = &query[start..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

fn parse_query_name_contains(query: &str) -> Option<String> {
    let marker = "name contains '";
    let start = query.find(marker)? + marker.len();
    let rest = &query[start..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}

fn public_download_url(id: &str) -> String {
    format!("https://drive.usercontent.google.com/download?id={id}&export=download&confirm=t")
}
