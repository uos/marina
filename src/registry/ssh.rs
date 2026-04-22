use std::fs;
use std::io::IsTerminal;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use glob::Pattern;
use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use russh::ChannelMsg;
use russh::client::{self, Config, Handle};
use russh::keys::PrivateKeyWithHashAlg;
use russh::keys::PublicKey;
use russh_sftp::client::SftpSession;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::model::bag_ref::BagRef;
use crate::registry::driver::{BagInfo, PushMeta, RegistryDriver, RemoteDescriptor};

pub struct SshRegistry {
    pub name: String,
    endpoint: SshEndpoint,
    auth_env: Option<String>,
    pool: Arc<Mutex<Option<Arc<Handle<ClientHandler>>>>>,
}

impl std::fmt::Debug for SshRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SshRegistry")
            .field("name", &self.name)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl Clone for SshRegistry {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            endpoint: self.endpoint.clone(),
            auth_env: self.auth_env.clone(),
            pool: Arc::clone(&self.pool),
        }
    }
}

#[derive(Debug, Clone)]
struct SshEndpoint {
    user_host: String,
    port: u16,
    root: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MetaFile {
    bag: BagRef,
    original_bytes: u64,
    packed_bytes: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bundle_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pointcloud: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mcap_compression: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pushed_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
struct HttpIndexEntry {
    bag: BagRef,
    original_bytes: u64,
    packed_bytes: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct HttpIndexFile {
    bags: Vec<HttpIndexEntry>,
}

struct ClientHandler;

impl client::Handler for ClientHandler {
    type Error = anyhow::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &PublicKey,
    ) -> Result<bool, Self::Error> {
        Ok(true)
    }
}

impl SshRegistry {
    pub fn from_uri(name: &str, uri: &str, auth_env: Option<String>) -> Result<Self> {
        let endpoint = SshEndpoint::parse(uri)?;
        Ok(Self {
            name: name.to_string(),
            endpoint,
            auth_env,
            pool: Arc::new(Mutex::new(None)),
        })
    }

    fn object_dir(&self, bag: &BagRef) -> String {
        format!("{}/{}", self.endpoint.root, bag.object_path())
    }

    fn data_path(&self, bag: &BagRef) -> String {
        format!("{}/bundle.marina.tar.gz", self.object_dir(bag))
    }

    fn meta_path(&self, bag: &BagRef) -> String {
        format!("{}/metadata.json", self.object_dir(bag))
    }

    async fn connect_inner(&self) -> Result<Handle<ClientHandler>> {
        let (user, host) = split_user_host(&self.endpoint.user_host)?;
        let config = Arc::new(Config::default());

        let mut handle =
            client::connect(config, (host.as_str(), self.endpoint.port), ClientHandler)
                .await
                .with_context(|| {
                    format!(
                        "failed connecting to ssh host {}:{}",
                        host, self.endpoint.port
                    )
                })?;

        let authed = match &self.auth_env {
            Some(var) => {
                let secret = std::env::var(var)
                    .with_context(|| format!("missing ssh auth env var '{}'", var))?;
                let secret_path = Path::new(&secret);
                if secret_path.exists() {
                    let passphrase_var = format!("{}_PASSPHRASE", var);
                    let passphrase = std::env::var(&passphrase_var).ok();
                    let key = russh::keys::load_secret_key(secret_path, passphrase.as_deref())
                        .with_context(|| {
                            format!("failed loading ssh key {}", secret_path.display())
                        })?;
                    handle
                        .authenticate_publickey(
                            &user,
                            PrivateKeyWithHashAlg::new(Arc::new(key), None),
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "ssh key auth failed for user '{}' using key {}",
                                user,
                                secret_path.display()
                            )
                        })?
                        .success()
                } else {
                    handle
                        .authenticate_password(&user, &secret)
                        .await
                        .with_context(|| format!("ssh password auth failed for user '{}'", user))?
                        .success()
                }
            }
            None => {
                let mut authed = false;

                // Try ssh-agent
                if let Ok(sock) = std::env::var("SSH_AUTH_SOCK") {
                    if let Ok(mut agent) =
                        russh::keys::agent::client::AgentClient::connect_uds(&sock).await
                    {
                        if let Ok(identities) = agent.request_identities().await {
                            for key in identities {
                                if handle
                                    .authenticate_publickey_with(&user, key, None, &mut agent)
                                    .await
                                    .map(|r| r.success())
                                    .unwrap_or(false)
                                {
                                    authed = true;
                                    break;
                                }
                            }
                        }
                    }
                }

                // Try default key files
                if !authed {
                    if let Some(home) = dirs::home_dir() {
                        let ssh_dir = home.join(".ssh");
                        for key_name in ["id_ed25519", "id_rsa", "id_ecdsa", "id_dsa"] {
                            let key_path = ssh_dir.join(key_name);
                            if key_path.exists() {
                                if let Ok(key) = russh::keys::load_secret_key(&key_path, None) {
                                    if handle
                                        .authenticate_publickey(
                                            &user,
                                            PrivateKeyWithHashAlg::new(Arc::new(key), None),
                                        )
                                        .await
                                        .map(|r| r.success())
                                        .unwrap_or(false)
                                    {
                                        authed = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }

                // Fall back to interactive password prompt
                if !authed {
                    let password =
                        rpassword::prompt_password(format!("Password for {}@{}: ", user, host))
                            .context("failed reading password")?;
                    handle
                        .authenticate_password(&user, &password)
                        .await
                        .with_context(|| format!("ssh password auth failed for user '{}'", user))?
                        .success()
                } else {
                    true
                }
            }
        };

        if !authed {
            return Err(anyhow!("ssh authentication failed"));
        }

        Ok(handle)
    }

    async fn get_handle(&self) -> Result<Arc<Handle<ClientHandler>>> {
        let mut guard = self.pool.lock().await;
        if let Some(h) = guard.as_ref() {
            return Ok(Arc::clone(h));
        }
        let handle = self.connect_inner().await?;
        let arc = Arc::new(handle);
        *guard = Some(Arc::clone(&arc));
        Ok(arc)
    }

    async fn get_handle_fresh(&self) -> Result<Arc<Handle<ClientHandler>>> {
        let mut guard = self.pool.lock().await;
        let handle = self.connect_inner().await?;
        let arc = Arc::new(handle);
        *guard = Some(Arc::clone(&arc));
        Ok(arc)
    }

    async fn channel_open_session(&self) -> Result<russh::Channel<client::Msg>> {
        let handle = self.get_handle().await?;
        if let Ok(ch) = handle.channel_open_session().await {
            return Ok(ch);
        }
        // Stale connection — reconnect once
        let handle = self.get_handle_fresh().await?;
        handle
            .channel_open_session()
            .await
            .context("failed opening ssh channel")
    }

    async fn run_ssh(&self, remote_cmd: &str) -> Result<()> {
        let mut channel = self.channel_open_session().await?;
        channel
            .exec(true, remote_cmd)
            .await
            .with_context(|| format!("failed to exec remote command: {}", remote_cmd))?;

        let mut stderr = Vec::new();
        let mut exit_code: u32 = 0;

        loop {
            match channel.wait().await {
                Some(ChannelMsg::Data { .. }) => {}
                Some(ChannelMsg::ExtendedData { data, .. }) => {
                    stderr.extend_from_slice(&data);
                }
                Some(ChannelMsg::ExitStatus { exit_status }) => {
                    exit_code = exit_status;
                }
                None => break,
                _ => {}
            }
        }

        if exit_code != 0 {
            let stderr_str = String::from_utf8_lossy(&stderr).to_string();
            return Err(anyhow!(
                "ssh command failed (exit {}): {}",
                exit_code,
                stderr_str.trim()
            ));
        }
        Ok(())
    }

    async fn run_ssh_capture(&self, remote_cmd: &str) -> Result<String> {
        let mut channel = self.channel_open_session().await?;
        channel
            .exec(true, remote_cmd)
            .await
            .with_context(|| format!("failed to exec remote command: {}", remote_cmd))?;

        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut exit_code: u32 = 0;

        loop {
            match channel.wait().await {
                Some(ChannelMsg::Data { data }) => {
                    stdout.extend_from_slice(&data);
                }
                Some(ChannelMsg::ExtendedData { data, .. }) => {
                    stderr.extend_from_slice(&data);
                }
                Some(ChannelMsg::ExitStatus { exit_status }) => {
                    exit_code = exit_status;
                }
                None => break,
                _ => {}
            }
        }

        if exit_code != 0 {
            let stderr_str = String::from_utf8_lossy(&stderr).to_string();
            return Err(anyhow!(
                "ssh command failed (exit {}): {}",
                exit_code,
                stderr_str.trim()
            ));
        }

        String::from_utf8(stdout).context("remote stdout was not valid UTF-8")
    }

    async fn open_sftp(&self) -> Result<SftpSession> {
        let channel = self.channel_open_session().await?;
        channel
            .request_subsystem(true, "sftp")
            .await
            .context("failed requesting sftp subsystem")?;
        SftpSession::new(channel.into_stream())
            .await
            .context("failed creating sftp session")
    }

    async fn sftp_upload(
        sftp: &SftpSession,
        local: &Path,
        remote_path: &str,
        label: Option<&str>,
    ) -> Result<()> {
        let size = fs::metadata(local)
            .with_context(|| format!("failed to stat {}", local.display()))?
            .len();
        let mut local_file = tokio::fs::File::open(local)
            .await
            .with_context(|| format!("failed opening local file {}", local.display()))?;
        let mut remote_file = sftp
            .create(remote_path)
            .await
            .with_context(|| format!("failed opening remote sftp file {}", remote_path))?;
        let pb = label.map(|l| transfer_bar(size, l));
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = local_file.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            remote_file.write_all(&buf[..n]).await?;
            if let Some(ref pb) = pb {
                pb.inc(n as u64);
            }
        }
        if let Some(pb) = pb {
            pb.finish_and_clear();
        }
        Ok(())
    }

    async fn sftp_download(
        sftp: &SftpSession,
        remote_path: &str,
        local: &Path,
        label: Option<&str>,
    ) -> Result<()> {
        let size = sftp
            .metadata(remote_path)
            .await
            .with_context(|| format!("failed to stat remote file {}", remote_path))?
            .size
            .unwrap_or(0);
        if let Some(parent) = local.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut local_file = tokio::fs::File::create(local)
            .await
            .with_context(|| format!("failed creating local file {}", local.display()))?;
        let mut remote_file = sftp
            .open(remote_path)
            .await
            .with_context(|| format!("failed opening remote sftp file {}", remote_path))?;
        let pb = label.map(|l| transfer_bar(size, l));
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = remote_file.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            local_file.write_all(&buf[..n]).await?;
            if let Some(ref pb) = pb {
                pb.inc(n as u64);
            }
        }
        if let Some(pb) = pb {
            pb.finish_and_clear();
        }
        Ok(())
    }

    async fn upload_file_with_progress(&self, local: &Path, remote_path: &str) -> Result<()> {
        let sftp = self.open_sftp().await?;
        Self::sftp_upload(
            &sftp,
            local,
            remote_path,
            Some(&format!("ssh upload {}", local.display())),
        )
        .await
    }

    /// Fetch all MetaFile records from the registry in a single SSH command.
    ///
    /// Uses ASCII record separator (0x1e) as delimiter between files —
    /// it cannot appear in valid JSON text.
    async fn fetch_all_meta(&self) -> Result<Vec<MetaFile>> {
        let cmd = format!(
            "find {} -type f -name metadata.json -exec sh -c 'for f; do printf \"\\036\"; cat \"$f\"; done' _ {{}} +",
            shell_quote(&self.endpoint.root)
        );
        let output = self.run_ssh_capture(&cmd).await?;
        let mut metas = Vec::new();
        for chunk in output.split('\x1e') {
            let chunk = chunk.trim();
            if chunk.is_empty() {
                continue;
            }
            if let Ok(meta) = serde_json::from_str::<MetaFile>(chunk) {
                metas.push(meta);
            }
        }
        Ok(metas)
    }
}

use async_trait::async_trait;

#[async_trait]
impl RegistryDriver for SshRegistry {
    async fn push(
        &self,
        _registry_name: &str,
        bag: &BagRef,
        packed_file: &Path,
        meta: &PushMeta,
    ) -> Result<()> {
        let target_dir = self.object_dir(bag);
        self.run_ssh(&format!(
            "rm -rf {} && mkdir -p {}",
            shell_quote(&target_dir),
            shell_quote(&target_dir)
        ))
        .await?;

        let tmp = std::env::temp_dir().join(format!("marina_meta_{}.json", bag.cache_key()));
        let meta_file = MetaFile {
            bag: bag.clone().without_attachment(),
            original_bytes: meta.original_bytes,
            packed_bytes: meta.packed_bytes,
            tags: bag.tags.clone(),
            bundle_hash: Some(meta.bundle_hash.clone()),
            pointcloud: Some(meta.pointcloud.clone()),
            mcap_compression: Some(meta.mcap_compression.clone()),
            pushed_at: Some(meta.pushed_at),
        };
        fs::write(&tmp, serde_json::to_vec_pretty(&meta_file)?)?;

        let sftp = self.open_sftp().await?;
        Self::sftp_upload(
            &sftp,
            packed_file,
            &self.data_path(bag),
            Some(&format!("ssh upload {}", packed_file.display())),
        )
        .await?;
        Self::sftp_upload(&sftp, &tmp, &self.meta_path(bag), None).await?;
        let _ = fs::remove_file(tmp);
        Ok(())
    }

    async fn bag_info(&self, bag: &BagRef) -> Result<Option<BagInfo>> {
        let meta_text = self
            .run_ssh_capture(&format!("cat {}", shell_quote(&self.meta_path(bag))))
            .await?;
        let meta: MetaFile = serde_json::from_str(&meta_text)?;
        Ok(Some(BagInfo {
            bundle_hash: meta.bundle_hash,
            original_bytes: meta.original_bytes,
            packed_bytes: meta.packed_bytes,
            pointcloud: meta.pointcloud,
            mcap_compression: meta.mcap_compression,
            pushed_at: meta.pushed_at,
        }))
    }

    async fn pull(&self, bag: &BagRef, out_packed_file: &Path) -> Result<RemoteDescriptor> {
        let parent = out_packed_file
            .parent()
            .ok_or_else(|| anyhow!("invalid output path"))?;
        fs::create_dir_all(parent)?;

        let sftp = self.open_sftp().await?;
        Self::sftp_download(
            &sftp,
            &self.data_path(bag),
            out_packed_file,
            Some(&bag.without_attachment().to_string()),
        )
        .await?;

        let meta_local = parent.join("remote_metadata.json");
        Self::sftp_download(&sftp, &self.meta_path(bag), &meta_local, None).await?;
        let meta_text = fs::read_to_string(&meta_local)?;
        let _ = fs::remove_file(meta_local);
        let meta: MetaFile = serde_json::from_str(&meta_text)?;

        Ok(RemoteDescriptor {
            registry_name: self.name.clone(),
            bag: meta.bag,
            original_bytes: meta.original_bytes,
            packed_bytes: meta.packed_bytes,
        })
    }

    async fn list(&self, filter: &str) -> Result<Vec<BagRef>> {
        let pattern = Pattern::new(filter).or_else(|_| Pattern::new("*"))?;
        Ok(self
            .fetch_all_meta()
            .await?
            .into_iter()
            .map(|m| m.bag.without_attachment())
            .filter(|b: &BagRef| pattern.matches(&b.to_string()))
            .collect())
    }

    async fn list_with_info(&self, filter: &str) -> Result<Vec<(BagRef, Option<BagInfo>)>> {
        let pattern = Pattern::new(filter).or_else(|_| Pattern::new("*"))?;
        Ok(self
            .fetch_all_meta()
            .await?
            .into_iter()
            .map(|meta| {
                let bag = meta.bag.without_attachment();
                let info = BagInfo {
                    bundle_hash: meta.bundle_hash,
                    original_bytes: meta.original_bytes,
                    packed_bytes: meta.packed_bytes,
                    pointcloud: meta.pointcloud,
                    mcap_compression: meta.mcap_compression,
                    pushed_at: meta.pushed_at,
                };
                (bag, Some(info))
            })
            .filter(|(b, _): &(BagRef, Option<BagInfo>)| pattern.matches(&b.to_string()))
            .collect())
    }

    async fn remove(&self, bag: &BagRef) -> Result<()> {
        let target_dir = self.object_dir(bag);
        self.run_ssh(&format!("rm -rf {}", shell_quote(&target_dir)))
            .await
    }

    async fn write_http_index(&self) -> Result<()> {
        let output = self
            .run_ssh_capture(&format!(
                "find {} -type f -name metadata.json",
                shell_quote(&self.endpoint.root)
            ))
            .await?;

        let mut bags = Vec::new();
        for line in output.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let meta_json = self
                .run_ssh_capture(&format!("cat {}", shell_quote(line)))
                .await?;
            let meta: MetaFile = serde_json::from_str(&meta_json)
                .with_context(|| format!("failed to parse metadata at remote path {}", line))?;
            bags.push(HttpIndexEntry {
                bag: meta.bag.without_attachment(),
                original_bytes: meta.original_bytes,
                packed_bytes: meta.packed_bytes,
                tags: if meta.tags.is_empty() {
                    meta.bag.tags.clone()
                } else {
                    meta.tags
                },
            });
        }
        bags.sort_by_key(|e| e.bag.to_string());
        bags.dedup_by(|a, b| a.bag == b.bag);

        let index = HttpIndexFile { bags };
        let tmp = std::env::temp_dir().join(format!("marina_http_index_{}.json", self.name));
        fs::write(&tmp, serde_json::to_vec_pretty(&index)?)?;
        let remote = format!("{}/index.json", self.endpoint.root);
        self.upload_file_with_progress(&tmp, &remote).await?;
        let _ = fs::remove_file(tmp);
        Ok(())
    }

    async fn check_write_access(&self) -> Result<()> {
        let probe = format!(
            "{}/.marina_write_probe_{}",
            self.endpoint.root,
            std::process::id()
        );
        self.run_ssh(&format!(
            "mkdir -p {} && rmdir {}",
            shell_quote(&probe),
            shell_quote(&probe)
        ))
        .await
    }
}

impl SshEndpoint {
    fn parse(uri: &str) -> Result<Self> {
        let raw = uri
            .strip_prefix("ssh://")
            .ok_or_else(|| anyhow!("ssh registry URI must start with ssh://"))?;

        let (authority, path) = if let Some(idx) = raw.find('/') {
            (&raw[..idx], &raw[idx..])
        } else {
            (raw, "")
        };

        if authority.is_empty() {
            return Err(anyhow!("ssh registry URI missing host"));
        }

        let (user_host, port) = parse_authority(authority)?;

        let root = if path.is_empty() {
            "~/marina-registry".to_string()
        } else {
            path.to_string()
        };

        Ok(Self {
            user_host,
            port,
            root,
        })
    }
}

fn parse_authority(authority: &str) -> Result<(String, u16)> {
    if let Some((left, right)) = authority.rsplit_once(':') {
        if !left.is_empty() && !right.is_empty() && right.chars().all(|c| c.is_ascii_digit()) {
            let port: u16 = right
                .parse()
                .with_context(|| format!("invalid ssh port '{}'", right))?;
            return Ok((left.to_string(), port));
        }
        // Trailing colon with no port (e.g. "host:/path") — strip the colon
        if right.is_empty() {
            return Ok((left.to_string(), 22));
        }
    }
    Ok((authority.to_string(), 22))
}

fn split_user_host(user_host: &str) -> Result<(String, String)> {
    if let Some((user, host)) = user_host.split_once('@') {
        if user.is_empty() || host.is_empty() {
            return Err(anyhow!("invalid ssh authority '{}'", user_host));
        }
        return Ok((user.to_string(), host.to_string()));
    }

    let user = std::env::var("USER").context("missing USER env var for ssh auth")?;
    Ok((user, user_host.to_string()))
}

fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

fn transfer_bar(total: u64, message: &str) -> ProgressBar {
    let pb = if total > 0 {
        ProgressBar::new(total)
    } else {
        ProgressBar::new_spinner()
    };
    if !std::io::stdout().is_terminal() {
        pb.set_draw_target(ProgressDrawTarget::hidden());
    }
    pb.set_style(
        ProgressStyle::with_template(
            "{msg} [{bar:40.cyan/blue}] {bytes}/{total_bytes} {bytes_per_sec} eta {eta}",
        )
        .unwrap_or_else(|_| ProgressStyle::default_bar()),
    );
    pb.enable_steady_tick(std::time::Duration::from_millis(100));
    pb.set_message(message.to_string());
    pb
}
