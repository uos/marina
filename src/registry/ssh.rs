use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use glob::Pattern;
use indicatif::{ProgressBar, ProgressStyle};
use serde::{Deserialize, Serialize};
use ssh2::Session;

use crate::model::bag_ref::BagRef;
use crate::registry::driver::{RegistryDriver, RemoteDescriptor};

#[derive(Debug, Clone)]
pub struct SshRegistry {
    pub name: String,
    endpoint: SshEndpoint,
    auth_env: Option<String>,
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
}

#[derive(Debug, Clone, Serialize)]
struct HttpIndexEntry {
    bag: BagRef,
    original_bytes: u64,
    packed_bytes: u64,
}

#[derive(Debug, Clone, Serialize)]
struct HttpIndexFile {
    bags: Vec<HttpIndexEntry>,
}

impl SshRegistry {
    pub fn from_uri(name: &str, uri: &str, auth_env: Option<String>) -> Result<Self> {
        let endpoint = SshEndpoint::parse(uri)?;
        Ok(Self {
            name: name.to_string(),
            endpoint,
            auth_env,
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

    fn connect(&self) -> Result<Session> {
        let (user, host) = split_user_host(&self.endpoint.user_host)?;
        let stream =
            TcpStream::connect((host.as_str(), self.endpoint.port)).with_context(|| {
                format!(
                    "failed connecting to ssh host {}:{}",
                    host, self.endpoint.port
                )
            })?;

        let mut session = Session::new().context("failed creating ssh session")?;
        session.set_tcp_stream(stream);
        session.handshake().context("ssh handshake failed")?;

        match &self.auth_env {
            Some(var) => {
                let secret = std::env::var(var)
                    .with_context(|| format!("missing ssh auth env var '{}'", var))?;
                let secret_path = Path::new(&secret);
                if secret_path.exists() {
                    let passphrase_var = format!("{}_PASSPHRASE", var);
                    let passphrase = std::env::var(&passphrase_var).ok();
                    session
                        .userauth_pubkey_file(&user, None, secret_path, passphrase.as_deref())
                        .with_context(|| {
                            format!(
                                "ssh key auth failed for user '{}' using key {}",
                                user,
                                secret_path.display()
                            )
                        })?;
                } else {
                    session
                        .userauth_password(&user, &secret)
                        .with_context(|| format!("ssh password auth failed for user '{}'", user))?;
                }
            }
            None => {
                let mut agent = session.agent().context("failed to initialize ssh agent")?;
                agent.connect().context("failed to connect ssh agent")?;
                agent
                    .list_identities()
                    .context("failed to list ssh identities")?;
                let identities = agent
                    .identities()
                    .context("failed reading ssh identities")?;
                let mut authed = false;
                for identity in identities {
                    if agent.userauth(&user, &identity).is_ok() {
                        authed = true;
                        break;
                    }
                }
                if !authed {
                    return Err(anyhow!(
                        "ssh auth failed for user '{}' using ssh-agent; set --auth-env for key/password auth",
                        user
                    ));
                }
            }
        }

        if !session.authenticated() {
            return Err(anyhow!("ssh authentication failed"));
        }

        Ok(session)
    }

    fn run_ssh(&self, remote_cmd: &str) -> Result<()> {
        let session = self.connect()?;
        let mut channel = session
            .channel_session()
            .context("failed opening ssh channel")?;
        channel
            .exec(remote_cmd)
            .with_context(|| format!("failed to exec remote command: {}", remote_cmd))?;

        let mut stderr = String::new();
        channel
            .stderr()
            .read_to_string(&mut stderr)
            .context("failed reading remote stderr")?;

        channel.wait_close().context("failed closing ssh channel")?;
        let code = channel
            .exit_status()
            .context("failed reading ssh exit status")?;
        if code != 0 {
            return Err(anyhow!(
                "ssh command failed (exit {}): {}",
                code,
                stderr.trim()
            ));
        }
        Ok(())
    }

    fn run_ssh_capture(&self, remote_cmd: &str) -> Result<String> {
        let session = self.connect()?;
        let mut channel = session
            .channel_session()
            .context("failed opening ssh channel")?;
        channel
            .exec(remote_cmd)
            .with_context(|| format!("failed to exec remote command: {}", remote_cmd))?;

        let mut stdout = String::new();
        channel
            .read_to_string(&mut stdout)
            .context("failed reading remote stdout")?;

        let mut stderr = String::new();
        channel
            .stderr()
            .read_to_string(&mut stderr)
            .context("failed reading remote stderr")?;

        channel.wait_close().context("failed closing ssh channel")?;
        let code = channel
            .exit_status()
            .context("failed reading ssh exit status")?;
        if code != 0 {
            return Err(anyhow!(
                "ssh command failed (exit {}): {}",
                code,
                stderr.trim()
            ));
        }

        Ok(stdout)
    }

    fn upload_file_with_progress(&self, local: &Path, remote_path: &str) -> Result<()> {
        let session = self.connect()?;
        let size = fs::metadata(local)?.len();
        let mut local_file = fs::File::open(local)
            .with_context(|| format!("failed opening local file {}", local.display()))?;

        let mut remote = session
            .scp_send(Path::new(remote_path), 0o644, size, None)
            .with_context(|| format!("failed opening remote scp target {}", remote_path))?;

        let pb = transfer_bar(size, &format!("ssh upload {}", local.display()));
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = local_file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            remote.write_all(&buf[..n])?;
            pb.inc(n as u64);
        }
        remote.send_eof().ok();
        remote.wait_eof().ok();
        remote.close().ok();
        remote.wait_close().ok();
        pb.finish_and_clear();
        Ok(())
    }

    fn download_file_with_progress(&self, remote_path: &str, local: &Path) -> Result<()> {
        let session = self.connect()?;
        let (mut remote, stat) = session
            .scp_recv(Path::new(remote_path))
            .with_context(|| format!("failed opening remote file {}", remote_path))?;

        let size = stat.size();
        if let Some(parent) = local.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut local_file = fs::File::create(local)
            .with_context(|| format!("failed creating local file {}", local.display()))?;

        let pb = transfer_bar(size, &format!("ssh download {}", remote_path));
        let mut buf = [0u8; 64 * 1024];
        loop {
            let n = remote.read(&mut buf)?;
            if n == 0 {
                break;
            }
            local_file.write_all(&buf[..n])?;
            pb.inc(n as u64);
        }
        remote.send_eof().ok();
        remote.wait_eof().ok();
        remote.close().ok();
        remote.wait_close().ok();
        pb.finish_and_clear();
        Ok(())
    }
}

impl RegistryDriver for SshRegistry {
    fn push(
        &self,
        _registry_name: &str,
        bag: &BagRef,
        packed_file: &Path,
        original_bytes: u64,
        packed_bytes: u64,
    ) -> Result<()> {
        let target_dir = self.object_dir(bag);
        self.run_ssh(&format!(
            "rm -rf {} && mkdir -p {}",
            shell_quote(&target_dir),
            shell_quote(&target_dir)
        ))?;

        self.upload_file_with_progress(packed_file, &self.data_path(bag))?;

        let tmp = std::env::temp_dir().join(format!("marina_meta_{}.json", bag.cache_key()));
        let meta = MetaFile {
            bag: bag.clone().without_attachment(),
            original_bytes,
            packed_bytes,
        };
        fs::write(&tmp, serde_json::to_vec_pretty(&meta)?)?;
        self.upload_file_with_progress(&tmp, &self.meta_path(bag))?;
        let _ = fs::remove_file(tmp);
        Ok(())
    }

    fn pull(&self, bag: &BagRef, out_packed_file: &Path) -> Result<RemoteDescriptor> {
        let parent = out_packed_file
            .parent()
            .ok_or_else(|| anyhow!("invalid output path"))?;
        fs::create_dir_all(parent)?;

        self.download_file_with_progress(&self.data_path(bag), out_packed_file)?;

        let meta_local = parent.join("remote_metadata.json");
        self.download_file_with_progress(&self.meta_path(bag), &meta_local)?;
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

    fn list(&self, filter: &str) -> Result<Vec<BagRef>> {
        let pattern = Pattern::new(filter).or_else(|_| Pattern::new("*"))?;
        let output = self.run_ssh_capture(&format!(
            "find {} -type f -name metadata.json",
            shell_quote(&self.endpoint.root)
        ))?;

        let mut out = Vec::new();
        for line in output.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let meta_json = self.run_ssh_capture(&format!("cat {}", shell_quote(line)))?;
            let meta: MetaFile = serde_json::from_str(&meta_json)
                .with_context(|| format!("failed to parse metadata at remote path {}", line))?;
            let bag = meta.bag.without_attachment();
            if pattern.matches(&bag.to_string()) {
                out.push(bag);
            }
        }

        Ok(out)
    }

    fn remove(&self, bag: &BagRef) -> Result<()> {
        let target_dir = self.object_dir(bag);
        self.run_ssh(&format!("rm -rf {}", shell_quote(&target_dir)))
    }

    fn write_http_index(&self) -> Result<()> {
        let output = self.run_ssh_capture(&format!(
            "find {} -type f -name metadata.json",
            shell_quote(&self.endpoint.root)
        ))?;

        let mut bags = Vec::new();
        for line in output.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let meta_json = self.run_ssh_capture(&format!("cat {}", shell_quote(line)))?;
            let meta: MetaFile = serde_json::from_str(&meta_json)
                .with_context(|| format!("failed to parse metadata at remote path {}", line))?;
            bags.push(HttpIndexEntry {
                bag: meta.bag.without_attachment(),
                original_bytes: meta.original_bytes,
                packed_bytes: meta.packed_bytes,
            });
        }
        bags.sort_by_key(|e| e.bag.to_string());
        bags.dedup_by(|a, b| a.bag == b.bag);

        let index = HttpIndexFile { bags };
        let tmp = std::env::temp_dir().join(format!("marina_http_index_{}.json", self.name));
        fs::write(&tmp, serde_json::to_vec_pretty(&index)?)?;
        let remote = format!("{}/index.json", self.endpoint.root);
        self.upload_file_with_progress(&tmp, &remote)?;
        let _ = fs::remove_file(tmp);
        Ok(())
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
        if !left.is_empty() && right.chars().all(|c| c.is_ascii_digit()) {
            let port: u16 = right
                .parse()
                .with_context(|| format!("invalid ssh port '{}'", right))?;
            return Ok((left.to_string(), port));
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
    pb.set_style(
        ProgressStyle::with_template("{msg} [{bar:40.cyan/blue}] {bytes}/{total_bytes}")
            .unwrap_or_else(|_| ProgressStyle::default_bar()),
    );
    pb.set_message(message.to_string());
    pb
}
