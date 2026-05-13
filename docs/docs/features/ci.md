# CI Integration

Marina is designed to work headlessly in CI pipelines. Every registry type that requires credentials supports an `--auth-env` flag that names an environment variable holding the secret. No interactive prompts, no stored session files needed on the runner.

## General pattern

When adding a registry, pass `--auth-env` with the **name** of the environment variable that will hold the credentials at runtime. The variable name is saved in the registry config.

~~~bash
marina registry add <name> <uri> --auth-env MY_SECRET_VAR
~~~

Then in your CI environment, set `MY_SECRET_VAR` to the appropriate value for the registry type (see below) and run Marina commands normally.

## Config and cache directories

By default Marina reads its config from `~/.config/marina/marina.rl` and writes cached data to `~/.cache/marina/`. Both paths can be overridden with environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `MARINA_CONFIG_DIR` | `~/.config/marina` | Directory containing `marina.rl` |
| `MARINA_CACHE_DIR` | `~/.cache/marina` | Downloaded dataset cache |

This makes it easy to ship the registry configuration as part of the repo and point CI runners at it directly — no `marina registry add` step needed on the runner:

~~~
repo/
└── ci/
    └── marina/
        └── marina.rl   ← registry config with auth_env set
~~~

~~~yaml
env:
  MARINA_CONFIG_DIR: ${{ github.workspace }}/ci/marina
  MARINA_SA_JSON: ${{ secrets.MARINA_SA_JSON }}
  MARINA_CACHE_DIR: /tmp/marina-cache

steps:
  - run: marina pull namespace/dataset:tag
~~~

A minimal `marina.rl` for a GDrive registry looks like:

~~~
registries {
  prod {
    uri = "gdrive://<folder_id>"
    kind = "gdrive"
    auth_env = "MARINA_SA_JSON"
  }
}
~~~

## Google Drive

Marina uses a Google Service Account for unattended Drive access.

**One-time setup**

1. Create a Service Account in [Google Cloud Console](https://console.cloud.google.com/apis/credentials).
2. Grant it access to the target Drive folder by sharing the folder with the service account's email address.
3. Download the key as JSON.
4. Store the JSON content as a CI secret (e.g. `MARINA_SA_JSON`).

**Register the registry** (done once, result can be committed to the repo):

~~~bash
marina registry add prod gdrive://<folder_id> --auth-env MARINA_SA_JSON
~~~

**CI pipeline**

~~~yaml
env:
  MARINA_SA_JSON: ${{ secrets.MARINA_SA_JSON }}

steps:
  - run: marina pull namespace/dataset:tag
  - run: marina push namespace/dataset:tag ./data/
~~~

The env var can hold either the raw JSON content (`{"type":"service_account",...}`) or a path to the JSON file on disk — we accept both.

## SSH

Marina authenticates with a private key or password, controlled by the `auth_env` variable's value at runtime.

**Key-based auth** (recommended)

Store the private key content as a CI secret, write it to a temporary file, and point the env var at the path:

~~~yaml
env:
  MARINA_SSH_KEY: ${{ secrets.MARINA_SSH_KEY }}       # path to key file
  MARINA_SSH_KEY_PASSPHRASE: ${{ secrets.SSH_PASSPHRASE }}  # omit if unencrypted
~~~

Or write the key inline to a temp file in the pipeline:

~~~yaml
steps:
  - run: |
      echo "$SSH_PRIVATE_KEY" > /tmp/marina_key
      chmod 600 /tmp/marina_key
      echo "MARINA_SSH_KEY=/tmp/marina_key" >> $GITHUB_ENV
    env:
      SSH_PRIVATE_KEY: ${{ secrets.SSH_PRIVATE_KEY }}

  - run: marina pull namespace/dataset:tag
~~~

**Password auth**

If the env var value is not a file path, Marina treats it as a password directly:

~~~yaml
env:
  MARINA_SSH_KEY: ${{ secrets.SSH_PASSWORD }}
~~~

**Register the registry**:

~~~bash
marina registry add prod ssh://user@host:22/path/to/registry --auth-env MARINA_SSH_KEY
~~~

For encrypted keys, Marina automatically reads `{VAR}_PASSPHRASE` — so if `--auth-env MARINA_SSH_KEY` is set, it looks for `MARINA_SSH_KEY_PASSPHRASE`.

**Jump hosts**

CI runner service accounts often do not load `~/.ssh/config`, so OpenSSH `ProxyJump` settings are not visible to Marina's native SSH client. Add the jump host to the registry config:

~~~awk
registries {
  prod {
    uri = "ssh://marina@internal.example.org:/srv/marina"
    kind = "ssh"
    auth_env = "MARINA_SSH_KEY"
    proxy_jump = "ci@bastion.example.org:22"
  }
}
~~~

Or set it per CI job without changing the config:

~~~yaml
env:
  MARINA_SSH_PROXY_JUMP: ci@bastion.example.org:22
~~~

`proxy_jump` is intentionally simple and uses `user@host[:port]`; it does not parse OpenSSH config aliases.

**OpenSSH transport**

By default Marina uses its native Rust SSH client. If a CI runner can connect with `/usr/bin/ssh` but the native client has platform-specific socket trouble, opt into the system OpenSSH tools:

~~~awk
registries {
  prod {
    uri = "ssh://marina@internal.example.org:/srv/marina"
    kind = "ssh"
    auth_env = "MARINA_SSH_KEY"
    ssh_transport = "openssh"
  }
}
~~~

Or set it per CI job:

~~~yaml
env:
  MARINA_SSH_TRANSPORT: openssh
~~~

The OpenSSH transport runs `ssh` for remote commands and `scp` for file transfers. If `auth_env` points to a key file, Marina passes it with `-i`.


## SSH host key verification

Marina's native SSH transport currently accepts any host key automatically. This is intentional for ease of use in CI, but means you should ensure the hostname/IP is correct in your registry URI and that your network is trusted.

The OpenSSH transport uses your system `ssh`/`scp` behavior, including `known_hosts` checks.
