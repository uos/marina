# Configuration

Marina stores its configuration in a single file:

~~~
~/.config/marina/marina.rl
~~~

The file is created automatically on first run. You can also create or edit it manually.
The format is [ratslang](https://codeberg.org/stelzo/ratslang), which supports physical units directly.

## Full Example

~~~awk
settings {
  time_display = relative
  completion_cache_ttl = 10min
  registry_timeout = 10s
  # default_registry = team-ssh
}

compression {
  pointcloud_mode = lossy
  pointcloud_accuracy = 1mm
  packed_mcap_compression = zstd
  packed_archive_compression = none
  unpacked_mcap_compression = lz4
}

# Your own SSH registry
registries {
  team_ssh {
    uri = "ssh://user@your-server.org:/data/marina-registry"
  }

  # Google Drive registry (authenticated)
  my_drive {
    uri = "gdrive://<folder-id>"
    kind = gdrive
  }

  # Local NFS mount
  nas {
    uri = "folder:///mnt/nas/marina"
  }
}
~~~

## Registry Blocks

Each block inside `registries { }` defines a storage backend. The block name is the registry identifier — use underscores; they are displayed as-is.

| Field | Required | Description |
|---|---|---|
| `uri` | yes | Connection URI (see below) |
| `kind` | no | Explicit backend type; inferred from URI scheme if omitted |
| `auth_env` | no | Environment variable holding an SSH private-key path |

### URI Schemes

| Scheme | Backend | Example |
|---|---|---|
| `ssh://` | SSH / SFTP server | `ssh://user@host:/path/to/registry` |
| `gdrive://` | Google Drive folder | `gdrive://<folder-id>` |
| `https://` / `http://` | Read-only HTTP | `https://datasets.example.org/marina` |
| `folder://` | Local or mounted filesystem | `folder:///mnt/nas/marina` |

Registries can also be managed without editing the file directly:

~~~bash
marina registry add team_ssh ssh://user@host:/path
marina registry rm team_ssh
~~~

## Compression Fields

All compression settings are optional. Omitting a field uses the built-in default.

| Field | Default | Description |
|---|---|---|
| `pointcloud_mode` | `lossy` | PointCloud2 compression mode: `off`, `lossy`, `lossless` |
| `pointcloud_accuracy` | `1mm` | Rounding accuracy when using lossy mode (any length unit) |
| `packed_mcap_compression` | `zstd` | MCAP chunk compression for remote archives: `none`, `zstd`, `lz4` |
| `packed_archive_compression` | `none` | Outer archive compression: `none`, `gzip` |
| `unpacked_mcap_compression` | `lz4` | MCAP chunk compression for the local cache copy: `none`, `zstd`, `lz4` |

## Settings Fields

Global behaviour settings live under the `settings` block.

| Field | Default | Description |
|---|---|---|
| `default_registry` | — | Registry used when `--registry` is omitted |
| `time_display` | `relative` | How timestamps are shown in list output: `relative` or `absolute` |
| `completion_cache_ttl` | `10min` | How long the shell-completion remote index is considered fresh before a background refresh is triggered (any time unit, e.g. `30min`, `1h`) |
| `registry_timeout` | `10s` | How long to wait for a registry to respond before giving up (any time unit) |

## Environment Variables

These variables are usually only used in automated scenarios and are just included here for completeness.

| Variable | Description |
|---|---|
| `MARINA_SSH_KEY` | Path to an SSH private key used for registry auth when set via `--auth-env` |
| `MARINA_GDRIVE_CLIENT_ID` | OAuth client ID for Google Drive auth (alternative to `--client-id` flag) |
| `MARINA_GDRIVE_CLIENT_SECRET` | OAuth client secret for Google Drive auth (alternative to `--client-secret` flag) |
| `MARINA_PROG_NAME` | Override the program name used in CLI help output (used by ROS verb wrapper) |

## Cache Location

Downloaded datasets are stored in:

~~~
~/.cache/marina/
~~~

Clear the cache with:

~~~bash
marina clean
~~~

Clear the cache and all added registry configuration:

~~~bash
marina clean --all
~~~
