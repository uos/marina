# marina

`marina` is a ROS bag manager to organize, share, and discover ROS bags across storage backends so you can finally stop emailing download links around.

- Shared local state:
  - Config: `~/.config/marina/registries.toml`
  - Cache: `~/.cache/marina/bags/...`
  - Catalog: `~/.config/marina/catalog.json`

## MCAP policy

Pushing requires a directory that contains both:

- at least one `.mcap`
- at least one `metadata.yaml` or `metadata.yml`

Using MCAP is a strong requirement. Convert your legacy bags with [this tutorial](https://mcap.dev/guides/getting-started).

### Converting Old Bags

- https://mcap.dev/guides/getting-started/ros-2
- https://mcap.dev/guides/cli

## Compression notes

During push, marina rewrites MCAP:
  - CDR-decodes `sensor_msgs/msg/PointCloud2`
  - Compresses with `cloudini` (default: `--pointcloud-mode lossy --pointcloud-accuracy-mm 1`)
  - Supports `--pointcloud-mode lossless` for zero accuracy loss
  - Supports `--pointcloud-mode off` to skip pointcloud transform
  - Writes output MCAP chunks with `--packed-mcap-compression` (`zstd` default)

During pull, marina rewrites MCAP back to standard PointCloud2 and writes the local ready MCAP with `--unpacked-mcap-compression` (`zstd` default).

Images are not transformed.

## CLI examples

Add registries:

```bash
marina registry add folder://./local-reg --name local
marina registry add ssh://user@registry.uos.de:/srv/marina --name ssh-main --kind ssh --auth-env MARINA_SSH_KEY
marina registry add gdrive://<folder_id> --name drive-main --kind gdrive --auth-env GOOGLE_DRIVE_TOKEN
```

Auth notes:

- `ssh`:
  - default: SSH agent auth
  - with `--auth-env VAR`: `VAR` value can be password or private-key path
  - optional key passphrase: `${VAR}_PASSPHRASE`
- `gdrive`:
  - with `--auth-env VAR`: `VAR` may be
    - an OAuth access token string, or
    - a path to a Google service-account JSON key, or
    - the full service-account JSON content
  - without `--auth-env`: auth is optional for reads (`search`/`pull`) and required for writes (`push`/`rm`)
  - without auth and without API key, marina falls back to the public folder endpoint + per-bag `.public.json` manifest uploaded during `push`
  - `GOOGLE_DRIVE_TOKEN` is also supported as default auth token env
  - URI format: `gdrive://<drive_folder_id>`

Find data to pull:

```bash
marina search "tag*"
marina search "team/tag:ouster*" --registry drive-main
```

See local cache:

```bash
marina list
```

Push/pull/export/remove:

```bash
marina push tag ./tag_bag --registry local
marina pull tag:ouster --registry local
marina pull "tag:*" --registry local
marina export "tag[traj.txt]" ./traj.txt
marina rm tag:ouster
marina rm tag:ouster --remote --registry local

# Lossless pointcloud packing
marina push tag ./tag_bag --registry local --pointcloud-mode lossless

# Keep pulled MCAP uncompressed for lower playback CPU
marina pull tag --registry local --unpacked-mcap-compression none
```

## Library usage

Rust:

```rust
use marina::{BagRef, Marina, ProgressReporter, WriterProgress};

let mut marina = Marina::load()?;
let bag: BagRef = "tag:ouster".parse()?;

let mut out = std::io::stdout();
let mut sink = WriterProgress::new(&mut out);
let mut progress = ProgressReporter::new(&mut sink);
let local = marina.pull_exact_with_progress(&bag, Some("local"), &mut progress)?;
println!("ready at {}", local.display());
# Ok::<(), anyhow::Error>(())
```

C:

```c
static void on_progress(const char *phase, const char *message, void *user_data) {
  (void)user_data;
  printf("[%s] %s\n", phase, message);
}

char *local = marina_pull_with_callback("tag:ouster", "local", on_progress, NULL);
if (!local) {
  char *err = marina_last_error_message();
  fprintf(stderr, "pull failed: %s\n", err ? err : "unknown error");
  marina_free_string(err);
}
```

Python:

```python
import marina

r = marina.resolve_detailed("tag:ouster")
if r.should_pull:
    path = marina.pull_with_progress(r.bag, r.registry, progress=True)
    print("ready at", path)
```
