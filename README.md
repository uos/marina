# marina

`marina` is a bagfile manager to organize, share, and discover ROS bags across storage backends so you can finally stop emailing download links around.

## Compression

Marina compresses PointCloud2 messages in MCAP bags for more efficient storage and transfer. To avoid unexpected data loss, the default compression mode is `lossless`, which is a reversible encoding that preserves all original data. If you want to achieve higher compression ratios at the cost of some mm accuracy, you can choose `lossy` mode, which we recommend for most use cases.

Compression defaults can be configured globally in `~/.config/marina/registries.toml`:

```toml
registry = []

[compression]
pointcloud_mode = "lossless"             # off | lossy | lossless
pointcloud_accuracy_mm = 1.0             # float
packed_mcap_compression = "zstd"         # none | zstd | lz4
packed_archive_compression = "gzip"      # gzip | none
unpacked_mcap_compression = "zstd"       # none | zstd | lz4
```

If `push`/`pull` compression flags are provided on the CLI, those values override the config for that command only.

## Registries

We support multiple registries to organize bags across different storage backends. Each registry has a unique name and URI, and may have optional auth config.

Every installation already comes with a default `osnabotics-public` registry for public read-only access to the shared datasets of our organization. You can add your own registries for private storage and sharing.

If you want to have your bagfile available in the `osnabotics-public` registry, please contact `info@osnabotics.org` and send us a publicly accessible Google Drive folder ID to a registry with the name of your bagfile and a reason why we should host it.

Now we will cover how to setup your own registries for private storage and sharing.

## SSH

Use `ssh-copy-id` to set up passwordless SSH key auth for your registry server. Password auth is supported but we need to ask for the password on every command, so SSH keys are highly recommended for a smooth experience.

```bash
ssh-copy-id -i ~/.ssh/<key_name>.pub <user>@your-registry-server.org
```

Alternatively, set key auth env `--auth-env MARINA_SSH_KEY` to the path of your private key instead of relying on SSH agent to pick the correct key.

Then add the registry:

```bash
marina registry add ssh://<user>@your-registry-server.org:/path/to/registry --name my-ssh
```

## HTTP

For simple public, read-only HTTP serving, add an `http://` or `https://` registry:

```bash
marina registry add https://datasets.example.org/marina --name web-main
```

You won't be able to run `push` or `remove` commands on HTTP registries because we expect them to be pushed to via a separate `ssh` registry with `--write-http-index` for running search/list.

## Google Drive

Google Drive registries support both public and private folders.

If the folder is publicly shared, you can add it without auth. But you won't be able to push or remove bags without auth.

```bash
marina registry add gdrive://<folder_id> --name public-drive
```

To push or remove bags, you must authenticate with your Google account. A good default is to create a new folder (e.g. `marina`) in Google Drive, share it publicly for read access, and then authenticate your user so you can push to it.

> [!WARNING]
> If the data is sensitive, you can also create a private folder but due to our current scope being very restricted, Marina cannot access privately shared folders to your user. On the other hand, you probably shouldn't be uploading sensitive data on Google Drive in the first place. Use a private SSH registry instead.

You will need the folder ID, which is the part after `folders/` in the folder URL. For example, in `https://drive.google.com/drive/folders/10hjoMIyWTOVNOo3zDOfHoSb1S55gO3rJ`, the folder ID is `10hjoMIyWTOVNOo3zDOfHoSb1S55gO3rJ`.

```bash
marina registry add gdrive://<folder-id> --name myregistry
marina registry auth myregistry
```

## Local Directory

For mounted network filesystems (e.g. NFS, SMB) or local disk storage, add a `folder://` registry:

```bash
marina registry add folder://./local-reg --name local
```

Auth is handled by the underlying filesystem permissions, so no additional config is needed.

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
