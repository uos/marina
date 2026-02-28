# marina

`marina` is a ROS bag manager to organize, share, and discover ROS bags across storage backends so you can finally stop emailing download links around.

- Shared local state:
  - Config: `~/.config/marina/registries.toml`
  - Cache: `~/.cache/marina/bags/...`
  - Catalog: `~/.config/marina/catalog.json`

## MCAP policy

Pushing requires a bag directory that contains both:

- at least one `.mcap`
- at least one `metadata.yaml` or `metadata.yml`

Using MCAP is a strong requirement. Convert your old ones with [this tutorial](https://mcap.dev/guides/getting-started).

## Compression notes

During push, marina rewrites MCAP:
  - CDR-decodes `sensor_msgs/msg/PointCloud2`
  - Compresses with `cloudini` (`lossy_zstd`, `0.001` m / 1 mm)
  - CDR-encodes compressed payload and marks channel metadata

During pull, marina rewrites MCAP back to standard PointCloud2:
  - CDR-decodes compressed payload
  - Cloudini-decompresses
  - CDR-encodes ROS `PointCloud2`

Images are not transformed.

## CLI examples

Add registries:

```bash
marina registry add folder://./local-reg --name local
marina registry add ssh://user@registry.uos.de:/srv/marina --name ssh-main --kind ssh --auth-env MARINA_SSH_KEY
marina registry add gdrive://<folder_id> --name drive-main --kind gdrive --auth-env GOOGLE_DRIVE_TOKEN
marina registry add aws://my-bucket/robot-bags --name s3-main --kind aws
```

Find data to pull:

```bash
marina search "dlg_*"
marina search "stelzo/*:ouster*" --registry drive-main
```

See local cache:

```bash
marina list
```

Push/pull/export/remove:

```bash
marina push dlg_cut ./test/dlg_cut --registry local
marina pull dlg_cut:ouster --registry local
marina pull "dlg_cut:*" --registry local
marina export "dlg_cut[traj.txt]" ./traj.txt
marina rm dlg_cut:ouster
marina rm dlg_cut:ouster --remote --registry local
```
