# marina

**Marina** is a dataset manager for robotics built to organize, share, and discover bags and datasets across teams and storage backends.

See the [Web Documentation](https://stelzo.codeberg.page/marina/) or `ssh marina@steado.tech` for more detailed information and setup.

## ROS 2 CLI

While Marina's dynamic autocomplete for modern shells is designed to work as a standalone tool, we provide a ROS 2 CLI mapping to ensure a more familiar user experience.

```bash
# Pull a dataset from any configured registry...
ros2 bag pull dlg_feldtage_24:cut

# and pass it straight to playback.
ros2 bag play $(ros2 bag resolve dlg_feldtage_24:cut)

# See available bags from configured remotes...
ros2 bag datasets --remote
# or use the alias.
ros2 bag ds --remote
```

Datasets are pulled from remote registries. You can easily create your own using SSH, Google Drive, HTTP or any filesystem folder.

```bash
# Record to local cache...
ros2 bag record --all -o $(ros2 bag import my_recording:session1)

# and push to your registry.
ros2 bag push my-run:v1 /path/to/bag/ --registry team_ssh
```

## Standalone

Marina supports Linux and MacOS environments without additional requirements. Just [install](https://stelzo.codeberg.page/marina/installation/packages.html) and run.

```bash
# Pull a dataset from any configured registry.
marina pull dlg_feldtage_24:cut

# Get and use the data from the local cache.
cat $(marina resolve dlg_feldtage_24:cut)/metadata.yaml

# See available bags from configured remotes...
marina list --remote
# or use the alias.
marina ls --remote

# Export the most verbose remote catalog JSON for the static explorer.
marina list --remote --format json > web/catalog.json

# Push any folder or bags to your registry.
marina bag push my-run:v1 /path/to/dataset/ --registry team_ssh
```

---

### License

<sup>
Licensed under either of <a href="https://codeberg.org/stelzo/marina/src/branch/main/LICENSE-APACHE">Apache License, Version
2.0</a> or <a href="https://codeberg.org/stelzo/marina/src/branch/main/LICENSE-MIT">MIT license</a> at your option.
</sup>

<br>

<sub>
Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this crate by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
</sub>
