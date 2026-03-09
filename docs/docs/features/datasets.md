# Datasets

Datasets in Marina are identified by a *name* with optional *namespace* and *tags*:

~~~
[<namespace>/]<name>[:<tag>[:<tag>...]]
~~~

**Namespace**: An optional prefix (e.g. a team or project name) that groups related datasets. Use a slash to separate it from the name.

**Tags**: Free-form strings separated by colons. Use them for versioning, environment labels, or anything else that helps your team stay organised. If you omit the tag, Marina matches datasets by name only.

~~~bash
marina pull outdoor-run                      # name only
marina pull outdoor-run:v2                   # version tag
marina pull outdoor-run:rainy                # descriptive tag
marina pull outdoor-run:2024-06-10           # date tag
marina pull teamA/outdoor-run:v2             # namespace + name + tag
marina pull teamA/outdoor-run:v2:ouster      # multiple tags
~~~

## List

List all datasets currently in your local cache:

~~~bash
marina ls
~~~

List all datasets available in configured remote registries:

~~~bash
marina ls --remote
~~~

Filter to a specific registry:

~~~bash
marina ls --remote --registry team-ssh
~~~

## Search

Search datasets by pattern (matched against dataset names):

~~~bash
marina search "*feldtage*"
~~~

## Pull

Download a dataset from a registry into the local cache:

~~~bash
marina pull <dataset>
~~~

Marina searches all configured registries by default. Target a specific one with `--registry`:

~~~bash
marina pull outdoor-run:v2 --registry team-ssh
~~~

By default, pulled bags are decompressed with LZ4 for the MCAP chunks. Override the decompression format:

~~~bash
marina pull outdoor-run:v2 --unpacked-mcap-compression zstd
marina pull outdoor-run:v2 --unpacked-mcap-compression none
~~~

!!! info "Cache"

    Pulled datasets are stored in `~/.cache/marina/`. Pulling the same dataset a second time is a no-op — Marina detects the cached version and returns immediately.

## Resolve

Print the local filesystem path to a cached dataset:

~~~bash
marina resolve outdoor-run:v2
~~~

This is useful for piping directly into other tools:

~~~bash
ros2 bag play $(marina resolve outdoor-run:v2)
~~~

If the dataset is not yet cached, Marina pulls it first.

## Push

Upload a local bagfile directory to a registry:

~~~bash
marina push <dataset> <path>
~~~

~~~bash
marina push outdoor-run:v2 /path/to/outdoor-run/
~~~

Push to a specific registry:

~~~bash
marina push outdoor-run:v2 /path/to/outdoor-run/ --registry team-ssh
~~~

Compression settings can be overridden per push — see the [Compression](./compression.md) page for details.

### Additional Push Options

| Flag | Description |
|---|---|
| `--dry-run` | Run the full pipeline (read, compress, pack) but skip uploading |
| `--move-to-cache` | Keep the processed archive in the local cache after push |
| `--write-http-index` | Update the HTTP index file for paired HTTP registries |
| `--no-progress` | Suppress the progress bar |

### Auto-confirm

Some destructive operations ask for confirmation. Pass `-y` / `--yes` globally to skip all prompts:

~~~bash
marina -y push outdoor-run:v2 /path/to/bag/ --registry team-ssh
~~~

## Import

Register an existing local bag directory in the marina catalog without pushing it to a registry:

~~~bash
marina import <dataset> <path>
~~~

~~~bash
marina import outdoor-run:v2 /path/to/outdoor-run/
~~~

The bag is copied into the marina cache and appears immediately in `marina ls` and shell completions. Pass `--move-to-cache` to move instead of copy (free when source and cache are on the same filesystem).

### Recording directly with ros2 bag

Omitting the path prepares a new cache directory and prints its path, making it usable as a recording target:

~~~bash
ros2 bag record -o $(marina import my_recording:session1) /topic1 /topic2
~~~

Marina registers the dataset name before recording starts. Once recording finishes the bag is already in the catalog and ready to push.

## Inspect

Show metadata and file listing for a dataset:

~~~bash
marina inspect <dataset>
~~~

~~~bash
marina inspect outdoor-run:v2
~~~

For a locally cached dataset, inspect lists all files with their sizes and marks the primary recording file. For a remote-only dataset, it queries the configured registries for stored metadata (original size, packed size, hash, push date) and notes that a pull is required to see the full file listing.

Limit the remote lookup to a specific registry:

~~~bash
marina inspect outdoor-run:v2 --registry team-ssh
~~~

If a registry does not respond within the configured [`registry_timeout`](../config.md#settings-fields), it is skipped with a warning and the remaining results are still shown.

## Export

Export a cached dataset to a specific directory (unpacked):

~~~bash
marina export <dataset> <output-path>
~~~

~~~bash
marina export outdoor-run:v2 /tmp/exported-run/
~~~

## Remove

Remove a dataset from the local cache:

~~~bash
marina rm outdoor-run:v2
~~~

Remove from a remote registry:

~~~bash
marina rm outdoor-run:v2 --remote --registry team-ssh
~~~

Update the HTTP index after removing from a registry that has a paired HTTP mirror:

~~~bash
marina rm outdoor-run:v2 --remote --registry team-ssh --write-http-index
~~~

## Clean

Remove all cached datasets from the local cache:

~~~bash
marina clean
~~~

Remove both the cache and the added registry configuration:

~~~bash
marina clean --all
~~~

!!! warning

    `marina clean --all` removes all registry configuration as well. You will need to re-add your registries afterwards.
