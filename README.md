# marina

`marina` is a dataset manager for robotics to organize, share, and discover datasets and metadata across storage backends so we can finally stop emailing download links around.
The focus lies primarily on ROS 2 bagfiles but plain folders are supported as well.

## Installation

Install a recent Rust toolchain.

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup default stable
rustup update
```

We also need a C++ 20 compiler and `cmake`. Make sure they are installed.

Then install marina.
```bash
cargo install marina
```

Now you can download the demo bag like this.

```bash
marina pull dlg_feldtage_24:cut
```

The bag will be placed in your local marina cache. You can get the path to it with the `resolve` command.
```bash
marina resolve dlg_feldtage_24:cut
```

## Shell Completions

Get automatic expansions in the shell with `Tab`.

### Bash
Add this to your `~/.bashrc` (or `~/.bash_profile` on Mac):

*Linux*
```bash
marina completions bash | sudo tee /etc/bash_completion.d/marina > /dev/null
```

*MacOS*
```bash
marina completions bash > $(brew --prefix)/etc/bash_completion.d/marina
```

### Zsh

```bash
mkdir -p ~/.zsh/completions
marina completions zsh > ~/.zsh/completions/_marina
```

Add these lines to `~/.zshrc` (before compinit)

```bash
fpath=(~/.zsh/completions $fpath)
autoload -U compinit && compinit
```

### Fish

```bash
marina completions fish > ~/.config/fish/completions/marina.fish
```

## ROS 2

Marina provides a ROS package to extend the `ros2 bag` CLI.

```bash
cd ~/ros2_ws/src
git clone ssh://git@codeberg.org/stelzo/marina.git
cd ..
colcon build
```

After sourcing you can `ros2 bag pull` etc. like with marina.

> [!WARNING]
> `ros2 bag list` was already taken, so the `list` command from marina is named `datasets` or `ds` in the ROS 2 CLI extension.

## Compression

Marina heavily uses compression for creating the archives in the registries. For ROS 2 PointCloud2 messages, we embed the [cloudini](https://github.com/facontidavide/cloudini) library for much better compression rates. Marina uses lossy compression with 1mm accuracy by default. You can easily switch to lossless compression or change the accuracy via config or CLI. The defaults for the config, is defined as follows:

`~/.config/marina/registries.toml`
 
```toml
[[registry]]
name = "osnabotics-public"
kind = "gdrive"
uri = "gdrive://10hjoMIyWTOVNOo3zDOfHoSb1S55gO3rJ"


[compression]
pointcloud_mode = "lossy"             # off | lossy | lossless
pointcloud_accuracy_mm = 1.0          # float
packed_mcap_compression = "zstd"      # none | zstd | lz4
packed_archive_compression = "none"   # gzip | none
unpacked_mcap_compression = "lz4"     # none | zstd | lz4
```

If compression flags are provided on the CLI, those values override the config for that command only.

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

> [!NOTE]
> If your key has no typical names and marina still asks you for the password, add the key to the ssh-agent.
> ```bash
> eval "$(ssh-agent -s)"
> ssh-add ~/.ssh/privatekey
> ```


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
