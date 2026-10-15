# Packages

Marina is packaged for all common platforms. Pick the method that works best for your environment.

The following options give you the standalone `marina` binary. See the [ROS 2 section](./ros.md) for the ROS cli extension.

## PyPI

Marina publishes two wheels to PyPI.

- `marina-cli` Marina binary
- `marina-py` The python module

~~~bash
# CLI only
uv tool install marina-cli
# or
pip install marina-cli

# Python API
pip install marina-py
~~~

## Ubuntu (PPA)

After setting up the [steado PPA](https://ppa.steado.tech/), install Marina with apt:

??? note "Set up the PPA"

    Run this once on Ubuntu 22.04, 24.04 or 26.04.

    ~~~sh
    curl -fsSL "https://ppa.steado.tech/ubuntu/key.gpg" | gpg --dearmor \
      | sudo tee /usr/share/keyrings/steado-archive-keyring.gpg >/dev/null
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/steado-archive-keyring.gpg] https://ppa.steado.tech/ubuntu $(. /etc/os-release && echo $UBUNTU_CODENAME) main" \
      | sudo tee /etc/apt/sources.list.d/steado.list
    sudo apt update
    ~~~

~~~bash
sudo apt install marina
~~~

## Arch Linux (AUR)

Use your preferred AUR helper:

~~~bash
paru -S marina
# or maybe
yay -S marina-bin
~~~

## Homebrew (macOS)

A tap with precompiled binaries is available for both Apple Silicon and Intel Macs.

~~~bash
brew tap uos/marina
brew install marina
~~~

## cargo-binstall

The [cargo-binstall](https://github.com/cargo-bins/cargo-binstall) target-triple tarballs are also available for supported targets.

~~~bash
cargo binstall marina
~~~


## Nix

Marina ships a flake. Run it directly or add it to your configuration:

~~~bash
# Run without installing
nix run git+https://codeberg.org/stelzo/marina -- ls --remote

# Install into your profile
nix profile install git+https://codeberg.org/stelzo/marina
~~~

Or as a flake input:

~~~nix
inputs.marina.url = "git+https://codeberg.org/stelzo/marina";
~~~

## Cargo

Marina is on [crates.io](https://crates.io/crates/marina), so Rustaceans can install it directly with Cargo.

The only build-time requirement beyond the Rust toolchain is a C compiler.

~~~bash
cargo install marina
~~~


