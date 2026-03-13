# Packages

Marina is packaged for all common platforms. Pick the method that works best for your environment.

The following options give you the standalone `marina` binary. See the [ROS 2 section](./ros.md) for the ROS cli extension.

## PyPI

Marina provides a Python wheel for Linux and MacOS. It gives you both the `marina` CLI and a Python API.

~~~bash
# CLI via uv
uv tool install marina-cli

# or both with pip
pip install marina-cli
~~~

## Ubuntu (PPA)

Supported Distributions

- **Ubuntu Noble** (24.04): `amd64`, `arm64`
- **Ubuntu Jammy** (22.04): `amd64`, `arm64`

After setting up the [UOS Robotics PPA](https://codeberg.org/uos-robotics/ppa/src/branch/pages/README.md), install Marina with apt:

~~~bash
sudo apt install marina
~~~

## Arch Linux (AUR)

Use your preferred AUR helper:

~~~bash
paru -S marina
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
nix run git+https://codeberg.org/stelzo/marina

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


