# Raw Archives

Precompiled binaries for common platforms are published with every Marina release.
They are available on [Codeberg Releases](https://codeberg.org/stelzo/marina/releases).


## Debian Package

Since Marina only uses base system libraries, you can easily install new deb packages on other Debian-based distros.

Download a `.deb` and install it directly:

=== "Bash"

    ```bash
    export MARINA_VERSION="0.2.8"
    export MARINA_ARCH=$(dpkg --print-architecture)
    
    curl -Lo marina.deb "[https://codeberg.org/uos-robotics/ppa/raw/branch/pages/ubuntu/pool/main/noble/marina_$MARINA_VERSION-1_$MARINA_ARCH.deb](https://codeberg.org/uos-robotics/ppa/raw/branch/pages/ubuntu/pool/main/noble/marina_$MARINA_VERSION-1_$MARINA_ARCH.deb)"
    sudo dpkg -i marina.deb
    ```

=== "Zsh"

    ```zsh
    export MARINA_VERSION="0.2.8"
    export MARINA_ARCH=$(dpkg --print-architecture)
    
    curl -Lo marina.deb "[https://codeberg.org/uos-robotics/ppa/raw/branch/pages/ubuntu/pool/main/noble/marina_$MARINA_VERSION-1_$MARINA_ARCH.deb](https://codeberg.org/uos-robotics/ppa/raw/branch/pages/ubuntu/pool/main/noble/marina_$MARINA_VERSION-1_$MARINA_ARCH.deb)"
    sudo dpkg -i marina.deb
    ```

=== "Fish"

    ```fish
    set -gx MARINA_VERSION 0.2.8
    set -gx MARINA_ARCH (dpkg --print-architecture)
    
    curl -Lo marina.deb "[https://codeberg.org/uos-robotics/ppa/raw/branch/pages/ubuntu/pool/main/noble/marina_$MARINA_VERSION-1_$MARINA_ARCH.deb](https://codeberg.org/uos-robotics/ppa/raw/branch/pages/ubuntu/pool/main/noble/marina_$MARINA_VERSION-1_$MARINA_ARCH.deb)"
    sudo dpkg -i marina.deb
    ```

## Manual Binary Install

Static musl builds have no system library dependencies and run on virtually any Linux distribution.

Download the binary for your platform, make it executable, and place it on your `$PATH`:

=== "Bash"

    ```bash
    export MARINA_VERSION="0.2.8"
    export MARINA_ARCH=$(uname -m)

    curl -L "https://codeberg.org/stelzo/marina/releases/download/v$MARINA_VERSION/marina-$MARINA_VERSION-$MARINA_ARCH-unknown-linux-musl.tar.gz" | tar xfzv -
    
    chmod +x marina-*/marina .
    mkdir -p ~/.local/bin
    mv marina-*/marina ~/.local/bin/

    # Make sure ~/.local/bin is on your PATH
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
    source ~/.bashrc
    ```

=== "Zsh"

    ```zsh
    export MARINA_VERSION="0.2.8"
    export MARINA_ARCH=$(uname -m)

    curl -L "https://codeberg.org/stelzo/marina/releases/download/v$MARINA_VERSION/marina-$MARINA_VERSION-$MARINA_ARCH-unknown-linux-musl.tar.gz" | tar xfzv -
    
    chmod +x marina-*/marina .
    mkdir -p ~/.local/bin
    mv marina-*/marina ~/.local/bin/

    # Make sure ~/.local/bin is on your PATH
    echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc
    source ~/.zshrc
    ```

=== "Fish"

    ```fish
    set -gx MARINA_VERSION 0.2.8
    set -gx MARINA_ARCH (uname -m)

    curl -L "https://codeberg.org/stelzo/marina/releases/download/v$MARINA_VERSION/marina-$MARINA_VERSION-$MARINA_ARCH-unknown-linux-musl.tar.gz" | tar xfzv -
    
    chmod +x marina-*/marina .
    mkdir -p ~/.local/bin
    mv marina-*/marina ~/.local/bin/

    # Make sure ~/.local/bin is on your PATH
    fish_add_path ~/.local/bin
    ```
