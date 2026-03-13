# Shell Completions

Once installed, pressing <kbd>Tab</kbd> expands subcommands, flags, and dataset names from both your local cache and all configured remote registries.

!!! info "Packages"

    If you installed Marina via the AUR, the Homebrew tap or non-ROS deb package (PPA), completion scripts get automatically installed.

## Setup


When installed from source, manual binary installation or a non-supporting install method, you can setup completions using the downloaded binary as follows.

!!! warning "No Completions for ROS 2"

    Proper completions are only supported with the `marina` binary without `ros2 bag`.

=== "Bash"

    === "Linux"

        ~~~bash
        marina completions bash | sudo tee /etc/bash_completion.d/marina > /dev/null
        ~~~

    === "macOS (Homebrew)"

        ~~~bash
        marina completions bash > $(brew --prefix)/etc/bash_completion.d/marina
        ~~~

    After installing, reload your shell or source the completion file:

    ~~~bash
    source /etc/bash_completion.d/marina
    ~~~

=== "Zsh"

    === "Linux"

        ~~~bash
        mkdir -p ~/.zsh/completions
        marina completions zsh > ~/.zsh/completions/_marina
        ~~~

    === "macOS (Homebrew)"

        ~~~bash
        marina completions zsh > $(brew --prefix)/share/zsh/site-functions/_marina
        ~~~

    Add the following lines to your `~/.zshrc` **before** the `compinit` call:

    ~~~bash
    fpath=(~/.zsh/completions $fpath)
    autoload -U compinit && compinit
    ~~~

=== "Fish"

    === "Linux"

        ~~~bash
        marina completions fish > ~/.config/fish/completions/marina.fish
        ~~~

    === "macOS (Homebrew)"

        ~~~bash
        marina completions fish > $(brew --prefix)/share/fish/completions/marina.fish
        ~~~

    Fish picks up completions automatically.

## Dataset Name Completion

Marina completes dataset names for `pull`, `push`, `resolve`, `export`, and `rm`.

Completions are always instant and get refreshed automatically in the background:

- When the index is missing or older than [`completion_cache_ttl_secs`](../../config.md#settings-fields) (default 10 minutes)
- After every `marina push`
- After `marina registry add` or `marina registry rm`

You can also trigger a manual refresh at any time:

~~~bash
marina complete-refresh &
~~~

