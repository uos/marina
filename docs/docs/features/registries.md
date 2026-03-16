# Registries

A registry is a named storage location where Marina keeps datasets. You can have as many registries as you like. Marina searches all of them when you run `pull` or `search`.

## Default Registry

Marina is packaged with a pre-configured public registry from the german non-profit organization [Osnabotics e.V.](https://www.osnabotics.org). The registry already provides some datasets for SLAM. You can disable the default registry at compile time or remove it from the config at `~/.config/marina/marina.rl` . If you'd like to add your dataset to the Osnabotics registry, make sure to use MCAP storage if using ROS 2 and reach out to us via Email [marina@osnabotics.org](mailto:marina@osnabotics.org) with a publicly accessible Google Drive folder ID and a short description.

## List Registries

~~~bash
marina registry ls
~~~

## Add a Registry

~~~bash
marina registry add <name> <uri>
~~~

### SSH

~~~bash
marina registry add team_ssh ssh://user@your-server.org:/path/to/registry
~~~

Marina uses SSH key authentication. Set up passwordless login first:

~~~bash
ssh-copy-id -i ~/.ssh/id_ed25519.pub user@your-server.org
~~~

??? info "Add your key to SSH agent"

    If your key has a non-standard name and Marina still asks for a password, add it to the SSH agent:

    ~~~bash
    eval "$(ssh-agent -s)"
    ssh-add ~/.ssh/privatekey
    ~~~

Alternatively, pass the key path explicitly:

~~~bash
marina registry add team_ssh ssh://user@your-server.org:/path/to/reg \
  --auth-env MARINA_SSH_KEY
~~~

Set the environment variable to the path of your private key before running Marina.

### Google Drive

You will need the folder ID — the part after `folders/` in the Drive Share URL.

For example, in `https://drive.google.com/drive/folders/10hjoMIyWTOVNOo3zDOfHoSb1S55gO3rJ`, the folder ID is `10hjo...`.

~~~bash
# Public folder. Read-only without auth
marina registry add public_drive gdrive://<folder-id>

# Private or writable. Authenticate after adding
marina registry add my_drive gdrive://<folder-id>
marina registry auth my_drive
~~~

`marina registry auth` opens a browser window for the Google OAuth flow. Credentials are persisted locally in `~/.config/marina/tokens`, so avoid adding them to git.

!!! warning "Private Folders"

    Marina currently cannot access privately shared folders (shared only with specific users). If your data is sensitive but you want to share it, use a private SSH registry instead.

### HTTP / HTTPS

HTTP registries are read-only, so you can pull and search but `push` and `rm` are not supported. They are typically maintained by pushing through a corresponding SSH registry with `--write-http-index`.

~~~bash
marina registry add web_main https://datasets.example.org/marina
~~~

### Local Folder

Any local or network-mounted directory works:

~~~bash
# Local directory
marina registry add local folder://./local-registry

# NFS / SMB mount
marina registry add nas folder:///mnt/nas/marina
~~~

Access control is handled by the underlying filesystem permissions.

## Remove a Registry

~~~bash
marina registry rm <name>
~~~

To also delete all data stored in the registry:

~~~bash
marina registry rm <name> --delete-data
~~~

!!! danger

    `--delete-data` permanently removes all datasets stored in that registry. This cannot be undone.

## Mirror a Registry

Copy all datasets from one registry to another:

~~~bash
marina registry mirror <source> <target>
~~~

Useful for backing up a remote registry to local storage or migrating between backends.

## Auth Status

Check whether a Google Drive registry is currently authenticated:

~~~bash
marina registry auth <name> --status
~~~
