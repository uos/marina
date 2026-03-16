# Introduction

**Marina** is a dataset manager for robotics built to organize, share, and discover bags and datasets across teams and storage backends.

=== "ROS 2 CLI"

    ~~~bash
    # Pull a dataset from any configured registry...
    ros2 bag pull dlg_feldtage_24:cut

    # and pass it straight to playback
    ros2 bag play $(ros2 bag resolve dlg_feldtage_24:cut)

    # See available bags from configured remotes
    ros2 bag datasets --remote
    # or use the alias
    ros2 bag ds --remote
    ~~~

=== "Standalone"

    ~~~bash
    # Pull a dataset from any configured registry
    marina pull dlg_feldtage_24:cut

    # Get and use the data from the local cache
    cat $(marina resolve dlg_feldtage_24:cut)/metadata.yaml

    # See available bags from configured remotes
    marina list --remote
    # or use the alias
    marina ls --remote
    ~~~

Datasets are pulled from remote registries. You can easily create your own using SSH, Google Drive, HTTP or any filesystem folder.

=== "ROS 2 CLI"

    ~~~bash
    # Record to local cache...
    ros2 bag record --all -o $(ros2 bag import my_recording:session1)

    # and push to your registry.
    ros2 bag push my-run:v1 /path/to/bag/ --registry team_ssh
    ~~~

=== "Standalone"

    ~~~bash
    # Push any folder or bags to your registry.
    marina bag push my-run:v1 /path/to/dataset/ --registry team_ssh
    ~~~

---

**Compression**

Marina automatically compresses bags when pushing and restores them on pull.
PointCloud2 messages are compressed with millimeter accuracy by default using the embedded [cloudini](https://github.com/facontidavide/cloudini) library.

Accuracy and compression are transparently marked but can also be changed or disabled at any time through [configuration](./config.md).


Get started by [installing](installation/ros.md) the CLI on your machine!
