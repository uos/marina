---
hide:
  - toc
---

<div class="sk-intro" markdown>
  <div class="sk-intro-copy" markdown>

<div class="sk-wordmark"><img src="images/marina.svg" alt="" width="72" height="72"><h1>Marina</h1></div>

<p class="sk-tagline">The missing ROS 2 bag<br>and dataset registry.</p>

<p>Organize, share, and discover bags and datasets.</p>

<div class="sk-actions" markdown>

[Get started](installation/ros.md){ .md-button .md-button--primary }
[Set up a registry](features/registries.md){ .md-button }

</div>

  </div>
  <div class="sk-intro-visual" markdown>

```bash
marina pull dlg_feldtage_24:cut
marina resolve dlg_feldtage_24:cut
marina push my-run:v1 ./run --registry lab
```

  </div>
</div>

<div class="sk-highlights">
  <div><strong>Your storage first</strong><span>SSH, Google Drive, HTTP or any filesystem folder.</span></div>
  <div><strong>Compressed clouds</strong><span>Point clouds are reduced to millimeter accuracy by default using <a href="https://github.com/facontidavide/cloudini">Cloudini</a>.</span></div>
  <div><strong>ROS 2 - if you insist</strong><span>Marina is just a binary with TUI and CLI first, library second, and a <code>ros2 bag</code> extension third.</span></div>
</div>

## About

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

Running `marina` without a command opens an interactive explorer over your local cache and every configured registry:

~~~bash
marina
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
    marina push my-run:v1 /path/to/dataset/ --registry team_ssh
    ~~~

---

**Compression**

Marina automatically compresses bags when pushing and restores them on pull.
PointCloud2 messages are compressed with millimeter accuracy by default using the embedded [cloudini](https://github.com/facontidavide/cloudini) library.

Accuracy and compression are transparently marked but can also be changed or disabled at any time through [configuration](./config.md).


Get started by [installing](installation/ros.md) the CLI on your machine!
