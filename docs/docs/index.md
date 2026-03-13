# Introduction

**Marina** is a dataset manager for robotics built to organize, share, and discover ROS 2 bagfiles or datasets across teams and storage backends.

~~~bash
# Pull a dataset from any configured registry
marina pull dlg_feldtage_24:cut

# Pass it straight to your tools
ros2 bag play $(marina resolve dlg_feldtage_24:cut)

# Directly record to local cache with ROS integration
ros2 bag record --all -o $(ros2 bag import my_recording:session1)
~~~

Datasets are pulled from remote registries. You can easily create your own using SSH, Google Drive, HTTP or any filesystem folder.

~~~bash
marina registry add team ssh://user@your-server.org:/data/bags
marina push my-run:v1 /path/to/bag/
~~~

---

**Automatic compression**

Marina automatically compresses bags when pushing and restores them on pull. PointCloud2 messages are compressed with millimetre accuracy using the embedded [cloudini](https://github.com/facontidavide/cloudini) library. This massively reduces network IO — a typical bottleneck for mobile robots.

---

**ROS 2 native**

Marina extends the `ros2 bag` CLI directly when installing for ROS, so the commands you already know just get better:

~~~bash
ros2 bag push my-run:v1 /path/to/bag/
ros2 bag pull my-run:v1
ros2 bag datasets --remote
~~~

Plain folders are supported as well, for non-ROS datasets and metadata.

---

Get started by installing the CLI on your machine: [Installation](installation/packages.md)
