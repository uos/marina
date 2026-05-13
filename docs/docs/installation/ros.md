# ROS 2

Marina integrates seamlessly with the ROS 2 toolchain by extending the `ros2 bag` CLI with `push`, `pull` etc.

## Binaries

Pre-built ROS packages are available through the [UOS Robotics PPA](https://codeberg.org/uos-robotics/ppa/src/branch/pages/README.md).

After setting up the PPA, install the package for your ROS distribution:

~~~bash
# Jazzy
sudo apt install ros-jazzy-marina

# Humble
sudo apt install ros-humble-marina
~~~

## Source

You can also build Marina as a ROS 2 package from source using `colcon`.

You will need a modern Rust compiler (1.85 or newer). Install it via rustup:

~~~bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
~~~

Then clone the Marina repository into your ROS workspace and build:

~~~bash
cd ~/ros2_ws/src
git clone https://codeberg.org/stelzo/marina.git
cd ..

rosdep install --from-paths src -y --ignore-src

colcon build --packages-select marina
source install/local_setup.bash
~~~

!!! note "Higher Disk Space Requirement"

    Building from source generates incremental Rust compilation artifacts. We recommend using the binary package from the PPA wherever possible.

## CLI Renaming

Once installed and sourced, all Marina commands are available under `ros2 bag`. 

Some examples:
~~~bash
# pull existing bag
ros2 bag pull dlg_feldtage_24:cut

# record directly to local cache
ros2 bag record -o $(ros2 bag import my-dataset:v1) --all
~~~

!!! warning "`list` → `datasets`"

    `ros2 bag list` is already taken by the ROS toolchain and lists available storage plugins.
    Marina's list command is exposed as `ros2 bag datasets` (alias: `ros2 bag ds`) within the ROS CLI extension.

    ~~~bash
    ros2 bag datasets --remote
    ros2 bag ds --remote --registry osnabotics_public
    ~~~
