#!/bin/bash
# assuming sourced ros environment and starting from the package root directory
# creates the deb in the current dir

# needed for creating the deb
sudo apt update -y && sudo apt-get install --no-install-recommends -y debhelper sed grep

# deps from the package.xml
sudo apt-get install --no-install-recommends -y cargo libclang-dev python3-catkin-pkg-modules libssl-dev python3-setuptools

# ament_cmake_python builds the ros2 bag extension with the first python3 on
# PATH. Use the system one the package installs into, not a tool venv such as
# the one the Lyrical image puts first, which has no setuptools.
PATH=$(printf '%s' "$PATH" | tr ':' '\n' | grep -v '/\.venv/bin$' | paste -sd: -)
export PATH

cargo_ver=$(grep -Po '^\s*version\s*=\s*"\K[^"]+' Cargo.toml)
xml_ver=$(grep -Po '<\s*version\s*>\K[^<]+' package.xml)

if [[ "$cargo_ver" != "$xml_ver" ]]; then
    echo "Versions differ!" >&2
    echo "   Cargo.toml  : $cargo_ver" >&2
    echo "   package.xml : $xml_ver" >&2
    exit 1
fi

shopt -s dotglob nullglob
mkdir marina-src
mv -- * marina-src/
shopt -u dotglob nullglob
tar -cJf "ros-$ROS_DISTRO-marina_$xml_ver.orig.tar.xz" marina-src

cd marina-src

bloom-generate rosdebian
dpkg-buildpackage -us -uc

