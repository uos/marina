#!/bin/bash

# TARGET_FILE="/etc/apt/sources.list.d/ubuntu.sources"
# OLD_URL="http://archive.ubuntu.com/ubuntu/"
# NEW_URL="http://ftp.tu-chemnitz.de/pub/linux/ubuntu-ports"

# if [ -f "$TARGET_FILE" ]; then
#     sed -i "s|^URIs: .*|URIs: $NEW_URL|g" "$TARGET_FILE"
# else
#     sed -i "s|archive.ubuntu.com/ubuntu/|ftp.tu-chemnitz.de/pub/linux/ubuntu-ports/|g" /etc/apt/sources.list
# fi

apt-get update -y
