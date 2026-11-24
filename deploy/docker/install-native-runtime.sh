#!/bin/sh
# Shared prerequisites for user-installed and Cloud-bundled Db2 connectors.
set -eu
apt-get update
apt-get install -y --no-install-recommends libxml2 libstdc++6
rm -rf /var/lib/apt/lists/*
