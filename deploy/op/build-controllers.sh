#!/usr/bin/env bash
# Build from the same source checkout as the OP server.
set -euo pipefail
output="$(mkdir -p "$1" && cd "$1" && pwd)"
cd "$(dirname "$0")/controller"
for platform in linux-amd64 darwin-arm64; do
    mkdir -p "$output/$platform"
    GOOS="${platform%-*}" GOARCH="${platform#*-}" CGO_ENABLED=0 \
        go build -trimpath -o "$output/$platform/tapstate-ctl" .
done
