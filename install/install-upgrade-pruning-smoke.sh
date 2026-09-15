#!/usr/bin/env bash
# One black-box case: upgrading a complete bundle removes the previous version directory.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
STUB="$(mktemp -d)"
trap 'rm -rf "$STUB"' EXIT
INSTALL_DIR="$STUB/install/bin"
mkdir -p "$STUB/shim"
cat > "$STUB/shim/uname" <<'EOF'
#!/bin/sh
case "$1" in
  -s) echo Darwin ;;
  -m) echo arm64 ;;
  *) echo unknown ;;
esac
EOF
chmod +x "$STUB/shim/uname"

make_asset() {
  local version="$1" stage="$STUB/stage/$1" release="$STUB/download/v$1" asset
  mkdir -p "$stage/tapstate-cli-$version/bin" "$stage/tapstate-cli-$version/libexec" "$release"
  printf '#!/bin/sh\necho "tapstate %s"\n' "$version" > "$stage/tapstate-cli-$version/bin/tapstate"
  printf '#!/bin/sh\necho mcp\n' > "$stage/tapstate-cli-$version/libexec/tapstate-mcp"
  chmod +x "$stage/tapstate-cli-$version/bin/tapstate" "$stage/tapstate-cli-$version/libexec/tapstate-mcp"
  asset="tapstate-$version-darwin-arm64.tar.gz"
  tar -czf "$release/$asset" -C "$stage" "tapstate-cli-$version"
  (
    cd "$release"
    if command -v sha256sum >/dev/null 2>&1; then
      sha256sum "$asset" > "$asset.sha256"
    else
      shasum -a 256 "$asset" > "$asset.sha256"
    fi
  )
  : > "$release/platform-minimums.txt"
}

fail() { printf 'FAIL  %s\n' "$1" >&2; exit 1; }

install_version() {
  local version="$1"
  if ! PATH="$STUB/shim:$PATH" TAPSTATE_TELEMETRY=off \
      TAPSTATE_BASE_URL="file://$STUB" TAPSTATE_INSTALL_DIR="$INSTALL_DIR" \
      TAPSTATE_VERSION="$version" sh "$HERE/install.sh" > "$STUB/install.log" 2>&1; then
    cat "$STUB/install.log" >&2
    fail "fixture installation failed for $version"
  fi
  [ -L "$INSTALL_DIR/tapstate" ] && \
    [ "$(readlink "$INSTALL_DIR/tapstate")" = "versions/$version/bin/tapstate" ] && \
    [ "$("$INSTALL_DIR/tapstate" --version)" = "tapstate $version" ] && \
    [ -x "$INSTALL_DIR/versions/$version/libexec/tapstate-mcp" ] || \
    fail "installation did not activate a complete $version bundle"
}

make_asset 0.2.1
make_asset 0.3.0
install_version 0.2.1
install_version 0.3.0

[ ! -e "$INSTALL_DIR/versions/0.2.1" ] || \
  fail "upgrade activated 0.3.0 but retained the older versions/0.2.1 directory"
printf 'PASS  upgrade prunes the previous version directory\n'
