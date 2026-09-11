#!/bin/sh
# Assemble the install site into a directory ready to deploy.
#
# The site is two scripts that live elsewhere in this repo, served under the names a user types:
#   /      -> the CLI-only installer   (install/install.sh)
#   /cli   -> the same installer, kept as an alias for the address already in circulation
#   /demo  -> the full-stack quickstart (deploy/quickstart/quickstart.sh)
#
# The root used to serve the quickstart. It now installs the CLI and nothing else, so that the first
# command a reader meets does one thing; the disposable demo has its own name.
#
# Copying rather than symlinking is deliberate: the deployment uploads file contents, and a symlink
# would upload as a link nobody can follow. The routing itself is vercel.json, checked in beside this
# script so the edge layout is code rather than something reconstructed from memory at deploy time.
#
# Usage: assemble.sh <out-dir>   (out-dir is created; anything already there is left alone)
set -eu

out="${1:?usage: assemble.sh <out-dir>}"
here="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
repo="$(CDPATH='' cd -- "$here/../.." && pwd)"

mkdir -p "$out"
cp "$repo/deploy/quickstart/quickstart.sh" "$out/quickstart.sh"
cp "$repo/install/install.sh"              "$out/install.sh"
cp "$here/vercel.json"                     "$out/vercel.json"
# The event receiver. It is a function, not content: it adds a route (/e) and touches neither of the
# two script routes, which is what keeps the byte-for-byte promise intact by construction rather than
# by care. Tests are not deployed.
mkdir -p "$out/api"
cp "$here/api/event.js"                    "$out/api/event.js"

# The same gate the release path runs. Assembling by hand and deploying that is a real path, and it
# must not be the one where a function with no route slips through.
"$here/check-site.sh" "$out" >/dev/null

echo "assembled into $out:"
echo "  /     <- install/install.sh"
echo "  /cli  <- install/install.sh (alias)"
echo "  /demo <- deploy/quickstart/quickstart.sh"
echo "  /e    <- deploy/install-site/api/event.js (install event receiver)"
