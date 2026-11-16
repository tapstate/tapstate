#!/usr/bin/env bash
# Run in the selected product checkout after downloading the CLI and OP image artifacts.
set -euo pipefail

version="$1"
source_sha="$2"
web_sha="$3"
boot_sha256="$4"

cp platform-minimums.txt staging/
(
  cd staging
  for f in tapstate-*.tar.gz; do sha256sum "$f" > "$f.sha256"; done
  sha256sum tapstate-*.tar.gz > checksums.txt
  ls -la
)

mkdir -p image/layout
tar -xf image/server-image.tar -C image/layout
provenance="staging/tapstate-$version-provenance.json"
python3 .github/scripts/web-provenance.py create \
  --oci-layout image/layout \
  --output "$provenance" \
  --version "$version" \
  --tapstate-revision "$source_sha" \
  --web-revision "$web_sha" --web-profile onprem \
  --boot-jar-sha256 "$boot_sha256"
python3 .github/scripts/web-provenance.py verify \
  --oci-layout image/layout --provenance "$provenance" --web-profile onprem \
  --boot-jar-sha256 "$boot_sha256"
.github/scripts/release-manifest.sh "$version" staging image/layout
