#!/usr/bin/env bash
# Regression cases for the explicit Web checkout and generated-resource contract.

set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
script="$repo_root/scripts/prepare-web-assets.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

failures=0
pass() { echo "ok - $1"; }
fail() { echo "not ok - $1" >&2; failures=$((failures + 1)); }

web="$work/web"
git init -q "$web"
git -C "$web" config user.email web-smoke@example.invalid
git -C "$web" config user.name web-smoke
mkdir -p "$web/apps/web/dist/assets"
printf '<!doctype html><title>Tapstate</title>\n' >"$web/apps/web/dist/index.html"
printf 'console.log("bundle")\n' >"$web/apps/web/dist/assets/app.js"
printf '{"packageManager":"pnpm@11.10.0"}\n' >"$web/package.json"
printf 'lockfileVersion: 9.0\n' >"$web/pnpm-lock.yaml"
git -C "$web" add .
git -C "$web" commit -qm fixture
sha="$(git -C "$web" rev-parse HEAD)"

out="$work/out"
if "$script" --web-root "$web" --web-revision "$sha" --output "$out" >/dev/null &&
   test -f "$out/static/index.html" &&
   test -f "$out/static/assets/app.js" &&
   grep -Fxq "revision=$sha" "$out/META-INF/tapstate-web.properties" &&
   test -s "$out/META-INF/tapstate-web.files.sha256"; then
    pass "a clean checkout at the explicit revision is staged with provenance"
else
    fail "a clean checkout at the explicit revision is staged with provenance"
fi

if "$script" --web-root "$web" --web-revision deadbeef --output "$work/unknown" >/dev/null 2>&1; then
    fail "an unknown revision is refused"
else
    pass "an unknown revision is refused"
fi

printf 'changed\n' >>"$web/apps/web/dist/index.html"
if "$script" --web-root "$web" --web-revision "$sha" --output "$work/dirty" >/dev/null 2>&1; then
    fail "a dirty checkout is refused"
else
    pass "a dirty checkout is refused"
fi
git -C "$web" checkout -- apps/web/dist/index.html
rm "$web/apps/web/dist/index.html"
if "$script" --web-root "$web" --web-revision "$sha" --output "$work/missing-index" >/dev/null 2>&1; then
    fail "a missing production entry point is refused"
else
    pass "a missing production entry point is refused"
fi

if [ "$failures" -gt 0 ]; then
    echo "prepare-web-assets-smoke: $failures failure(s)" >&2
    exit 1
fi
echo "prepare-web-assets-smoke: all cases passed"
