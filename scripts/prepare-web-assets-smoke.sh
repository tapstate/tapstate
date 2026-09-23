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
if "$script" --web-root "$web" --web-revision "$sha" \
   --tapstate-revision 0123456789abcdef0123456789abcdef01234567 \
   --release-version 0.5.0 --output "$out" >/dev/null &&
   test -f "$out/static/index.html" &&
   test -f "$out/static/assets/app.js" &&
   grep -Fxq "revision=$sha" "$out/META-INF/tapstate-web.properties" &&
   grep -Fxq 'tapstate.revision=0123456789abcdef0123456789abcdef01234567' "$out/META-INF/tapstate-web.properties" &&
   grep -Fxq 'release.version=0.5.0' "$out/META-INF/tapstate-web.properties" &&
   test -s "$out/META-INF/tapstate-web.files.sha256"; then
    pass "a clean checkout at the explicit revision is staged with provenance"
else
    fail "a clean checkout at the explicit revision is staged with provenance"
fi

if "$script" --web-root "$web" --web-revision short \
   --tapstate-revision 0123456789abcdef0123456789abcdef01234567 \
   --release-version 0.5.0 --output "$work/short-revision" >/dev/null 2>&1; then
    fail "a non-full Web revision is refused"
else
    pass "a non-full Web revision is refused"
fi

if "$script" --web-root "$web" --web-revision "$sha" \
   --tapstate-revision 0123456789abcdef0123456789abcdef01234567 \
   --release-version 0x5.0 --output "$work/invalid-version" >/dev/null 2>&1; then
    fail "an invalid release version is refused"
else
    pass "an invalid release version is refused"
fi

if "$script" --web-root "$web" --web-revision deadbeef \
   --tapstate-revision 0123456789abcdef0123456789abcdef01234567 \
   --release-version 0.5.0 --output "$work/unknown" >/dev/null 2>&1; then
    fail "an unknown revision is refused"
else
    pass "an unknown revision is refused"
fi

git -C "$web" commit --allow-empty -qm second-commit
if "$script" --web-root "$web" --web-revision "$sha" \
   --tapstate-revision 0123456789abcdef0123456789abcdef01234567 \
   --release-version 0.5.0 --output "$work/mismatched-head" >/dev/null 2>&1; then
    fail "a valid but mismatched revision is refused"
else
    pass "a valid but mismatched revision is refused"
fi
sha="$(git -C "$web" rev-parse HEAD)"

printf 'changed\n' >>"$web/apps/web/dist/index.html"
if "$script" --web-root "$web" --web-revision "$sha" \
   --tapstate-revision 0123456789abcdef0123456789abcdef01234567 \
   --release-version 0.5.0 --output "$work/dirty" >/dev/null 2>&1; then
    fail "a dirty checkout is refused"
else
    pass "a dirty checkout is refused"
fi
git -C "$web" checkout -- apps/web/dist/index.html
printf 'untracked\n' >"$web/untracked.txt"
if "$script" --web-root "$web" --web-revision "$sha" \
   --tapstate-revision 0123456789abcdef0123456789abcdef01234567 \
   --release-version 0.5.0 --output "$work/untracked" >/dev/null 2>&1; then
    fail "an untracked checkout is refused"
else
    pass "an untracked checkout is refused"
fi
rm "$web/untracked.txt"
rm "$web/apps/web/dist/index.html"
if "$script" --web-root "$web" --web-revision "$sha" \
   --tapstate-revision 0123456789abcdef0123456789abcdef01234567 \
   --release-version 0.5.0 --output "$work/missing-index" >/dev/null 2>&1; then
    fail "a missing production entry point is refused"
else
    pass "a missing production entry point is refused"
fi

if [ "$failures" -gt 0 ]; then
    echo "prepare-web-assets-smoke: $failures failure(s)" >&2
    exit 1
fi
echo "prepare-web-assets-smoke: all cases passed"
