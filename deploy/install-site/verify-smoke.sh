#!/usr/bin/env bash
#
# Test harness for deploy/install-site/verify-published.sh. Serves the three entry points from a local
# loopback stub -- no network, no Vercel -- and drives the verifier against bodies that are correct,
# stale on one side, stale on the other, routed to the wrong file, and missing.
#
# The one-sided cases are the point of the harness. A verifier that checks only the CLI entry point
# passes case 2 and fails nothing, which is exactly the hole that let the quickstart entry point sit a
# release behind while the one that was watched looked fine. Each case here fails a verifier that
# drops the route it names -- the alias included, because a rewrite is a thing a deploy can get wrong
# on its own.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
VERIFY="$HERE/verify-published.sh"

PASS=0; FAIL=0
ok()  { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }

command -v python3 >/dev/null 2>&1 || { printf 'python3 is required for the loopback stub\n' >&2; exit 1; }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"; [ -n "${STUB_PID:-}" ] && kill "$STUB_PID" 2>/dev/null' EXIT

# The tree side: what a deployment would upload.
EXPECTED="$TMP/expected"
mkdir -p "$EXPECTED"
printf 'CLI_VERSION="9.9.9"\n# quickstart\n'    > "$EXPECTED/quickstart.sh"
printf 'PINNED_VERSION="9.9.9"\n# installer\n'  > "$EXPECTED/install.sh"

# The served side: swapped per case by rewriting these two files. Which route serves which file is
# read from ROUTES on every request, so a case can also swap the routing itself.
SERVED="$TMP/served"
mkdir -p "$SERVED"
ROUTES="$TMP/routes"

cat > "$TMP/stub.py" <<'PY'
import http.server, socketserver, sys, os

root, portfile, routesfile = sys.argv[1], sys.argv[2], sys.argv[3]

def routes():
    table = {}
    with open(routesfile) as fh:
        for line in fh:
            if line.strip():
                path, name = line.split()
                table[path] = name
    return table

class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        name = routes().get(self.path)
        path = os.path.join(root, name) if name else None
        if not path or not os.path.exists(path):
            self.send_response(404); self.end_headers(); return
        body = open(path, "rb").read()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass

srv = socketserver.TCPServer(("127.0.0.1", 0), H)
open(portfile, "w").write(str(srv.server_address[1]))
srv.serve_forever()
PY

# The mapping the site publishes: the root and its alias install the CLI, the demo has its own name.
route_current() { printf '/ install.sh\n/cli install.sh\n/demo quickstart.sh\n' > "$ROUTES"; }
# The mapping the site used to publish: the root was the demo. A deploy that ships this rewrite
# serves every byte it was given and still hands the wrong script to whoever pipes the root.
route_previous() { printf '/ quickstart.sh\n/cli install.sh\n/demo quickstart.sh\n' > "$ROUTES"; }
route_current

python3 "$TMP/stub.py" "$SERVED" "$TMP/port" "$ROUTES" &
STUB_PID=$!
for _ in $(seq 1 50); do [ -s "$TMP/port" ] && break; sleep 0.1; done
[ -s "$TMP/port" ] || { printf 'the loopback stub never came up\n' >&2; exit 1; }
BASE="http://127.0.0.1:$(cat "$TMP/port")"

serve_fresh_quickstart() { cp "$EXPECTED/quickstart.sh" "$SERVED/quickstart.sh"; }
serve_fresh_installer()  { cp "$EXPECTED/install.sh"    "$SERVED/install.sh"; }
serve_stale_quickstart() { printf 'CLI_VERSION="9.9.8"\n# quickstart\n'   > "$SERVED/quickstart.sh"; }
serve_stale_installer()  { printf 'PINNED_VERSION="9.9.8"\n# installer\n' > "$SERVED/install.sh"; }

run_verify() { sh "$VERIFY" "$BASE" "$EXPECTED" 2>&1; }

# --- 1. every entry point matches ---------------------------------------------------------------
serve_fresh_quickstart; serve_fresh_installer
out="$(run_verify)"; rc=$?
if [ "$rc" -eq 0 ]; then ok "matching bodies on every entry point verify clean"
else bad "matching bodies were rejected (rc=$rc): $out"; fi

# --- 2. the demo entry point is stale -------------------------------------------------------------
# Fails a verifier that watches only the CLI routes -- the shape that actually shipped.
serve_stale_quickstart; serve_fresh_installer
out="$(run_verify)"; rc=$?
if [ "$rc" -ne 0 ] && grep -q "$BASE/demo is not what this tree would deploy" <<<"$out"; then
  ok "a stale demo entry point is caught and named"
else bad "a stale demo entry point was not caught (rc=$rc): $out"; fi

# --- 3. the CLI entry point is stale, at the root and at its alias --------------------------------
# Both routes are named. A verifier that checks the alias and infers the root -- or the reverse --
# reports the one it looked at and stays silent about the other, and the two are separate rewrites.
serve_fresh_quickstart; serve_stale_installer
out="$(run_verify)"; rc=$?
if [ "$rc" -ne 0 ] && grep -q "$BASE/ is not what this tree would deploy" <<<"$out" \
   && grep -q "$BASE/cli is not what this tree would deploy" <<<"$out"; then
  ok "a stale CLI entry point is caught and named at / and at /cli"
else bad "a stale CLI entry point was not caught on both routes (rc=$rc): $out"; fi

# --- 3b. every body is current but the root is routed to the demo --------------------------------
# The previous mapping, served by a deploy whose files are all fresh. Nothing is stale, and the root
# still hands the demo to whoever asked for the CLI; only a check that reads each route against the
# file it is supposed to serve sees it, and it has to name the route, not a version.
serve_fresh_quickstart; serve_fresh_installer; route_previous
out="$(run_verify)"; rc=$?
if [ "$rc" -ne 0 ] && grep -q "$BASE/ is not what this tree would deploy" <<<"$out" \
   && ! grep -q "$BASE/cli is not what" <<<"$out" && ! grep -q "$BASE/demo is not what" <<<"$out"; then
  ok "a root routed to the wrong file is caught and named, with the other routes left clean"
else bad "a root routed to the wrong file was not caught as such (rc=$rc): $out"; fi
route_current

# --- 4. the failure message names the two versions ------------------------------------------------
# A digest mismatch alone does not tell anyone what to do; the pins are what identify the skew.
serve_fresh_quickstart; serve_stale_installer
out="$(run_verify)"; rc=$?
if grep -q 'served  pin 9.9.8' <<<"$out" && grep -q 'tree    pin 9.9.9' <<<"$out"; then
  ok "the failure names the served pin and the tree's pin"
else bad "the failure does not name both pins: $out"; fi

# --- 5. a missing entry point is a failure, not a pass --------------------------------------------
serve_fresh_installer; rm -f "$SERVED/quickstart.sh"
out="$(run_verify)"; rc=$?
if [ "$rc" -ne 0 ] && grep -q "$BASE/demo could not be fetched" <<<"$out"; then
  ok "an entry point that does not answer fails loudly"
else bad "a missing entry point did not fail (rc=$rc): $out"; fi

# --- 6. same pin, different body ------------------------------------------------------------------
# The case that separates this verifier from a version comparison. An installer fix that ships without
# a version bump leaves both sides reading the same pin while the served script is the old one, and a
# check built on the version number calls that healthy.
serve_fresh_quickstart
printf 'PINNED_VERSION="9.9.9"\n# installer, but an older body\n' > "$SERVED/install.sh"
out="$(run_verify)"; rc=$?
if [ "$rc" -ne 0 ]; then ok "a stale body carrying the current pin is still caught"
else bad "a stale body with a matching pin passed (rc=$rc): $out"; fi

# --- 7. no digest tool at all -------------------------------------------------------------------
# The failure mode is a pass, not an error: with the refusal inside digest(), the exit ended only the
# command substitution, both sides came back empty, and two empty strings compare equal. A verifier
# that reports ok when it could not hash anything is worse than one that is absent.
serve_fresh_quickstart; serve_fresh_installer
# A PATH with everything the script needs except a way to hash. Emptying PATH instead would only
# prove that sh cannot be found.
nodigest="$(mktemp -d)"
for tool in sh curl awk sed grep cat rm mktemp printf; do
  real="$(command -v "$tool" 2>/dev/null)" && ln -sf "$real" "$nodigest/$tool"
done
out="$(PATH="$nodigest" sh "$VERIFY" "$BASE" "$EXPECTED" 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && ! grep -q '^ok ' <<<"$out" && grep -qi 'no sha256 tool' <<<"$out"; then
  ok "with no digest tool it refuses instead of reporting a result"
else
  bad "reported a result without being able to hash (rc=$rc): $out"
fi
rm -rf "$nodigest"

# --- 8. the checksum command exists but fails -----------------------------------------------------
# Distinct from case 7, and it survived that fix: the tool is present, so the up-front check passes,
# and it fails when actually run. The status of a pipeline is its last command's, so awk reported
# success over empty output -- and BOTH sides come back empty, which compare equal. That is the same
# match-on-nothing the up-front refusal removed, one layer down.
#
# Both sides have to fail for the bug to show. A case where only one input is unhashable proves
# nothing: the other digest is real, the two differ, and the verifier fails for the wrong reason.
serve_fresh_quickstart; serve_fresh_installer
failing="$(mktemp -d)"
printf '#!/bin/sh\nexit 1\n' > "$failing/sha256sum"; chmod +x "$failing/sha256sum"
printf '#!/bin/sh\nexit 1\n' > "$failing/shasum";    chmod +x "$failing/shasum"
out="$(PATH="$failing:$PATH" sh "$VERIFY" "$BASE" "$EXPECTED" 2>&1)"; rc=$?
if [ "$rc" -ne 0 ] && ! grep -q '^ok ' <<<"$out"; then
  ok "a checksum command that fails is a failure, not two empty digests that match"
else
  bad "a failed checksum was reported as a match (rc=$rc): $out"
fi
rm -rf "$failing"

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
