#!/bin/sh
#
# Refuse an assembled site whose functions and routing disagree.
#
# The site is two static scripts plus one function, and the function is reachable only through a
# rewrite. Those two travel separately -- the release path takes each from whichever of the tag or the
# checkout has it -- so they can disagree, and when they do nothing downstream says so: the deploy
# succeeds, both static entry points still match byte for byte, and the route answers 404 while the
# function sits there working. Shipped exactly that way once: a receiver taken from the default branch
# behind routing taken from a tag that had never heard of it. The installer's report fails silently by
# design, so a whole release could have gone out collecting nothing.
#
# Two directions, because only checking one leaves the other free:
#   a function nothing routes to   -- deployed, working, unreachable. The incident above.
#   a route with no function       -- 404 on a path the routing promises.
#
# Usage: check-site.sh <assembled-dir>
set -eu

dir="${1:?usage: check-site.sh <assembled-dir>}"
die() { printf 'check-site: %s\n' "$1" >&2; exit 1; }

[ -d "$dir" ] || die "$dir is not a directory"
[ -f "$dir/vercel.json" ] || die "$dir has no vercel.json, so nothing routes to anything"
command -v python3 >/dev/null 2>&1 || die "python3 is required"

DIR="$dir" python3 - <<'PY'
import json, os, sys

d = os.environ["DIR"]
with open(os.path.join(d, "vercel.json")) as fh:
    try:
        cfg = json.load(fh)
    except ValueError as e:
        print("check-site: vercel.json is not valid JSON: %s" % e, file=sys.stderr)
        raise SystemExit(1)

# Vercel serves api/<name>.js at /api/<name> on its own, but the site promises the short path, so the
# rewrite is what the installer actually calls. A function reachable only at its default path is the
# defect: every client we ship asks for the short one.
destinations = {r.get("destination", "") for r in cfg.get("rewrites", []) if isinstance(r, dict)}

api_dir = os.path.join(d, "api")
functions = sorted(
    n[:-len(".js")] for n in (os.listdir(api_dir) if os.path.isdir(api_dir) else [])
    if n.endswith(".js") and not n.endswith(".test.js")
)

bad = []
for name in functions:
    route = "/api/%s" % name
    if route not in destinations:
        bad.append("api/%s.js is deployed but no rewrite points at %s -- it would answer only on its "
                   "default path, and nothing we ship asks for that" % (name, route))

for dest in sorted(destinations):
    if dest.startswith("/api/"):
        target = os.path.join(d, dest.lstrip("/") + ".js")
        if not os.path.exists(target):
            bad.append("a rewrite points at %s but %s.js is not in the site -- that path would 404"
                       % (dest, dest.lstrip("/")))

if bad:
    for line in bad:
        print("check-site: " + line, file=sys.stderr)
    raise SystemExit(1)

print("check-site: %d function(s) and their routes agree" % len(functions))
PY
