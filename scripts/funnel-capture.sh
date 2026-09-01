#!/bin/sh
#
# Keep the public distribution counters that expire.
#
# GitHub's traffic API serves only the trailing 14 days, so a counter not captured within that window
# is gone permanently. This job appends to a durable series so the numbers survive the window.
#
# Two measurement types, deliberately stored differently, because conflating them is the defect that
# produces a plausible and badly wrong number:
#
#   daily observation   /traffic/clones, /traffic/views. The API returns one row per day for the last
#                       14 days, and re-sends rows it has already sent. Stored keyed by date and
#                       upserted, so re-running over an overlapping window changes nothing.
#   cumulative snapshot release asset download_count, and Docker Hub pulls. These are running totals,
#                       not "that day's downloads". Stored as (observed_at, value); a period's figure
#                       is the difference between two snapshots, never the snapshot itself.
#
# Everything captured here is a DISTRIBUTION SIGNAL. These numbers are not people and are never summed
# with each other: one person contributes clones, downloads and pulls at once, and CI contributes far
# more than any person. Measured over 2026-08-18..08-31: 6525 clones from 316 unique cloners against
# 750 views from 34 unique visitors, weekends collapsing and Mondays spiking -- the shape of automation.
# The funnel denominator is the installer callback, not anything in this file.
#
# Usage:
#   funnel-capture.sh --store <dir> [--fixture-dir <dir>] [--observed-at <iso8601>]
#   funnel-capture.sh --store <dir> --delta <metric>
#   funnel-capture.sh --store <dir> --gaps
#
# --fixture-dir reads canned API payloads instead of calling the network, which is what makes this
# testable with no credentials.
set -eu

store=""
fixture_dir=""
observed_at=""
mode="capture"
metric=""
repo="${FUNNEL_REPO:-tapstate/tapstate}"

die() { printf 'funnel-capture: %s\n' "$1" >&2; exit 1; }

while [ $# -gt 0 ]; do
    case "$1" in
        --store)       store="${2:?--store needs a directory}"; shift 2 ;;
        --fixture-dir) fixture_dir="${2:?--fixture-dir needs a directory}"; shift 2 ;;
        --observed-at) observed_at="${2:?--observed-at needs a timestamp}"; shift 2 ;;
        --delta)       mode="delta"; metric="${2:?--delta needs a metric}"; shift 2 ;;
        --gaps)        mode="gaps"; shift ;;
        *)             die "unknown argument: $1" ;;
    esac
done

[ -n "$store" ] || die "--store is required"
command -v python3 >/dev/null 2>&1 || die "python3 is required"

if [ "$mode" = "capture" ]; then
    mkdir -p "$store/daily" "$store/snapshots"
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' EXIT
    if [ -n "$fixture_dir" ]; then
        for f in clones views releases; do
            [ -f "$fixture_dir/$f.json" ] && cp "$fixture_dir/$f.json" "$tmp/$f.json" || : > "$tmp/$f.json"
        done
    else
        command -v gh >/dev/null 2>&1 || die "gh is required when --fixture-dir is not given"
        # A token without push access on the repo cannot read /traffic/*; that failure is loud here
        # rather than silently producing an empty series.
        gh api "repos/$repo/traffic/clones" > "$tmp/clones.json" || die "cannot read /traffic/clones -- the token needs push access on $repo"
        gh api "repos/$repo/traffic/views"  > "$tmp/views.json"  || die "cannot read /traffic/views -- the token needs push access on $repo"
        gh api "repos/$repo/releases"       > "$tmp/releases.json" || die "cannot read /releases on $repo"
    fi
    OBSERVED_AT="$observed_at" STORE="$store" SRC="$tmp" python3 - <<'PY'
import json, os, sys, datetime

store = os.environ["STORE"]
src = os.environ["SRC"]
observed_at = os.environ.get("OBSERVED_AT") or datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def load(name):
    p = os.path.join(src, name + ".json")
    if not os.path.exists(p) or os.path.getsize(p) == 0:
        return None
    with open(p) as fh:
        return json.load(fh)

def upsert_daily(metric, rows):
    """Keyed by date. The API re-sends the same trailing days on every call, so this must replace a
    row rather than append one; appending is what double-counts an overlapping window."""
    path = os.path.join(store, "daily", metric + ".jsonl")
    existing = {}
    if os.path.exists(path):
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if line:
                    existing[json.loads(line)["date"]] = json.loads(line)
    for row in rows:
        existing[row["date"]] = row
    with open(path, "w") as fh:
        for date in sorted(existing):
            fh.write(json.dumps(existing[date], separators=(",", ":")) + "\n")

def append_snapshot(metric, value):
    """A running total. Stored with the moment it was read, so a period's figure is a difference."""
    path = os.path.join(store, "snapshots", metric + ".jsonl")
    with open(path, "a") as fh:
        fh.write(json.dumps({"observed_at": observed_at, "value": value}, separators=(",", ":")) + "\n")

for name, key in (("clones", "clones"), ("views", "views")):
    payload = load(name)
    if not payload:
        continue
    rows = [
        {"date": d["timestamp"][:10], "count": d["count"], "uniques": d["uniques"]}
        for d in payload.get(key, [])
    ]
    if rows:
        upsert_daily(name, rows)

releases = load("releases")
if releases:
    total = sum(a.get("download_count", 0) for r in releases for a in r.get("assets", []))
    append_snapshot("release_asset_downloads", total)
PY
    printf 'funnel-capture: captured into %s\n' "$store"
    exit 0
fi

if [ "$mode" = "delta" ]; then
    STORE="$store" METRIC="$metric" python3 - <<'PY'
import json, os
path = os.path.join(os.environ["STORE"], "snapshots", os.environ["METRIC"] + ".jsonl")
if not os.path.exists(path):
    raise SystemExit("funnel-capture: no snapshots for " + os.environ["METRIC"])
rows = [json.loads(l) for l in open(path) if l.strip()]
if len(rows) < 2:
    # One snapshot is not a period. Saying so beats reporting the running total as if it were one.
    raise SystemExit("funnel-capture: need two snapshots to compute a delta, have %d" % len(rows))
print(rows[-1]["value"] - rows[-2]["value"])
PY
    exit 0
fi

if [ "$mode" = "gaps" ]; then
    STORE="$store" python3 - <<'PY'
import json, os, datetime
daily = os.path.join(os.environ["STORE"], "daily")
found = False
for name in sorted(os.listdir(daily)) if os.path.isdir(daily) else []:
    if not name.endswith(".jsonl"):
        continue
    rows = [json.loads(l) for l in open(os.path.join(daily, name)) if l.strip()]
    if not rows:
        continue
    dates = sorted(datetime.date.fromisoformat(r["date"]) for r in rows)
    missing = []
    cur = dates[0]
    while cur <= dates[-1]:
        if cur not in dates:
            missing.append(cur.isoformat())
        cur += datetime.timedelta(days=1)
    metric = name[:-len(".jsonl")]
    if missing:
        found = True
        # Named, never zero-filled: a hole the reader can see beats a total that silently understates.
        print("gap %s: %s" % (metric, " ".join(missing)))
    else:
        print("gap %s: none (%s..%s)" % (metric, dates[0], dates[-1]))
raise SystemExit(0 if True else 1)
PY
    exit 0
fi
