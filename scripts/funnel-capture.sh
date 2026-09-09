#!/bin/sh
#
# Keep the public distribution counters that expire, and fold received install events into the one
# series the report divides by.
#
# GitHub's traffic API serves only the trailing 14 days, so a counter not captured within that window
# is gone permanently. This job appends to a durable series so the numbers survive the window.
#
# Three measurement types, deliberately stored differently, because conflating them is the defect that
# produces a plausible and badly wrong number:
#
#   daily observation   /traffic/clones, /traffic/views. The API returns one row per day for the last
#                       14 days, and re-sends rows it has already sent. Stored keyed by date and
#                       upserted, so re-running over an overlapping window changes nothing.
#   cumulative snapshot release asset download_count. A running total, not "that day's downloads".
#                       Stored as (observed_at, value); a period's figure is the difference between
#                       the snapshot bounding the period's start and the one bounding its end, never
#                       the snapshot itself and never "the two newest".
#   install events      one JSON file per event, written by the receiver because a unique path is the
#                       only write that cannot lose a concurrent one. Folded here into a single JSONL,
#                       which is the shape the denominator is counted from. Without this step the
#                       events accumulate unread and the report says L1 is unavailable forever.
#
# Everything captured here except the install events is a DISTRIBUTION SIGNAL. Those numbers are not
# people and are never summed with each other: one person contributes clones and downloads at once, and
# CI contributes far more than any person. Measured over 2026-08-18..08-31: 6525 clones from 316 unique
# cloners against 750 views from 34 unique visitors, weekends collapsing and Mondays spiking -- the
# shape of automation. The funnel denominator is the installer callback, nothing else here.
#
# Usage:
#   funnel-capture.sh --store <dir> [--fixture-dir <dir>] [--observed-at <iso8601>]
#   funnel-capture.sh --store <dir> --delta <metric> [--week <YYYY-MM-DD>]
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
week=""
repo="${FUNNEL_REPO:-tapstate/tapstate}"

die() { printf 'funnel-capture: %s\n' "$1" >&2; exit 1; }

while [ $# -gt 0 ]; do
    case "$1" in
        --store)       store="${2:?--store needs a directory}"; shift 2 ;;
        --fixture-dir) fixture_dir="${2:?--fixture-dir needs a directory}"; shift 2 ;;
        --observed-at) observed_at="${2:?--observed-at needs a timestamp}"; shift 2 ;;
        --delta)       mode="delta"; metric="${2:?--delta needs a metric}"; shift 2 ;;
        --week)        week="${2:?--week needs a YYYY-MM-DD start date}"; shift 2 ;;
        --gaps)        mode="gaps"; shift ;;
        *)             die "unknown argument: $1" ;;
    esac
done

[ -n "$store" ] || die "--store is required"
command -v python3 >/dev/null 2>&1 || die "python3 is required"

if [ "$mode" = "capture" ]; then
    mkdir -p "$store/daily" "$store/snapshots" "$store/events"
    tmp="$(mktemp -d)"
    trap 'rm -rf "$tmp"' EXIT
    if [ -n "$fixture_dir" ]; then
        for f in clones views releases; do
            # Not `[ -f x ] && cp || :` -- that is SC2015, and it also runs the fallback when the cp
            # itself fails, which is the case where an empty series would be worst.
            if [ -f "$fixture_dir/$f.json" ]; then
                cp "$fixture_dir/$f.json" "$tmp/$f.json"
            else
                : > "$tmp/$f.json"
            fi
        done
    else
        command -v gh >/dev/null 2>&1 || die "gh is required when --fixture-dir is not given"
        # A token without push access on the repo cannot read /traffic/*; that failure is loud here
        # rather than silently producing an empty series.
        gh api "repos/$repo/traffic/clones" > "$tmp/clones.json" || die "cannot read /traffic/clones -- the token needs push access on $repo"
        gh api "repos/$repo/traffic/views"  > "$tmp/views.json"  || die "cannot read /traffic/views -- the token needs push access on $repo"
        # --paginate, because /releases serves 30 per page. Unpaginated, the sum below is over the 30
        # most recent releases rather than all of them, so publishing the 31st drops the oldest out of
        # a total stored as monotonic -- and the next period's difference comes out NEGATIVE, with
        # nothing to say the series stopped being cumulative. --slurp yields an array of pages, which
        # the reader below flattens.
        gh api --paginate --slurp "repos/$repo/releases" > "$tmp/releases.json" || die "cannot read /releases on $repo"
    fi
    OBSERVED_AT="$observed_at" STORE="$store" SRC="$tmp" python3 - <<'PY'
import json, os, datetime

store = os.environ["STORE"]
src = os.environ["SRC"]
observed_at = os.environ.get("OBSERVED_AT") or datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def load(name):
    p = os.path.join(src, name + ".json")
    if not os.path.exists(p) or os.path.getsize(p) == 0:
        return None
    with open(p) as fh:
        return json.load(fh)

def read_jsonl(path):
    if not os.path.exists(path):
        return []
    out = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out

def upsert_daily(metric, rows):
    """Keyed by date. The API re-sends the same trailing days on every call, so this must replace a
    row rather than append one; appending is what double-counts an overlapping window."""
    path = os.path.join(store, "daily", metric + ".jsonl")
    existing = {}
    for row in read_jsonl(path):
        existing[row["date"]] = row
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

def compact_events():
    """Fold the receiver's one-file-per-event writes into the single JSONL the denominator is counted
    from. This step is the whole reason the report has a denominator at all: the receiver cannot append
    to a shared file without losing concurrent writes, and the report cannot count a directory.

    Idempotent, and safe to interrupt. Keyed by (installation_id, timestamp), so re-folding an event
    changes nothing, and only the files actually folded are removed -- a run that dies between the
    write and the unlink repeats harmlessly on the next one. A file that does not parse as an event is
    left exactly where it is: deleting it would destroy the only copy of whatever it really is."""
    ev_dir = os.path.join(store, "events")
    if not os.path.isdir(ev_dir):
        return 0
    loose = sorted(n for n in os.listdir(ev_dir) if n.endswith(".json"))
    if not loose:
        return 0
    path = os.path.join(ev_dir, "installs.jsonl")
    rows, seen = [], set()
    for row in read_jsonl(path):
        key = (row.get("installation_id"), row.get("timestamp"))
        if key not in seen:
            seen.add(key)
            rows.append(row)
    folded, added = [], 0
    for name in loose:
        full = os.path.join(ev_dir, name)
        try:
            with open(full) as fh:
                row = json.load(fh)
        except (ValueError, OSError):
            continue
        if not isinstance(row, dict):
            continue
        folded.append(full)
        key = (row.get("installation_id"), row.get("timestamp"))
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)
        added += 1
    rows.sort(key=lambda r: ((r.get("timestamp") or ""), (r.get("installation_id") or "")))
    with open(path, "w") as fh:
        for row in rows:
            fh.write(json.dumps(row, separators=(",", ":")) + "\n")
    for full in folded:
        os.remove(full)
    return added

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
    # `gh api --paginate --slurp` gives a list of pages; a fixture is a flat list of releases. Accept
    # both rather than making the fixtures carry a shape only the network produces.
    if isinstance(releases[0], list):
        releases = [r for page in releases for r in page]
    total = sum(a.get("download_count", 0) for r in releases for a in r.get("assets", []))
    append_snapshot("release_asset_downloads", total)

n = compact_events()
print("funnel-capture: folded %d new install event(s) into events/installs.jsonl" % n)
PY
    printf 'funnel-capture: captured into %s\n' "$store"
    exit 0
fi

if [ "$mode" = "delta" ]; then
    STORE="$store" METRIC="$metric" WEEK="$week" python3 - <<'PY'
import json, os, datetime
path = os.path.join(os.environ["STORE"], "snapshots", os.environ["METRIC"] + ".jsonl")
if not os.path.exists(path):
    raise SystemExit("funnel-capture: no snapshots for " + os.environ["METRIC"])
rows = sorted((json.loads(l) for l in open(path) if l.strip()), key=lambda r: r["observed_at"])
week = os.environ.get("WEEK") or ""
if not week:
    # No period asked for, so this answers "what changed between the two most recent readings" -- a
    # real question, but NOT any calendar week's figure. Pass --week to get a period.
    if len(rows) < 2:
        raise SystemExit("funnel-capture: need two snapshots to compute a delta, have %d" % len(rows))
    print(rows[-1]["value"] - rows[-2]["value"])
    raise SystemExit(0)
# A period's figure is bounded by the period, never by which readings happen to be newest: the job
# enables workflow_dispatch, so a second reading inside one week is ordinary, and "the two newest"
# then reports that week's real growth as ~0.
start = datetime.date.fromisoformat(week)
# The week closes when the next one opens. The reading taken on the week's first day is its opening
# value; requiring the base to be strictly earlier would reach back an entire extra period, because
# the job runs early on the first day.
closes = (start + datetime.timedelta(days=7)).isoformat()
base = [r for r in rows if r["observed_at"][:10] <= start.isoformat()]
final = [r for r in rows if r["observed_at"][:10] <= closes]
if not base or final[-1] is base[-1]:
    raise SystemExit("funnel-capture: no snapshot pair bounds the week of %s" % start)
print(final[-1]["value"] - base[-1]["value"])
PY
    exit 0
fi

if [ "$mode" = "gaps" ]; then
    STORE="$store" python3 - <<'PY'
import json, os, datetime
daily = os.path.join(os.environ["STORE"], "daily")
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
        # Named, never zero-filled: a hole the reader can see beats a total that silently understates.
        print("gap %s: %s" % (metric, " ".join(missing)))
    else:
        print("gap %s: none (%s..%s)" % (metric, dates[0], dates[-1]))
PY
    exit 0
fi
