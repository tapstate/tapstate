#!/bin/sh
#
# The weekly funnel report, computed entirely from the stored series -- no manual scraping, which is
# the point: a number nobody can reproduce is a number nobody can act on.
#
# The report has one denominator and it is stated as such. Canonical L1 is "an install completed",
# counted by unique installation_id, and it is the only figure anything may be divided by. Everything
# under distribution signals is listed item by item and never added up: one person contributes clones,
# downloads and pulls at once, and CI contributes far more than any person. Measured on this repo over
# 2026-08-18..08-31: 6525 clones from 316 unique cloners against 750 views from 34 unique visitors.
# A funnel built on that sum would be wrong by two orders of magnitude.
#
# Usage: funnel-report.sh --store <dir> --week <YYYY-MM-DD>   (the week starting that day, 7 days)
set -eu

store=""
week=""
die() { printf 'funnel-report: %s\n' "$1" >&2; exit 1; }

while [ $# -gt 0 ]; do
    case "$1" in
        --store) store="${2:?--store needs a directory}"; shift 2 ;;
        --week)  week="${2:?--week needs a YYYY-MM-DD start date}"; shift 2 ;;
        *) die "unknown argument: $1" ;;
    esac
done
[ -n "$store" ] || die "--store is required"
[ -n "$week" ] || die "--week is required"
command -v python3 >/dev/null 2>&1 || die "python3 is required"

STORE="$store" WEEK="$week" python3 - <<'PY'
import json, os, datetime

store = os.environ["STORE"]
start = datetime.date.fromisoformat(os.environ["WEEK"])
end = start + datetime.timedelta(days=6)
days = {(start + datetime.timedelta(days=i)).isoformat() for i in range(7)}

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

print("tapstate funnel -- week of %s (%s..%s)" % (start, start, end))
print()

# --- canonical L1 -----------------------------------------------------------------------------
# Unique installation_id, not event count: a reinstall in the same place is one installation, and
# counting rows would inflate the one figure everything else is divided by.
events = [e for e in read_jsonl(os.path.join(store, "events", "installs.jsonl"))
          if e.get("timestamp", "")[:10] in days]
ids = {e.get("installation_id") for e in events if e.get("installation_id")}
print("CANONICAL L1 -- installs completed (unique installation_id)")
if not os.path.exists(os.path.join(store, "events", "installs.jsonl")):
    print("  no event store yet -- the install endpoint is not receiving. L1 is unavailable,")
    print("  which is different from L1 being zero.")
else:
    print("  installs completed: %d" % len(ids))
    by_version = {}
    for e in events:
        by_version.setdefault(e.get("version", "unknown"), set()).add(e.get("installation_id"))
    if by_version:
        print("  by version:")
        for v in sorted(by_version, reverse=True):
            print("    %-10s %d" % (v, len(by_version[v])))
    by_entry = {}
    for e in events:
        by_entry.setdefault(e.get("entrypoint", "unknown"), set()).add(e.get("installation_id"))
    if by_entry:
        print("  by entry point: " + ", ".join(
            "%s %d" % (k, len(v)) for k, v in sorted(by_entry.items())))
print()

# --- distribution signals ---------------------------------------------------------------------
print("DISTRIBUTION SIGNALS -- listed one by one, never summed, and none of them a user count")
daily_dir = os.path.join(store, "daily")
if os.path.isdir(daily_dir):
    for name in sorted(os.listdir(daily_dir)):
        if not name.endswith(".jsonl"):
            continue
        rows = [r for r in read_jsonl(os.path.join(daily_dir, name)) if r["date"] in days]
        metric = name[:-len(".jsonl")]
        total = sum(r.get("count", 0) for r in rows)
        uniq = sum(r.get("uniques", 0) for r in rows)
        print("  %-24s %-8d (%d unique-per-day, %d day(s) observed)" % (metric, total, uniq, len(rows)))

snap_dir = os.path.join(store, "snapshots")
if os.path.isdir(snap_dir):
    for name in sorted(os.listdir(snap_dir)):
        if not name.endswith(".jsonl"):
            continue
        rows = read_jsonl(os.path.join(snap_dir, name))
        metric = name[:-len(".jsonl")].replace("_", " ")
        if len(rows) < 2:
            print("  %-24s %-8s (one snapshot only -- a period needs two)" % (metric, "n/a"))
            continue
        # A running total is never the period's figure. Printing 130 where 30 belongs is the defect
        # this line exists to avoid, and it looks entirely plausible on its own.
        delta = rows[-1]["value"] - rows[-2]["value"]
        print("  %-24s %-8d (snapshot delta %d -> %d)" % (metric, delta, rows[-2]["value"], rows[-1]["value"]))
print()
print("  One person contributes several of these at once and CI contributes more than any person,")
print("  so they are never summed and none of them is a count of people.")
print()

# --- coverage ---------------------------------------------------------------------------------
print("COVERAGE")
missing_any = False
if os.path.isdir(daily_dir):
    for name in sorted(os.listdir(daily_dir)):
        if not name.endswith(".jsonl"):
            continue
        have = {r["date"] for r in read_jsonl(os.path.join(daily_dir, name))}
        observed = sorted(have)
        if not observed:
            continue
        # Only days inside both the requested week and the range actually observed can be called
        # missing; a week before collection started is not a hole in the data.
        lo, hi = max(observed[0], start.isoformat()), min(observed[-1], end.isoformat())
        gap = sorted(d for d in days if lo <= d <= hi and d not in have)
        if gap:
            missing_any = True
            print("  gap in %s: %s" % (name[:-len('.jsonl')], " ".join(gap)))
if not missing_any:
    print("  gaps: none")
print("  This covers the community funnel only. Offline and restricted-network deployments never")
print("  appear here, and these numbers must not be added to any enterprise figure.")
PY
