#!/bin/sh
#
# The weekly funnel report, computed entirely from the stored series -- no manual scraping, which is
# the point: a number nobody can reproduce is a number nobody can act on.
#
# The report has one denominator and it is stated as such. Canonical L1 is "an install completed",
# counted by unique installation_id, and it is the only figure anything may be divided by. Everything
# under distribution signals is listed item by item and never added up: one person contributes clones
# and downloads at once, and CI contributes far more than any person. Measured on this repo over
# 2026-08-18..08-31: 6525 clones from 316 unique cloners against 750 views from 34 unique visitors.
# A funnel built on that sum would be wrong by two orders of magnitude.
#
# Every figure here distinguishes "not collected" from "zero". They are opposite findings that look
# identical in a total, and a report that prints 0 for both is how a capture job stays broken for a
# month without anyone noticing.
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
# The week closes when the next one opens: a reading dated start+7 is this week's closing value.
closes = (start + datetime.timedelta(days=7)).isoformat()
days = {(start + datetime.timedelta(days=i)).isoformat() for i in range(7)}

EXPECTED_DAILY = ("clones", "views")
EXPECTED_SNAPSHOTS = ("release_asset_downloads",)

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

def read_events():
    """The folded JSONL plus any event files the capture job has not folded yet, deduped by
    (installation_id, timestamp). Reading only the JSONL makes the report blind between a receiver
    write and the next capture run; reading only the loose files makes it blind afterwards. Returns
    None -- not an empty list -- when no event store exists at all, because "nothing has arrived" and
    "nowhere to arrive" are different answers and only one of them is a bug."""
    ev_dir = os.path.join(store, "events")
    jsonl = os.path.join(ev_dir, "installs.jsonl")
    loose = []
    if os.path.isdir(ev_dir):
        loose = sorted(n for n in os.listdir(ev_dir) if n.endswith(".json"))
    if not os.path.exists(jsonl) and not loose:
        return None
    rows, seen = [], set()
    for row in read_jsonl(jsonl):
        key = (row.get("installation_id"), row.get("timestamp"))
        if key not in seen:
            seen.add(key)
            rows.append(row)
    for name in loose:
        try:
            with open(os.path.join(ev_dir, name)) as fh:
                row = json.load(fh)
        except (ValueError, OSError):
            continue
        if not isinstance(row, dict):
            continue
        key = (row.get("installation_id"), row.get("timestamp"))
        if key not in seen:
            seen.add(key)
            rows.append(row)
    return rows

print("tapstate funnel -- week of %s (%s..%s)" % (start, start, end))
print()

# --- canonical L1 -----------------------------------------------------------------------------
# Unique installation_id, not event count: a reinstall in the same place is one installation, and
# counting rows would inflate the one figure everything else is divided by.
all_events = read_events()
print("CANONICAL L1 -- installs completed (unique installation_id)")
l1 = None
if all_events is None:
    print("  no event store yet -- the install endpoint is not receiving. L1 is unavailable,")
    print("  which is different from L1 being zero.")
else:
    events = [e for e in all_events if e.get("timestamp", "")[:10] in days]
    ids = {e.get("installation_id") for e in events if e.get("installation_id")}
    l1 = len(ids)
    print("  installs completed: %d" % l1)
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
distribution_seen = 0
have_daily = {}
for name in (sorted(os.listdir(daily_dir)) if os.path.isdir(daily_dir) else []):
    if name.endswith(".jsonl"):
        have_daily[name[:-len(".jsonl")]] = os.path.join(daily_dir, name)
# Every expected metric prints a line whether or not its series exists. An absent series that prints
# nothing is indistinguishable from a quiet week, and the two want opposite reactions.
for metric in sorted(set(EXPECTED_DAILY) | set(have_daily)):
    if metric not in have_daily:
        print("  %-24s %-8s (no series captured -- nothing has ever written it)" % (metric, "n/a"))
        continue
    rows = [r for r in read_jsonl(have_daily[metric]) if r["date"] in days]
    total = sum(r.get("count", 0) for r in rows)
    uniq = sum(r.get("uniques", 0) for r in rows)
    distribution_seen += total
    print("  %-24s %-8d (%d unique-per-day, %d day(s) observed)" % (metric, total, uniq, len(rows)))

snap_dir = os.path.join(store, "snapshots")
have_snap = {}
for name in (sorted(os.listdir(snap_dir)) if os.path.isdir(snap_dir) else []):
    if name.endswith(".jsonl"):
        have_snap[name[:-len(".jsonl")]] = os.path.join(snap_dir, name)
for metric in sorted(set(EXPECTED_SNAPSHOTS) | set(have_snap)):
    label = metric.replace("_", " ")
    if metric not in have_snap:
        print("  %-24s %-8s (no series captured -- nothing has ever written it)" % (label, "n/a"))
        continue
    rows = sorted(read_jsonl(have_snap[metric]), key=lambda r: r["observed_at"])
    # A running total is never the period's figure, and neither is the difference between whichever
    # two readings happen to be newest. The period is bounded by the period: the last reading before
    # the week opens, and the last one before it closes. Positional selection reports a re-run inside
    # one week as ~0 growth, and reports today's figure under a past week's header -- both measured.
    # The reading taken ON the week's first day is that week's opening value, and the reading taken on
    # the day the next week opens is its closing value -- the job runs early on a Monday, so requiring
    # the base to be strictly earlier than the week would silently reach back a whole extra period.
    base = [r for r in rows if r["observed_at"][:10] <= start.isoformat()]
    final = [r for r in rows if r["observed_at"][:10] <= closes]
    if not base or final[-1] is base[-1]:
        if not rows:
            why = "no snapshots"
        elif not base:
            why = "nothing read on or before %s" % start
        else:
            why = "nothing read since %s, so the week has no closing value" % base[-1]["observed_at"][:10]
        print("  %-24s %-8s (%s -- a period needs a reading on each side of it)" % (label, "n/a", why))
        continue
    delta = final[-1]["value"] - base[-1]["value"]
    distribution_seen += max(delta, 0)
    print("  %-24s %-8d (snapshot delta %d -> %d, %s -> %s)" % (
        label, delta, base[-1]["value"], final[-1]["value"],
        base[-1]["observed_at"][:10], final[-1]["observed_at"][:10]))
print()
print("  One person contributes several of these at once and CI contributes more than any person,")
print("  so they are never summed and none of them is a count of people.")
print()

# --- coverage ---------------------------------------------------------------------------------
print("COVERAGE")
missing_any = False
for metric in sorted(set(EXPECTED_DAILY) | set(have_daily)):
    if metric not in have_daily:
        missing_any = True
        print("  %s: never collected -- no series exists" % metric)
        continue
    have = {r["date"] for r in read_jsonl(have_daily[metric])}
    observed = sorted(have)
    if not observed:
        missing_any = True
        print("  %s: the series exists but holds no observations at all" % metric)
        continue
    # A week before collection started, or after it stopped, is not a hole in the data -- but it is
    # not coverage either. Clamping to the observed range and saying nothing was the defect: when the
    # week lies wholly outside that range the clamp is empty, so no day can be called missing and the
    # report answers "gaps: none" for a week in which nothing was observed at all.
    if observed[-1] < start.isoformat():
        missing_any = True
        print("  %s: collection stopped after %s -- the whole requested week is unobserved"
              % (metric, observed[-1]))
        continue
    if observed[0] > end.isoformat():
        missing_any = True
        print("  %s: collection started %s -- the whole requested week predates it"
              % (metric, observed[0]))
        continue
    lo, hi = max(observed[0], start.isoformat()), min(observed[-1], end.isoformat())
    if lo > start.isoformat() or hi < end.isoformat():
        missing_any = True
        print("  %s: only %s..%s of the requested week is inside the observed range (%s..%s)"
              % (metric, lo, hi, observed[0], observed[-1]))
    gap = sorted(d for d in days if lo <= d <= hi and d not in have)
    if gap:
        missing_any = True
        print("  gap in %s: %s" % (metric, " ".join(gap)))
if not missing_any:
    print("  gaps: none")

# The one cross-check that catches a silently dead receiver. Distribution signals and installs come
# through completely separate paths -- a pull from GitHub's API versus a push from the installer -- so
# traffic with no installs at all is either a real collapse or, far more often, the callback failing
# quietly. Neither is visible in any single figure above.
if distribution_seen > 0 and (l1 is None or l1 == 0):
    print()
    print("  WARNING: distribution signals are non-zero for this week while L1 is %s."
          % ("unavailable" if l1 is None else "zero"))
    print("  Either nobody who fetched the software ran the installer, or the install callback is")
    print("  not arriving. Check the receiver before reading any ratio off this page.")
print("  This covers the community funnel only. Offline and restricted-network deployments never")
print("  appear here, and these numbers must not be added to any enterprise figure.")
PY
