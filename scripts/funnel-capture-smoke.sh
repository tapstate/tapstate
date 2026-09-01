#!/bin/sh
#
# Smoke for funnel-capture.sh -- the weekly job that keeps the public distribution counters.
#
# What it pins, and why each case exists rather than being a restatement of the code:
#   - overlapping windows are idempotent. The traffic API always returns the same trailing 14 days, so
#     a job that appends rather than upserts double-counts every day it has already seen. That defect
#     is invisible on a single run and only a second run over the same window exposes it.
#   - cumulative counters are not daily ones. Release asset download_count is a running total, not
#     "downloads that day". Feeding 100 then 130 must report 30 for the week; an implementation that
#     treats every counter as a daily observation reports 130 and looks entirely plausible.
#   - a gap is reported, never zero-filled. A series with three missing days must say so: zero-filling
#     produces a good-looking and wrong number, which is worse than an obvious hole.
#
# Fixtures are canned API payloads, so this runs with no network and no credentials.
set -eu

here="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
capture="$here/funnel-capture.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

fail() { printf 'funnel-capture-smoke: FAIL: %s\n' "$1" >&2; exit 1; }
pass() { printf 'funnel-capture-smoke: ok: %s\n' "$1"; }

[ -x "$capture" ] || fail "funnel-capture.sh is missing or not executable at $capture"

# A 14-day daily window, the shape /traffic/clones returns.
window() {
    # $1 = first day offset label, emits days 01..14 of 2026-08 with a fixed count each
    cat <<JSON
{"count":140,"uniques":14,"clones":[
{"timestamp":"2026-08-18T00:00:00Z","count":10,"uniques":1},
{"timestamp":"2026-08-19T00:00:00Z","count":10,"uniques":1},
{"timestamp":"2026-08-20T00:00:00Z","count":10,"uniques":1}
]}
JSON
}

# --- case 1: overlapping windows are idempotent -------------------------------------------------
store="$work/store1"; fx="$work/fx1"; mkdir -p "$fx"
window > "$fx/clones.json"
: > "$fx/releases.json"
sh "$capture" --store "$store" --fixture-dir "$fx" >/dev/null
sh "$capture" --store "$store" --fixture-dir "$fx" >/dev/null
rows="$(wc -l < "$store/daily/clones.jsonl" | tr -d ' ')"
[ "$rows" = "3" ] || fail "case 1: expected 3 daily rows after two runs of the same window, got $rows"
total="$(awk -F'"count":' '{split($2,a,",|}"); s+=a[1]} END{print s+0}' "$store/daily/clones.jsonl")"
[ "$total" = "30" ] || fail "case 1: expected total 30 after two runs (not doubled), got $total"
pass "case 1: overlapping window is idempotent"

# --- case 2: cumulative snapshots yield a delta, not the running total --------------------------
store="$work/store2"; fx="$work/fx2"; mkdir -p "$fx"
window > "$fx/clones.json"
printf '[{"assets":[{"name":"cli","download_count":100}]}]\n' > "$fx/releases.json"
sh "$capture" --store "$store" --fixture-dir "$fx" --observed-at 2026-08-25T00:00:00Z >/dev/null
printf '[{"assets":[{"name":"cli","download_count":130}]}]\n' > "$fx/releases.json"
sh "$capture" --store "$store" --fixture-dir "$fx" --observed-at 2026-09-01T00:00:00Z >/dev/null
delta="$(sh "$capture" --store "$store" --delta release_asset_downloads)"
[ "$delta" = "30" ] || fail "case 2: expected weekly delta 30 from snapshots 100 -> 130, got '$delta'"
pass "case 2: cumulative counter reports a delta, not the running total"

# --- case 3: a gap is reported, not zero-filled --------------------------------------------------
store="$work/store3"; fx="$work/fx3"; mkdir -p "$fx"
cat > "$fx/clones.json" <<'JSON'
{"count":20,"uniques":2,"clones":[
{"timestamp":"2026-08-18T00:00:00Z","count":10,"uniques":1},
{"timestamp":"2026-08-22T00:00:00Z","count":10,"uniques":1}
]}
JSON
: > "$fx/releases.json"
sh "$capture" --store "$store" --fixture-dir "$fx" >/dev/null
gaps="$(sh "$capture" --store "$store" --gaps)"
echo "$gaps" | grep -q "2026-08-19" || fail "case 3: expected the gap report to name 2026-08-19, got '$gaps'"
echo "$gaps" | grep -q "2026-08-21" || fail "case 3: expected the gap report to name 2026-08-21, got '$gaps'"
grep -q '"date":"2026-08-20"' "$store/daily/clones.jsonl" && fail "case 3: missing days were zero-filled into the series"
pass "case 3: gap is named, not zero-filled"

# --- the weekly report -----------------------------------------------------------------------------
report="$here/funnel-report.sh"
[ -x "$report" ] || fail "funnel-report.sh is missing or not executable at $report"

store="$work/store4"; fx="$work/fx4"; mkdir -p "$fx" "$store/events"
window > "$fx/clones.json"
printf '[{"assets":[{"name":"cli","download_count":100}]}]\n' > "$fx/releases.json"
sh "$capture" --store "$store" --fixture-dir "$fx" --observed-at 2026-08-18T00:00:00Z >/dev/null
printf '[{"assets":[{"name":"cli","download_count":130}]}]\n' > "$fx/releases.json"
sh "$capture" --store "$store" --fixture-dir "$fx" --observed-at 2026-08-20T00:00:00Z >/dev/null

# Four events, three installations: one id repeats, which is what a reinstall in place looks like.
cat > "$store/events/installs.jsonl" <<'JSON'
{"installation_id":"aaa","version":"0.3.0","os":"darwin","arch":"arm64","entrypoint":"cli","timestamp":"2026-08-18T01:00:00Z"}
{"installation_id":"aaa","version":"0.3.0","os":"darwin","arch":"arm64","entrypoint":"cli","timestamp":"2026-08-19T01:00:00Z"}
{"installation_id":"bbb","version":"0.3.0","os":"linux","arch":"x64","entrypoint":"quickstart","timestamp":"2026-08-19T02:00:00Z"}
{"installation_id":"ccc","version":"0.2.9","os":"linux","arch":"x64","entrypoint":"cli","timestamp":"2026-08-20T02:00:00Z"}
JSON
out="$(sh "$report" --store "$store" --week 2026-08-18)"

# L1 counts installations, not events. An implementation counting rows reports 4 here, and on real
# data -- where reinstalls are common -- that inflates the denominator silently.
echo "$out" | grep -qE 'installs completed.*: *3' \
  || fail "case 4: expected L1 = 3 unique installations, report said: $(echo "$out" | grep -i 'installs completed')"
echo "$out" | grep -qE '0\.3\.0 +2' || fail "case 4: expected 0.3.0 -> 2 in the per-version split"
echo "$out" | grep -qE '0\.2\.9 +1' || fail "case 4: expected 0.2.9 -> 1 in the per-version split"
pass "case 4: L1 counts unique installations and splits by version"

# The snapshot pair must surface as its difference. 130 appearing as the week's figure is the defect.
echo "$out" | grep -qE 'release asset downloads +30\b' \
  || fail "case 5: expected the release delta 30, report said: $(echo "$out" | grep -i 'release asset')"
echo "$out" | grep -qE 'release asset downloads +130\b' \
  && fail "case 5: the report presented the running total as the week's figure"
pass "case 5: cumulative counter appears as a delta, not the running total"

# Distribution signals stay separate. A single summed figure is the specific thing that must not
# exist, so the report is checked for the sum it would print if someone added them up.
# `grep -c` exits 1 on zero matches, which under set -e would kill the run before the remaining
# cases ever execute -- silently, and looking exactly like a pass.
sum_line="$(echo "$out" | grep -icE 'total distribution|distribution total|all signals' || true)"
[ "$sum_line" = "0" ] || fail "case 6: the report printed a summed distribution figure"
echo "$out" | grep -qi 'never summed' || fail "case 6: the report does not warn that signals are not summed"
echo "$out" | grep -qi 'community' || fail "case 6: the report does not say it covers the community funnel only"
pass "case 6: distribution signals are listed separately, with the warning kept"

# A hole is named. Zero-filling produces a good-looking, wrong number.
store="$work/store5"; fx="$work/fx5"; mkdir -p "$fx" "$store/events"
cat > "$fx/clones.json" <<'JSON'
{"count":20,"uniques":2,"clones":[
{"timestamp":"2026-08-18T00:00:00Z","count":10,"uniques":1},
{"timestamp":"2026-08-20T00:00:00Z","count":10,"uniques":1}
]}
JSON
: > "$fx/releases.json"
sh "$capture" --store "$store" --fixture-dir "$fx" >/dev/null
out2="$(sh "$report" --store "$store" --week 2026-08-18)"
echo "$out2" | grep -q "2026-08-19" || fail "case 7: the report did not name the missing day"
echo "$out2" | grep -qi 'gaps: *none' && fail "case 7: the report claimed no gaps while a day was missing"
pass "case 7: the report names the gap rather than filling it"

printf 'funnel-capture-smoke: all cases passed\n'
