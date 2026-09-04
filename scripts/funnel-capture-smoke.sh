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

# --- publishing the series into a git repository ----------------------------------------------------
publish="$here/funnel-publish.sh"
[ -x "$publish" ] || fail "funnel-publish.sh is missing or not executable at $publish"

# a bare origin plus a working clone, so push is exercised rather than mocked
origin="$work/origin.git"; git init -q --bare "$origin"
clone="$work/clone"; git clone -q "$origin" "$clone"
git -C "$clone" -c user.email=t@example.com -c user.name=t commit -q --allow-empty -m init
git -C "$clone" push -q origin HEAD:main
base_commits="$(git -C "$clone" rev-list --count HEAD)"

store="$clone/funnel"; fx="$work/fx6"; mkdir -p "$fx"
window > "$fx/clones.json"
: > "$fx/releases.json"
sh "$capture" --store "$store" --fixture-dir "$fx" >/dev/null
sh "$publish" --repo-dir "$clone" >/dev/null
after_first="$(git -C "$clone" rev-list --count HEAD)"
[ "$after_first" -gt "$base_commits" ] || fail "case 8: publishing new data added no commit"
pass "case 8: new data lands as a commit"

# Re-running with nothing new must not manufacture a commit. The job runs weekly forever, so an
# unconditional commit turns the history into one empty commit per week and buries the real ones --
# and `git commit` with nothing staged also exits non-zero, which would fail the job every quiet week.
sh "$capture" --store "$store" --fixture-dir "$fx" >/dev/null
# The exit code is half the assertion. Dropping the no-change guard makes `git commit` fail on an
# empty index, which also produces no commit -- so a count-only check passes while every quiet week
# turns into a failed job. Both halves have to be asserted for this case to mean anything.
if sh "$publish" --repo-dir "$clone" >/dev/null 2>&1; then pub_rc=0; else pub_rc=$?; fi
after_second="$(git -C "$clone" rev-list --count HEAD)"
[ "$pub_rc" = "0" ] || fail "case 9: a quiet run exited $pub_rc; every week with no new data would fail the job"
[ "$after_second" = "$after_first" ] || fail "case 9: an unchanged run still produced a commit"
pass "case 9: an unchanged run commits nothing and still succeeds"

# and the push actually reached the origin, rather than only committing locally.
# Reads `main:`, the branch funnel-publish.sh pushes -- NOT `HEAD:`. A bare repository's HEAD is
# whatever init.defaultBranch says, which is `master` unless something set it, and only `main` was
# ever created here. Reading HEAD therefore resolved a branch that does not exist, so this case was
# red on any machine that had not been configured otherwise -- and, worse, it was red with and
# without the push, which makes its mutation evidence vacuous: a case that fails identically whether
# the code works is not testing the code.
git -C "$origin" show main:funnel/daily/clones.jsonl >/dev/null 2>&1 \
  || fail "case 10: the series is not present on the origin -- it was committed but never pushed"
pass "case 10: the series reaches the origin"


# --- the receiver's events become the denominator ---------------------------------------------------
# The receiver writes one file per event because a unique path is the only write that cannot lose a
# concurrent one. Nothing reads that shape: the report counts a JSONL. Until this step existed the
# events piled up unread and the report said "L1 is unavailable" every week, forever, while installs
# were arriving -- the exact opposite of what was happening, and the denominator this line exists to
# produce never came into being at all.
store="$work/store7"; fx="$work/fx7"; mkdir -p "$fx" "$store/events"
: > "$fx/clones.json"; : > "$fx/releases.json"
cat > "$store/events/2026-08-18T01-00-00Z-aaaa1111.json" <<'JSON'
{"installation_id":"e1","version":"0.4.1","os":"darwin","arch":"arm64","entrypoint":"cli","country":"SG","timestamp":"2026-08-18T01:00:00Z"}
JSON
cat > "$store/events/2026-08-19T02-00-00Z-bbbb2222.json" <<'JSON'
{"installation_id":"e2","version":"0.4.1","os":"linux","arch":"x64","entrypoint":"quickstart","country":"ZZ","timestamp":"2026-08-19T02:00:00Z"}
JSON
sh "$capture" --store "$store" --fixture-dir "$fx" >/dev/null
[ -f "$store/events/installs.jsonl" ] \
  || fail "case 11: the events were never folded -- no installs.jsonl exists"
folded="$(wc -l < "$store/events/installs.jsonl" | tr -d ' ')"
[ "$folded" = "2" ] || fail "case 11: expected 2 folded events, got $folded"
left="$(find "$store/events" -name '*.json' ! -name 'installs.jsonl' | wc -l | tr -d ' ')"
[ "$left" = "0" ] || fail "case 11: $left event file(s) were folded but not consumed, so the next run refolds them"
pass "case 11: the receiver's per-event files are folded into the series the report counts"

# Idempotent, because the job runs forever and a re-run must not double the denominator. Re-adding an
# already-folded event must change nothing: the key is (installation_id, timestamp), not the filename.
cat > "$store/events/2026-08-18T01-00-00Z-cccc3333.json" <<'JSON'
{"installation_id":"e1","version":"0.4.1","os":"darwin","arch":"arm64","entrypoint":"cli","country":"SG","timestamp":"2026-08-18T01:00:00Z"}
JSON
sh "$capture" --store "$store" --fixture-dir "$fx" >/dev/null
folded2="$(wc -l < "$store/events/installs.jsonl" | tr -d ' ')"
[ "$folded2" = "2" ] || fail "case 12: refolding the same event changed the count to $folded2 -- L1 would inflate on every re-run"
pass "case 12: refolding an already-folded event does not inflate the denominator"

# A file that is not an event is left exactly where it is. Deleting it would destroy the only copy of
# whatever it actually is, and this directory is the one place a receiver write lands.
printf 'not json at all\n' > "$store/events/broken.json"
sh "$capture" --store "$store" --fixture-dir "$fx" >/dev/null
[ -f "$store/events/broken.json" ] || fail "case 13: an unparseable file was deleted rather than left alone"
pass "case 13: a file that does not parse as an event is kept, not silently dropped"

# The report must not be blind in the window between a receiver write and the next capture run, and
# an absent event store must still say "unavailable" rather than zero -- they are opposite findings.
store="$work/store8"; mkdir -p "$store/events" "$store/daily"
cat > "$store/events/2026-08-18T03-00-00Z-dddd4444.json" <<'JSON'
{"installation_id":"e9","version":"0.4.1","os":"linux","arch":"x64","entrypoint":"cli","country":"ZZ","timestamp":"2026-08-18T03:00:00Z"}
JSON
out3="$(sh "$report" --store "$store" --week 2026-08-18)"
echo "$out3" | grep -qE 'installs completed: *1' \
  || fail "case 14: an event not yet folded was invisible to the report: $(echo "$out3" | grep -i 'L1\|installs completed')"
pass "case 14: an event that has arrived but not been folded still counts"

store="$work/store9"; mkdir -p "$store/daily"
out4="$(sh "$report" --store "$store" --week 2026-08-18)"
echo "$out4" | grep -qi 'L1 is unavailable' \
  || fail "case 15: with no event store at all the report did not say L1 is unavailable"
echo "$out4" | grep -qE 'installs completed: *0' \
  && fail "case 15: the report printed L1 as zero when it is actually unavailable"
pass "case 15: no event store reports L1 unavailable, never zero"

# --- release totals span every page ----------------------------------------------------------------
# /releases serves 30 per page. Unpaginated, the "cumulative" total is the newest 30 releases, so
# publishing the 31st drops the oldest out and the next period's difference comes out NEGATIVE with
# nothing to say the series stopped being cumulative. `--paginate --slurp` returns an array of pages,
# so the reader has to flatten -- and a reader that does not flatten sums zero assets and reports 0.
store="$work/store10"; fx="$work/fx10"; mkdir -p "$fx"
: > "$fx/clones.json"
printf '[[{"assets":[{"download_count":100}]},{"assets":[{"download_count":5}]}],[{"assets":[{"download_count":7}]}]]\n' > "$fx/releases.json"
sh "$capture" --store "$store" --fixture-dir "$fx" --observed-at 2026-08-18T04:17:00Z >/dev/null
snap="$(sed -n '1p' "$store/snapshots/release_asset_downloads.jsonl")"
echo "$snap" | grep -q '"value":112' \
  || fail "case 16: expected 112 summed across both pages, got: $snap"
pass "case 16: a paginated releases payload is flattened before summing"

# --- a period's figure is bounded by that period ---------------------------------------------------
# `rows[-1] - rows[-2]` is "the two newest readings", not "the readings bounding the week asked for".
# Measured on the old code: --week 2026-08-18 printed 370, a figure for 08-25..09-01, under the
# 08-18 header -- while the daily metrics on the same page were correctly week-filtered.
store="$work/store11"; mkdir -p "$store/snapshots" "$store/daily" "$store/events"
cat > "$store/snapshots/release_asset_downloads.jsonl" <<'JSON'
{"observed_at":"2026-08-18T04:17:00Z","value":100}
{"observed_at":"2026-08-25T04:17:00Z","value":130}
{"observed_at":"2026-09-01T04:17:00Z","value":500}
JSON
out5="$(sh "$report" --store "$store" --week 2026-08-18)"
echo "$out5" | grep -qE 'release asset downloads +30\b' \
  || fail "case 17: the week of 08-18 did not report its own 30: $(echo "$out5" | grep -i 'release asset')"
echo "$out5" | grep -qE 'release asset downloads +370\b' \
  && fail "case 17: the report printed a later period's figure under the requested week's header"
out6="$(sh "$report" --store "$store" --week 2026-08-25)"
echo "$out6" | grep -qE 'release asset downloads +370\b' \
  || fail "case 17: the week of 08-25 did not report its own 370: $(echo "$out6" | grep -i 'release asset')"
pass "case 17: the snapshot delta is bounded by the requested week, not by which readings are newest"

# workflow_dispatch is enabled, so a manual run or a re-run after a failed step is ordinary. A second
# reading inside one week must not report that week's real growth as ~0.
printf '{"observed_at":"2026-09-01T09:00:00Z","value":500}\n' >> "$store/snapshots/release_asset_downloads.jsonl"
out7="$(sh "$report" --store "$store" --week 2026-08-25)"
echo "$out7" | grep -qE 'release asset downloads +370\b' \
  || fail "case 18: a second reading in one week collapsed it: $(echo "$out7" | grep -i 'release asset')"
pass "case 18: a re-run inside one week does not zero that week"

# A week with no closing reading is n/a, not the running total and not zero.
out8="$(sh "$report" --store "$store" --week 2026-09-01)"
echo "$out8" | grep -qE 'release asset downloads +n/a' \
  || fail "case 19: a week with no closing reading did not report n/a: $(echo "$out8" | grep -i 'release asset')"
pass "case 19: a week with no reading on its far side reports n/a"

# --- coverage tells the truth when nothing was observed --------------------------------------------
# Clamping the gap window to the observed range meant that when the week and the range do not overlap
# the window is empty, so no day can be called missing and the report answered "gaps: none" for a week
# in which nothing was observed at all -- the precise "plausible and badly wrong" answer this section
# exists to prevent. Measured: a series ending 2026-08-02, asked for --week 2026-09-01.
store="$work/store12"; fx="$work/fx12"; mkdir -p "$fx" "$store/events"
cat > "$fx/clones.json" <<'JSON'
{"count":10,"uniques":1,"clones":[
{"timestamp":"2026-08-01T00:00:00Z","count":5,"uniques":1},
{"timestamp":"2026-08-02T00:00:00Z","count":5,"uniques":1}
]}
JSON
: > "$fx/releases.json"
sh "$capture" --store "$store" --fixture-dir "$fx" >/dev/null
out9="$(sh "$report" --store "$store" --week 2026-09-01)"
echo "$out9" | grep -qi 'gaps: *none' \
  && fail "case 20: a week entirely outside the observed range reported no gaps"
echo "$out9" | grep -qi 'collection stopped after 2026-08-02' \
  || fail "case 20: the report did not say collection had stopped: $out9"
pass "case 20: a week wholly outside the observed range says so instead of 'gaps: none'"

# An expected series that nothing has ever written prints n/a. Printing nothing at all makes an absent
# capture source indistinguishable from a quiet week, and those want opposite reactions.
echo "$out9" | grep -qE 'views +n/a' \
  || fail "case 21: a never-captured series printed nothing rather than n/a: $out9"
pass "case 21: an expected series that was never captured prints n/a, not silence"

# --- the one cross-check that catches a dead receiver ----------------------------------------------
# Distribution signals and installs arrive by completely separate paths -- a pull from GitHub's API
# against a push from the installer -- so traffic with no installs at all is either a real collapse
# or, far more often, the callback failing quietly. Neither is visible in any single figure.
store="$work/store13"; fx="$work/fx13"; mkdir -p "$fx"
window > "$fx/clones.json"
: > "$fx/releases.json"
sh "$capture" --store "$store" --fixture-dir "$fx" >/dev/null
out10="$(sh "$report" --store "$store" --week 2026-08-18)"
echo "$out10" | grep -qi 'WARNING: distribution signals are non-zero' \
  || fail "case 22: traffic with no installs at all raised no warning: $out10"
pass "case 22: traffic with no installs raises a warning rather than reading as a real ratio"

printf 'funnel-capture-smoke: all cases passed\n'
