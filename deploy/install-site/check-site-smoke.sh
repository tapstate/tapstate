#!/bin/sh
#
# Smoke for check-site.sh. Each case builds a site directory by hand and asks whether the checker
# refuses it -- the point being that the checker's whole job is to refuse, so a checker that never
# refuses passes an "it ran" test while catching nothing.
#
# Case 2 is the incident this exists for, reproduced exactly: the receiver from one source, the
# routing from another, deployed together, /e answering 404 with the function working fine behind it.
set -eu

here="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"
check="$here/check-site.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

fail() { printf 'check-site-smoke: FAIL: %s\n' "$1" >&2; exit 1; }
pass() { printf 'check-site-smoke: ok: %s\n' "$1"; }

[ -x "$check" ] || fail "check-site.sh is missing or not executable at $check"

site() {   # $1 dir, $2 vercel.json body; creates the two static entry points
    mkdir -p "$1/api"
    printf '#!/bin/sh\n' > "$1/quickstart.sh"
    printf '#!/bin/sh\n' > "$1/install.sh"
    printf '%s\n' "$2" > "$1/vercel.json"
}

WITH_E='{"rewrites":[{"source":"/","destination":"/quickstart.sh"},{"source":"/cli","destination":"/install.sh"},{"source":"/e","destination":"/api/event"}]}'
NO_E='{"rewrites":[{"source":"/","destination":"/quickstart.sh"},{"source":"/cli","destination":"/install.sh"}]}'

# --- case 1: a consistent site is accepted ---------------------------------------------------------
d="$work/ok"; site "$d" "$WITH_E"; printf 'export default () => {};\n' > "$d/api/event.js"
sh "$check" "$d" >/dev/null 2>&1 || fail "case 1: a site whose function and route agree was refused"
pass "case 1: a consistent site is accepted"

# --- case 2: the incident -- a function nothing routes to ------------------------------------------
# The release path takes the receiver from whichever of the tag or the checkout has it, and the
# routing separately. A tag that carries a vercel.json without the rewrite does NOT trigger the
# receiver's fallback, because that fallback tests whether the file exists, not whether it mentions
# the route. Result: a working function no client can reach.
d="$work/orphan"; site "$d" "$NO_E"; printf 'export default () => {};\n' > "$d/api/event.js"
if sh "$check" "$d" >/dev/null 2>&1; then
    fail "case 2: a deployed function with no rewrite pointing at it was accepted -- this is the shape that shipped"
fi
sh "$check" "$d" 2>&1 | grep -q 'no rewrite points at /api/event' \
    || fail "case 2: the refusal did not name the unreachable function"
pass "case 2: a function nothing routes to is refused, by name"

# --- case 3: the other direction -- a route with no function ---------------------------------------
d="$work/dangling"; site "$d" "$WITH_E"
if sh "$check" "$d" >/dev/null 2>&1; then
    fail "case 3: a rewrite pointing at a function that is not in the site was accepted"
fi
sh "$check" "$d" 2>&1 | grep -q 'that path would 404' \
    || fail "case 3: the refusal did not say the promised path would 404"
pass "case 3: a route with nothing behind it is refused"

# --- case 4: a site with no functions at all is fine -----------------------------------------------
# The site predates the receiver for every tag older than it, and those must still publish.
d="$work/static"; site "$d" "$NO_E"
sh "$check" "$d" >/dev/null 2>&1 || fail "case 4: a purely static site was refused"
pass "case 4: a site with no functions is accepted"

# --- case 5: unparseable routing is refused, not skipped -------------------------------------------
# Deliberately WITHOUT a function. With one present, a checker that swallowed the parse error and
# carried on with an empty config would still refuse -- via the orphan check -- and this case would
# pass while testing nothing, which is what it did on first writing. No function means the parse is
# the only thing left that can refuse it.
d="$work/broken"; site "$d" 'not json'; rm -rf "$d/api"
if sh "$check" "$d" >/dev/null 2>&1; then
    fail "case 5: a vercel.json that is not JSON was accepted"
fi
sh "$check" "$d" 2>&1 | grep -q 'not valid JSON' \
    || fail "case 5: the refusal did not name the parse failure"
pass "case 5: unparseable routing is refused rather than read as empty"

# --- case 6: a missing vercel.json is refused ------------------------------------------------------
d="$work/noconfig"; mkdir -p "$d/api"; printf 'export default () => {};\n' > "$d/api/event.js"
if sh "$check" "$d" >/dev/null 2>&1; then
    fail "case 6: a site with no vercel.json at all was accepted"
fi
pass "case 6: a site with no routing at all is refused"

# --- case 7: the real checked-in site passes -------------------------------------------------------
# Not a synthetic fixture: what assemble.sh actually produces has to satisfy this, or the gate is
# green on hand-built directories and red on the product.
d="$work/real"; sh "$here/assemble.sh" "$d" >/dev/null
sh "$check" "$d" >/dev/null 2>&1 || fail "case 7: the site assemble.sh produces does not satisfy the checker"
pass "case 7: the site assemble.sh produces is internally consistent"

printf 'check-site-smoke: all cases passed\n'
