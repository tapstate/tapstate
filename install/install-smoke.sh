#!/usr/bin/env bash
#
# Test harness for install/install.sh. Exercises the installer as a black box against a local file://
# stub release tree, so no network is touched: platform detection and its four-tuple mapping, the
# unsupported-platform refusals (Windows/musl/unknown — the AC17 negatives, which must exit non-zero and
# leave no binary behind), sha256 verification (a mismatch must refuse), the TAPSTATE_INSTALL_DIR seam,
# and an idempotent re-run. A fake `uname` (and, for the musl case, a fake `ldd`) placed first on PATH
# drives the platform each run sees. Exit 0 iff every check passes.
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
INSTALL_SH="$HERE/install.sh"
# The version under test is the script's own pin, so these fixtures can never drift from the default
# a bare run installs; and the pin itself is held to pom.xml the same way the quickstart's is.
VERSION="$(sed -n 's/^PINNED_VERSION="\(.*\)"$/\1/p' "$INSTALL_SH")"
[ -n "$VERSION" ] || { printf 'cannot read PINNED_VERSION from %s\n' "$INSTALL_SH" >&2; exit 1; }
POM_VERSION="$(sed -n 's/.*<revision>\(.*\)<\/revision>.*/\1/p' "$HERE/../pom.xml" | head -1)"
if [ "$VERSION" != "$POM_VERSION" ]; then
  printf 'FAIL  pinned version %s does not match pom.xml revision %s -- bump install.sh\n' "$VERSION" "$POM_VERSION" >&2
  exit 1
fi

PASS=0; FAIL=0
ok()  { printf '  PASS  %s\n' "$1"; PASS=$((PASS + 1)); }
bad() { printf '  FAIL  %s\n' "$1"; FAIL=$((FAIL + 1)); }

sha256_of() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1"; else shasum -a 256 "$1"; fi; }

# --- a stub release tree: <stub>/download/v<ver>/tapstate-<ver>-<platform>.tar.gz (+ .sha256) --------
STUB="$(mktemp -d)"
trap 'rm -rf "$STUB"' EXIT
make_asset() {   # $1 = platform label; the fake binary echoes its platform so a test can prove the mapping
  platform="$1"
  d="$STUB/download/v$VERSION"; mkdir -p "$d"
  stage="$(mktemp -d)"
  mkdir -p "$stage/tapstate-cli-$VERSION/bin" "$stage/tapstate-cli-$VERSION/libexec"
  printf '#!/bin/sh\necho "tapstate %s %s"\n' "$VERSION" "$platform" > "$stage/tapstate-cli-$VERSION/bin/tapstate"
  chmod +x "$stage/tapstate-cli-$VERSION/bin/tapstate"
  printf '#!/bin/sh\necho mcp\n' > "$stage/tapstate-cli-$VERSION/libexec/tapstate-mcp"
  chmod +x "$stage/tapstate-cli-$VERSION/libexec/tapstate-mcp"
  echo license > "$stage/LICENSE"; echo notice > "$stage/NOTICE"
  asset="tapstate-$VERSION-$platform.tar.gz"
  tar -czf "$d/$asset" -C "$stage" "tapstate-cli-$VERSION" LICENSE NOTICE
  ( cd "$d" && sha256_of "$asset" > "$asset.sha256" )
  rm -rf "$stage"
}
for p in darwin-arm64 darwin-x64 linux-x64 linux-arm64; do make_asset "$p"; done

# run install.sh seeing a fake platform. args: OS ARCH MUSL(glibc|musl) INSTALL_DIR [VERSION]
# The fifth argument is the version this platform reports about itself -- a macOS product version on
# Darwin, a glibc version on Linux -- installed as a fake `sw_vers` or `ldd` accordingly, which is what
# drives the recommended-version notice. Leaving it empty means the run sees whatever this machine has
# (on Linux, no sw_vers at all), the same as every test written before the notice existed.
run_install() {
  local fos="$1" farch="$2" fmusl="$3" idir="$4" fver="${5:-}" shim
  shim="$(mktemp -d)"
  cat > "$shim/uname" <<EOF
#!/bin/sh
case "\$1" in
  -s) echo "$fos" ;;
  -m) echo "$farch" ;;
  *)  echo unknown ;;
esac
EOF
  chmod +x "$shim/uname"
  if [ -n "$fver" ]; then
    case "$fos" in
      Darwin) printf '#!/bin/sh\necho "%s"\n' "$fver" > "$shim/sw_vers"; chmod +x "$shim/sw_vers" ;;
      # the real banner's shape, package suffix and all, so the parse is tested against what ldd prints
      Linux)  printf '#!/bin/sh\necho "ldd (Ubuntu GLIBC %s-0ubuntu8.7) %s"\n' "$fver" "$fver" > "$shim/ldd"
              chmod +x "$shim/ldd" ;;
    esac
  fi
  # written last so the musl case wins: is_musl reads this same ldd, and refusing musl comes first
  if [ "$fmusl" = musl ]; then
    printf '#!/bin/sh\necho "musl libc (x86_64)\\nVersion 1.2.4"\n' > "$shim/ldd"
    chmod +x "$shim/ldd"
  fi
  OUT="$(PATH="$shim:$PATH" \
         TAPSTATE_VERSION="$VERSION" \
         TAPSTATE_BASE_URL="file://$STUB" \
         TAPSTATE_INSTALL_DIR="$idir" \
         sh "$INSTALL_SH" 2>&1)"
  RC=$?
  rm -rf "$shim"
}

printf '\033[1minstall smoke — %s\033[0m\n' "$INSTALL_SH"

# --- positive: the four supported tuples map correctly and install a runnable binary ----------------
for triple in "Darwin arm64 darwin-arm64" "Darwin x86_64 darwin-x64" "Linux x86_64 linux-x64" "Linux aarch64 linux-arm64"; do
  # shellcheck disable=SC2086
  set -- $triple
  idir="$(mktemp -d)/bin"
  run_install "$1" "$2" glibc "$idir"
  if [ "$RC" -eq 0 ] && [ -L "$idir/tapstate" ] \
     && [ -x "$idir/versions/$VERSION/bin/tapstate" ] \
     && [ -x "$idir/versions/$VERSION/libexec/tapstate-mcp" ] \
     && "$idir/tapstate" | grep -q "tapstate $VERSION $3"; then
    ok "maps $1/$2 -> $3 and installs an executable bundle with an atomic stable entry"
  else
    bad "install $1/$2 (want $3) rc=$RC: $OUT"
  fi
done

# --- detect-only: --print-platform maps and prints the tuple, downloading and writing nothing -------
# The demo bootstrap (quickstart.sh) reuses this to gate on the platform before it fetches anything, so
# an unsupported platform leaves the working directory untouched. It must print only the tuple, need no
# network (no version resolution), and create no install directory.
detect_shim="$(mktemp -d)"
# shellcheck disable=SC2016
printf '#!/bin/sh\ncase "$1" in -s) echo Darwin ;; -m) echo arm64 ;; *) echo unknown ;; esac\n' > "$detect_shim/uname"
chmod +x "$detect_shim/uname"
idir="$(mktemp -d)/bin"
out="$(PATH="$detect_shim:$PATH" TAPSTATE_INSTALL_DIR="$idir" sh "$INSTALL_SH" --print-platform 2>&1)"; rc=$?
if [ "$rc" -eq 0 ] && [ "$out" = darwin-arm64 ] && [ ! -e "$idir/tapstate" ] && [ ! -d "$idir" ]; then
  ok "--print-platform prints the tuple and downloads/writes nothing"
else
  bad "--print-platform (rc=$rc, out='$out', install dir present=$( [ -e "$idir" ] && echo yes || echo no ))"
fi
# and unsupported platforms still fail loudly in detect-only mode, pointing the user elsewhere
cat > "$detect_shim/uname" <<'EOF'
#!/bin/sh
case "$1" in -s) echo MINGW64_NT-10.0 ;; -m) echo x86_64 ;; *) echo unknown ;; esac
EOF
chmod +x "$detect_shim/uname"
idir="$(mktemp -d)/bin"
out="$(PATH="$detect_shim:$PATH" TAPSTATE_INSTALL_DIR="$idir" sh "$INSTALL_SH" --print-platform 2>&1)"; rc=$?
rm -rf "$detect_shim"
if [ "$rc" -ne 0 ] && [ ! -e "$idir" ] && printf '%s' "$out" | grep -qiE 'wsl|source'; then
  ok "--print-platform on an unsupported platform fails loudly and writes nothing"
else
  bad "--print-platform unsupported (rc=$rc, out='$out')"
fi

# --- the declared platform list: what a release is checked against -----------------------------------
# The release manifest check asks the installer which platforms it can serve, and fails a release whose
# assets do not match. So the list has to be answerable on a machine no build exists for -- the check
# runs on one runner and speaks for all four -- and it has to be the same list detect_platform maps
# onto, which the four positive cases above already prove by installing through it.
unsupported_shim="$(mktemp -d)"
cat > "$unsupported_shim/uname" <<'EOF'
#!/bin/sh
case "$1" in -s) echo Plan9 ;; -m) echo sparc ;; *) echo unknown ;; esac
EOF
chmod +x "$unsupported_shim/uname"
idir="$(mktemp -d)/bin"
out="$(PATH="$unsupported_shim:$PATH" TAPSTATE_INSTALL_DIR="$idir" sh "$INSTALL_SH" --print-platforms 2>&1)"; rc=$?
rm -rf "$unsupported_shim"
expected="$(printf 'darwin-arm64\ndarwin-x64\nlinux-arm64\nlinux-x64')"
if [ "$rc" -eq 0 ] && [ "$out" = "$expected" ] && [ ! -e "$idir" ]; then
  ok "--print-platforms lists every published platform, from a machine that is not one of them"
else
  bad "--print-platforms (rc=$rc, out='$(printf '%s' "$out" | tr '\n' ' ')', install dir present=$( [ -e "$idir" ] && echo yes || echo no ))"
fi

# --- idempotent: an identical version retains the live bundle before updating the stable entry --------
idir="$(mktemp -d)/bin"
run_install Darwin arm64 glibc "$idir"; rc1=$RC
printf '\n# immutable-version-sentinel\n' >> "$idir/versions/$VERSION/bin/tapstate"
run_install Darwin arm64 glibc "$idir"; rc2=$RC
if [ "$rc1" -eq 0 ] && [ "$rc2" -eq 0 ] && [ -L "$idir/tapstate" ] \
   && [ -x "$idir/versions/$VERSION/bin/tapstate" ] \
   && [ -x "$idir/versions/$VERSION/libexec/tapstate-mcp" ] \
   && grep -q 'immutable-version-sentinel' "$idir/versions/$VERSION/bin/tapstate"; then
  ok "re-run retains the complete immutable version bundle (exit 0, stable entry intact)"
else
  bad "re-run replaced or damaged the immutable version bundle (rc1=$rc1 rc2=$rc2): $OUT"
fi

# --- AC17 negatives: unsupported platforms refuse, non-zero, no binary left behind ------------------
neg() {   # OS ARCH MUSL grep-for label
  local idir; idir="$(mktemp -d)/bin"
  run_install "$1" "$2" "$3" "$idir"
  if [ "$RC" -ne 0 ] && printf '%s' "$OUT" | grep -qiE "$4" && [ ! -e "$idir/tapstate" ]; then
    ok "$5"
  else
    bad "$5 (rc=$RC, binary present=$( [ -e "$idir/tapstate" ] && echo yes || echo no )): $OUT"
  fi
}
neg "MINGW64_NT-10.0" x86_64  glibc 'wsl|source'         "refuses Git Bash / MinGW (points to WSL or source)"
neg "MSYS_NT-10.0"    x86_64  glibc 'wsl|source'         "refuses MSYS2 (points to WSL or source)"
neg "CYGWIN_NT-10.0"  x86_64  glibc 'wsl|source'         "refuses Cygwin (points to WSL or source)"
neg "Linux"           x86_64  musl  'musl'               "refuses musl libc (Alpine)"
neg "Linux"           riscv64 glibc 'architecture|riscv' "refuses an unknown architecture, never guesses"
neg "SunOS"           x86_64  glibc 'system|sunos'       "refuses an unknown OS, never guesses"

# --- sha256 verification: a mismatch refuses (does not silently install) -----------------------------
echo "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef  tapstate-$VERSION-darwin-arm64.tar.gz" \
  > "$STUB/download/v$VERSION/tapstate-$VERSION-darwin-arm64.tar.gz.sha256"
idir="$(mktemp -d)/bin"
run_install Darwin arm64 glibc "$idir"
if [ "$RC" -ne 0 ] && [ ! -e "$idir/tapstate" ] && printf '%s' "$OUT" | grep -qiE 'checksum|sha256|verif'; then
  ok "refuses to install when the sha256 does not match the download"
else
  bad "sha256 mismatch not refused (rc=$RC): $OUT"
fi
make_asset darwin-arm64   # restore the good checksum

# --- the pinned default over HTTP: no TAPSTATE_VERSION -> PINNED_VERSION, end to end ----------------
# A bare run must not consult /releases/latest at all: that redirect names only full releases, so it
# cannot see a prerelease, and the pinned default is the whole point. The stub still answers the
# redirect -- with a deliberately wrong version -- so a script that quietly went back to asking it
# installs the wrong thing here and fails. The stub double-forks and publishes its port + pid.
if command -v python3 >/dev/null 2>&1; then
  cat > "$STUB/httpstub.py" <<'PY'
import http.server, os, socketserver, sys
root = sys.argv[1]
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        host, port = self.server.server_address
        if self.path == "/releases/latest":
            self.send_response(302)
            self.send_header("Location", "http://%s:%d/releases/tag/v9999.0.0" % (host, port))
            self.end_headers(); return
        if self.path.startswith("/releases/tag/"):
            self.send_response(200); self.send_header("Content-Length", "0"); self.end_headers(); return
        if self.path.startswith("/releases/download/"):
            local = root + self.path[len("/releases"):]
            if os.path.isfile(local):
                data = open(local, "rb").read()
                self.send_response(200); self.send_header("Content-Length", str(len(data))); self.end_headers()
                self.wfile.write(data); return
        self.send_response(404); self.end_headers()
    def log_message(self, *a):
        pass
srv = socketserver.TCPServer(("127.0.0.1", 0), H)
with open(sys.argv[3], "w") as f:
    f.write(str(srv.server_address[1]))
if os.fork() > 0:
    os._exit(0)
os.setsid()
with open(sys.argv[4], "w") as f:
    f.write(str(os.getpid()))
srv.serve_forever()
PY
  python3 "$STUB/httpstub.py" "$STUB" "$VERSION" "$STUB/port" "$STUB/pid"
  port="$(cat "$STUB/port")"
  shim="$(mktemp -d)"
  # the $1 is the fake uname script's own argument — it must stay literal here.
  # shellcheck disable=SC2016
  printf '#!/bin/sh\ncase "$1" in -s) echo Darwin ;; -m) echo arm64 ;; *) echo unknown ;; esac\n' > "$shim/uname"
  chmod +x "$shim/uname"
  idir="$(mktemp -d)/bin"
  out="$(PATH="$shim:$PATH" TAPSTATE_BASE_URL="http://127.0.0.1:$port/releases" TAPSTATE_INSTALL_DIR="$idir" sh "$INSTALL_SH" 2>&1)"; rc=$?
  if [ -f "$STUB/pid" ]; then kill "$(cat "$STUB/pid")" 2>/dev/null || true; fi
  rm -rf "$shim"
  if [ "$rc" -eq 0 ] && [ -x "$idir/tapstate" ] && printf '%s' "$out" | grep -q "tapstate $VERSION"; then
    ok "a bare run installs the pinned version over HTTP, never asking /releases/latest"
  else
    bad "pinned-default install failed (rc=$rc): $out"
  fi
else
  printf '  SKIP  pinned-default over HTTP (python3 not available)\n'
fi

# --- the recommended macOS version: said out loud, never enforced -----------------------------------
# A native binary carries the deployment target of the machine that built it, so an older macOS may not
# load it -- and when that happens it happens at launch, from dyld, far from the install that caused it.
# So the installer says so. It does not refuse: unlike the platforms above, a binary for this one exists,
# and whether to try it is the user's call. Every case below therefore asserts the install *succeeded*;
# what varies is only whether the notice was printed. The recommendation belongs to the release (build
# machines move between releases), so it is published alongside the assets and read from there.
MINIMUMS="$STUB/download/v$VERSION/platform-minimums.txt"
FLOOR=
set_floor() { FLOOR="$1"; printf 'darwin-arm64 macos %s\ndarwin-x64 macos %s\n' "$1" "$1" > "$MINIMUMS"; }

# Detect the notice by a phrase only it carries. Matching on the version alone would be fooled by a
# temp path that happens to contain the same digits.
noticed() { printf '%s' "$OUT" | grep -q 'may not launch'; }

say() {   # MACOS_VERSION EXPECT(notice|quiet) LABEL
  local idir said; idir="$(mktemp -d)/bin"
  run_install Darwin arm64 glibc "$idir" "$1"
  if [ "$RC" -ne 0 ] || [ ! -x "$idir/tapstate" ]; then
    bad "$3 -- the install must never be refused (rc=$RC): $OUT"; return
  fi
  if noticed; then said=notice; else said=quiet; fi
  if [ "$said" != "$2" ]; then
    bad "$3 (wanted $2, got $said): $OUT"; return
  fi
  # a notice that does not name both versions leaves the reader to guess which macOS this needs
  if [ "$2" = notice ] && ! { printf '%s' "$OUT" | grep -qF "$FLOOR" && printf '%s' "$OUT" | grep -qF "$1"; }; then
    bad "$3 -- notice names neither the recommendation nor the running version: $OUT"; return
  fi
  ok "$3"
}

set_floor 15.0
say 14.7 notice "says so below the recommended version, and installs anyway, naming both versions"
say 15.0 quiet  "stays quiet on exactly the recommended version"
say 15.5 quiet  "stays quiet on a newer macOS in the same major"
say 26.1 quiet  "stays quiet on a higher major -- the version the next runner generation will publish"

# Version fields are numbers, not text, and both directions of getting that wrong are covered. Compared
# as text, 15.9 sorts above 15.10 -- so the machine that most needs telling would hear nothing -- and a
# bare "15" sorts below "15.0", which would nag a machine sitting exactly on the recommendation. Neither
# is hypothetical: macOS reports both shapes, and minor versions do reach double digits.
set_floor 15.10
say 15.9 notice "says so for 15.9 against a 15.10 recommendation (fields compared as numbers, not as text)"
set_floor 15.0
say 15 quiet "stays quiet on a bare major equal to the recommendation (an absent field counts as zero)"

# Each platform carries its own recommendation, and the two darwin legs are built on separate machines
# that need not move in step. The line is selected by the platform tuple, not by being first in the file.
printf 'darwin-arm64 macos 15.0\ndarwin-x64 macos 26.0\n' > "$MINIMUMS"
idir="$(mktemp -d)/bin"
run_install Darwin x86_64 glibc "$idir" 15.5
if [ "$RC" -eq 0 ] && [ -x "$idir/tapstate" ] && noticed && printf '%s' "$OUT" | grep -qF 26.0; then
  ok "reads the recommendation of the platform being installed, not whichever line comes first"
else
  bad "platform-keyed lookup (rc=$RC, noticed=$(noticed && echo yes || echo no)): $OUT"
fi
idir="$(mktemp -d)/bin"
run_install Darwin arm64 glibc "$idir" 15.5
if [ "$RC" -eq 0 ] && [ -x "$idir/tapstate" ] && ! noticed; then
  ok "the same file leaves arm64 at 15.5 alone, whose own recommendation is lower"
else
  bad "arm64 nagged by the x64 recommendation (rc=$RC): $OUT"
fi

# a macOS recommendation says nothing about Linux, which has no sw_vers and no entry in the file
idir="$(mktemp -d)/bin"
run_install Linux x86_64 glibc "$idir"
if [ "$RC" -eq 0 ] && [ -x "$idir/tapstate" ] && ! noticed; then
  ok "a macOS recommendation does not reach a Linux install"
else
  bad "linux install saw the macOS notice (rc=$RC): $OUT"
fi

# --- the other platform's requirement travels the same path, told apart by the requirement field ------
# A Linux binary is tied to the newest glibc symbols it references rather than to an OS version, so its
# line names `glibc` and the running version comes from ldd. One code path serves both, which is what
# lets a release add a requirement this installer has never heard of: it is passed by, not guessed at.
printf 'linux-x64 glibc 2.34\nlinux-arm64 glibc 2.34\n' > "$MINIMUMS"

lsay() {   # GLIBC_VERSION EXPECT(notice|quiet) LABEL
  local idir said; idir="$(mktemp -d)/bin"
  run_install Linux x86_64 glibc "$idir" "$1"
  if [ "$RC" -ne 0 ] || [ ! -x "$idir/tapstate" ]; then
    bad "$3 -- the install must never be refused (rc=$RC): $OUT"; return
  fi
  if noticed; then said=notice; else said=quiet; fi
  if [ "$said" != "$2" ]; then
    bad "$3 (wanted $2, got $said): $OUT"; return
  fi
  if [ "$2" = notice ] && ! { printf '%s' "$OUT" | grep -qF 2.34 && printf '%s' "$OUT" | grep -qF "$1"; }; then
    bad "$3 -- notice names neither the recommendation nor the running version: $OUT"; return
  fi
  ok "$3"
}

lsay 2.31 notice "says so below the recommended glibc, and installs anyway, naming both versions"
lsay 2.34 quiet  "stays quiet on exactly the recommended glibc"
lsay 2.39 quiet  "stays quiet on a newer glibc"

# a requirement this installer does not implement is passed by in silence -- the seam that lets a later
# release publish something older installers were never taught to check
printf 'linux-x64 gizmo 9.9\n' > "$MINIMUMS"
idir="$(mktemp -d)/bin"
run_install Linux x86_64 glibc "$idir" 2.31
if [ "$RC" -eq 0 ] && [ -x "$idir/tapstate" ] && ! noticed; then
  ok "a requirement this installer does not know is passed by, not guessed at"
else
  bad "unknown requirement kind must produce no notice (rc=$RC): $OUT"
fi

# a release that publishes no minimums has nothing to compare against, and must not invent one
rm -f "$MINIMUMS"
idir="$(mktemp -d)/bin"
run_install Darwin arm64 glibc "$idir" 14.7
if [ "$RC" -eq 0 ] && [ -x "$idir/tapstate" ] && ! noticed; then
  ok "a release without published minimums installs silently (nothing to compare against)"
else
  bad "missing minimums must produce no notice (rc=$RC): $OUT"
fi

# --- the tap alias: opt-in shortcut, never a second copy of the binary --------------------------------
# `tapstate` stays the only real command; `tap` exists to save keystrokes. It is a link to the same
# target the stable entry points at, so an upgrade moves both at once -- two independent copies would
# drift the moment a version directory is replaced.
idir="$(mktemp -d)/bin"
run_install Darwin arm64 glibc "$idir"
if [ -L "$idir/tap" ] \
   && [ "$(readlink "$idir/tap")" = "$(readlink "$idir/tapstate")" ]; then
  ok "installs a tap alias resolving to the same target as tapstate"
else
  bad "tap alias missing or pointing elsewhere: tap=$(readlink "$idir/tap" 2>/dev/null) tapstate=$(readlink "$idir/tapstate" 2>/dev/null)"
fi

# An upgrade must move the alias too. Creating it only on a first install leaves `tap` pointing into a
# version directory the installer has since deleted -- a dangling link that reports "no such file".
idir="$(mktemp -d)/bin"
run_install Darwin arm64 glibc "$idir"
FIRST_VERSION="$VERSION"
VERSION=9.9.9; make_asset darwin-arm64        # a second release in the same stub tree
run_install Darwin arm64 glibc "$idir"
VERSION="$FIRST_VERSION"
if [ -L "$idir/tap" ] && [ -e "$idir/tap" ] \
   && [ "$(readlink "$idir/tap")" = "$(readlink "$idir/tapstate")" ]; then
  ok "an upgrade moves the alias with the stable entry rather than stranding it"
else
  bad "alias stranded after upgrade: tap=$(readlink "$idir/tap" 2>/dev/null) (exists: $([ -e "$idir/tap" ] && echo yes || echo no))"
fi

# A machine that already has a `tap` on PATH (node-tap ships one) keeps it. The installer must say so:
# skipping silently leaves the user with no alias and no idea why.
idir="$(mktemp -d)/bin"
occupied="$(mktemp -d)"
printf '#!/bin/sh\necho "not tapstate"\n' > "$occupied/tap"; chmod +x "$occupied/tap"
OUT="$(PATH="$occupied:$PATH" \
       TAPSTATE_VERSION="$VERSION" \
       TAPSTATE_BASE_URL="file://$STUB" \
       TAPSTATE_INSTALL_DIR="$idir" \
       sh "$INSTALL_SH" 2>&1)"; RC=$?
# The message has to be looked for by a phrase that cannot appear by accident: grepping for "tap"
# alone passes on any line mentioning tapstate, so this assertion would hold before the feature exists.
if [ "$RC" -eq 0 ] && [ ! -e "$idir/tap" ] && printf '%s' "$OUT" | grep -q 'already on PATH'; then
  ok "leaves an existing tap alone and says why the alias was skipped"
else
  bad "existing tap not respected or skip not explained rc=$RC: $OUT"
fi
rm -rf "$occupied"

# A foreign `tap` sitting in the install directory itself. PATH cannot see it whenever that directory
# is not on PATH -- which is the default this installer prints instructions about -- so a check that
# asks PATH alone finds nothing and the mv overwrites someone else's file.
idir="$(mktemp -d)/bin"
mkdir -p "$idir"
printf '#!/bin/sh\necho "not tapstate"\n' > "$idir/tap"; chmod +x "$idir/tap"
before="$(cat "$idir/tap")"
OUT="$(TAPSTATE_VERSION="$VERSION" TAPSTATE_BASE_URL="file://$STUB" TAPSTATE_INSTALL_DIR="$idir" \
       sh "$INSTALL_SH" 2>&1)"; RC=$?
if [ "$RC" -eq 0 ] && [ ! -L "$idir/tap" ] && [ "$(cat "$idir/tap")" = "$before" ] \
   && printf '%s' "$OUT" | grep -q 'is not ours'; then
  ok "a foreign tap inside the install directory is left untouched, and the skip says so"
else
  bad "local foreign tap was overwritten or the skip not explained rc=$RC: $OUT (is-link: $([ -L "$idir/tap" ] && echo yes || echo no))"
fi

# And the mirror image: our own alias must still be upgraded when some other tap precedes it on PATH.
# Deciding on PATH alone skipped the upgrade there, leaving the shortcut pointing into a version
# directory this script deletes on the way out -- a dangling command, produced by an installer that
# reported success.
#
# The second install has to be a different release, or the case proves nothing: install the same
# version twice and the alias matches whether or not it was rewritten, so the skipped-upgrade bug
# passes. The stub tree already carries 9.9.9 for exactly this.
idir="$(mktemp -d)/bin"
run_install Darwin arm64 glibc "$idir"
occupied="$(mktemp -d)"
printf '#!/bin/sh\necho "not tapstate"\n' > "$occupied/tap"; chmod +x "$occupied/tap"
FIRST_VERSION="$VERSION"
VERSION=9.9.9; make_asset darwin-arm64
PATH="$occupied:$PATH" run_install Darwin arm64 glibc "$idir"
VERSION="$FIRST_VERSION"
if [ "$RC" -eq 0 ] && [ "$(readlink "$idir/tap")" = "versions/9.9.9/bin/tapstate" ]; then
  ok "our own alias is upgraded to the new release even when another tap precedes it on PATH"
else
  bad "own alias not upgraded past a PATH conflict rc=$RC: tap=$(readlink "$idir/tap" 2>/dev/null)"
fi
rm -rf "$occupied"

# A crafted link that matches the shape but escapes the directory. The ownership test is a glob, so
# `..` is the way through it; adopting such a link means replacing a file this installation does not
# own, which is the same failure the plain foreign case covers, reached by a different road.
idir="$(mktemp -d)/bin"
mkdir -p "$idir"
elsewhere="$(mktemp -d)"
printf '#!/bin/sh\necho "not tapstate"\n' > "$elsewhere/tapstate"; chmod +x "$elsewhere/tapstate"
ln -s "versions/../../..$elsewhere/bin/tapstate" "$idir/tap"
target_before="$(readlink "$idir/tap")"
run_install Darwin arm64 glibc "$idir"
if [ "$RC" -eq 0 ] && [ "$(readlink "$idir/tap")" = "$target_before" ] \
   && printf '%s' "$OUT" | grep -q 'is not ours'; then
  ok "a link that matches the shape but escapes the directory is not adopted"
else
  bad "traversal link adopted rc=$RC: now=$(readlink "$idir/tap" 2>/dev/null) was=$target_before"
fi
rm -rf "$elsewhere"

# The alias is staged under a temporary name and moved into place, so a run that dies between the two
# must not leave that name behind. This drives the failure with an `mv` that refuses the alias move --
# the same window an interrupt opens, reached deterministically -- and then looks for the dot-file. The
# work area is a temp dir nobody sees again; this one lands in a directory the user keeps forever.
idir="$(mktemp -d)/bin"
shim="$(mktemp -d)"
cat > "$shim/mv" <<'SHIM'
#!/bin/sh
# Refuse only the alias staging move; everything else this installer does must still work, or the
# case would prove that a broken mv breaks the install rather than that the trap cleans up.
case "$*" in *.tap.*) exit 1 ;; esac
exec /bin/mv "$@"
SHIM
chmod +x "$shim/mv"
OUT="$(PATH="$shim:$PATH" \
       TAPSTATE_VERSION="$VERSION" \
       TAPSTATE_BASE_URL="file://$STUB" \
       TAPSTATE_INSTALL_DIR="$idir" \
       sh "$INSTALL_SH" 2>&1)"; RC=$?
STRANDED="$(find "$idir" -maxdepth 1 -name '.tap.*' 2>/dev/null | wc -l | tr -d ' ')"
if [ "$RC" -ne 0 ] && [ "$STRANDED" = 0 ]; then
  ok "an aborted alias move leaves no staged dot-file behind"
else
  bad "staged alias stranded or the abort was not detected rc=$RC stranded=$STRANDED: $OUT"
fi
rm -rf "$shim"

# --- the install event: what it carries, when it fires, and when it must not ------------------------
# A local sink stands in for the endpoint, so these run with no network and no credentials. Each case
# asserts on what actually arrived, not on what the script claims it sends.
if ! command -v python3 >/dev/null 2>&1; then
  bad "install event: python3 is needed for the local sink"
else
  BEACON_DIR="$(mktemp -d)"
  : > "$BEACON_DIR/log"
  python3 - "$BEACON_DIR" <<'PYEOF' &
import http.server, os, sys
d = sys.argv[1]
class H(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get('Content-Length') or 0)
        body = self.rfile.read(n).decode('utf-8', 'replace')
        with open(os.path.join(d, 'log'), 'a') as fh:
            fh.write(body + "\n")
        self.send_response(204); self.end_headers()
    def log_message(self, *a): pass
srv = http.server.HTTPServer(('127.0.0.1', 0), H)
with open(os.path.join(d, 'port'), 'w') as fh:
    fh.write(str(srv.server_address[1]))
srv.serve_forever()
PYEOF
  BEACON_PID=$!
  for _ in $(seq 1 50); do [ -s "$BEACON_DIR/port" ] && break; sleep 0.1; done
  BEACON_URL="http://127.0.0.1:$(cat "$BEACON_DIR/port")/e"

  # A second stub version, so "which version did it report" has two possible answers. With only the
  # pinned one in the tree the assertion cannot fail, and the field it guards is what makes the funnel
  # per-version at all.
  OTHER_VERSION=9.9.9
  _real_version="$VERSION"; VERSION="$OTHER_VERSION"; make_asset darwin-arm64; VERSION="$_real_version"

  beacon_reset() { : > "$BEACON_DIR/log"; }
  beacon_count() { grep -c . "$BEACON_DIR/log" 2>/dev/null | tr -d ' '; }
  beacon_field() { sed -n "s/.*\"$1\":\"\([^\"]*\)\".*/\1/p" "$BEACON_DIR/log" | head -1; }

  # run the installer with an explicit version and arbitrary args, capturing stderr separately
  # $1 install_dir  $2 version  $3 stderr_file  $4 stdout_file  rest: args to install.sh
  # Extra environment goes through `env` on purpose. Writing ${EV_ENV} among the assignment prefixes
  # does not work: bash parses assignments before expanding, so the expanded text becomes the command
  # name and the run dies -- which is a failure shaped exactly like "nothing was sent".
  ev_run() {
    local idir="$1" fver="$2" errf="$3" outf="$4"; shift 4
    local shim; shim="$(mktemp -d)"
    # shellcheck disable=SC2016
    printf '#!/bin/sh\ncase "$1" in -s) echo Darwin ;; -m) echo arm64 ;; *) echo unknown ;; esac\n' > "$shim/uname"
    chmod +x "$shim/uname"
    # shellcheck disable=SC2086
    env ${EV_ENV:-} \
      PATH="$shim:$PATH" \
      TAPSTATE_VERSION="$fver" \
      TAPSTATE_BASE_URL="file://$STUB" \
      TAPSTATE_INSTALL_DIR="$idir" \
      TAPSTATE_TELEMETRY_URL="$BEACON_URL" \
      sh "$INSTALL_SH" "$@" >"$outf" 2>"$errf"
    rm -rf "$shim"
  }

  ev_err="$(mktemp)"
  ev_out="$(mktemp)"

  # version fidelity: the event carries what was actually installed, not the script's own pin. An
  # implementation reporting PINNED_VERSION is byte-identical on a default run, so only an explicit
  # TAPSTATE_VERSION separates the two -- and the per-version funnel rests entirely on this field.
  d1="$(mktemp -d)/bin"
  beacon_reset; ev_run "$d1" "$OTHER_VERSION" "$ev_err" "$ev_out"
  got="$(beacon_field version)"
  if [ "$got" = "$OTHER_VERSION" ]; then ok "install event carries the resolved version ($got)"
  else bad "install event version: expected $OTHER_VERSION, got '$got'"; fi
  if [ "$(beacon_field entrypoint)" = cli ]; then ok "install event defaults entrypoint=cli"
  else bad "install event entrypoint: got '$(beacon_field entrypoint)'"; fi
  first_id="$(beacon_field installation_id)"

  # idempotence: the same installation root keeps its id, so a reinstall is not a second install --
  # and the denominator is exactly "installs", so regenerating here inflates it.
  beacon_reset; ev_run "$d1" "$OTHER_VERSION" "$ev_err" "$ev_out"
  second_id="$(beacon_field installation_id)"
  if [ -n "$first_id" ] && [ "$first_id" = "$second_id" ]; then ok "installation_id survives a reinstall in place"
  else bad "installation_id changed on reinstall: '$first_id' -> '$second_id'"; fi

  # isolation: another installation root is another installation. This pins the semantics -- an id
  # written to a fixed $HOME path would hand back the same value here.
  d2="$(mktemp -d)/bin"
  beacon_reset; ev_run "$d2" "$OTHER_VERSION" "$ev_err" "$ev_out"
  other_id="$(beacon_field installation_id)"
  if [ -n "$other_id" ] && [ "$other_id" != "$first_id" ]; then ok "a second installation root gets its own id"
  else bad "second installation root reused the first id ('$other_id')"; fi

  # opt-out: nothing sent AND nothing written. Skipping only the request still leaves an identifier on
  # the user's disk, and no network assertion would ever notice.
  d3="$(mktemp -d)/bin"
  beacon_reset; EV_ENV="TAPSTATE_TELEMETRY=off" ev_run "$d3" "$OTHER_VERSION" "$ev_err" "$ev_out"; EV_ENV=""
  if [ ! -x "$d3/tapstate" ]; then
    bad "TAPSTATE_TELEMETRY=off: the install itself did not happen, so this case proves nothing"
  elif [ "$(beacon_count)" = 0 ]; then ok "TAPSTATE_TELEMETRY=off sends nothing (install did complete)"
  else bad "TAPSTATE_TELEMETRY=off still sent $(beacon_count) event(s)"; fi
  if find "$d3" -name '*installation*id*' 2>/dev/null | grep -q .; then
    bad "TAPSTATE_TELEMETRY=off still wrote an id file"
  else ok "TAPSTATE_TELEMETRY=off writes no id file"; fi

  # the platform gate stays side-effect free: quickstart.sh calls it before every install, so a beacon
  # hung at script entry would double every install this ever measures.
  # The directory must already exist, which is the quickstart's real shape (TAPSTATE_INSTALL_DIR=$PWD,
  # the demo directory it is standing in). With a not-yet-created directory an unrelated guard inside
  # the sender also happens to suppress the event, and the case would pass while the defect it exists
  # to catch -- an event fired from the platform gate -- was present.
  d4="$(mktemp -d)/bin"; mkdir -p "$d4"
  beacon_reset; ev_run "$d4" "$OTHER_VERSION" "$ev_err" "$ev_out" --print-platform
  if [ "$(beacon_count)" = 0 ]; then ok "--print-platform sends no event"
  else bad "--print-platform sent $(beacon_count) event(s)"; fi

  # disclosure on stderr: quickstart.sh runs the installer with stdout dropped, so a disclosure written
  # to stdout is invisible on the path most first-time users take -- while a stdout-based test passes.
  d5="$(mktemp -d)/bin"
  beacon_reset; ev_run "$d5" "$OTHER_VERSION" "$ev_err" "$ev_out"
  if ! grep -qi 'anonymous install event' "$ev_err"; then
    bad "disclosure not on stderr; a quickstart user would never see it"
  elif ! grep -qi 'TAPSTATE_TELEMETRY=off' "$ev_err"; then
    bad "disclosure on stderr does not say how to turn it off"
  elif grep -qiE 'anonymous install event|TAPSTATE_TELEMETRY=off' "$ev_out"; then
    bad "part of the disclosure went to stdout, which the quickstart drops"
  else ok "the whole disclosure is on stderr, none of it on stdout"; fi

  rm -f "$ev_err" "$ev_out"
  kill "$BEACON_PID" 2>/dev/null; wait "$BEACON_PID" 2>/dev/null
fi

# --- summary ----------------------------------------------------------------------------------------
echo
printf '\033[1minstall smoke: %d passed, %d failed\033[0m\n' "$PASS" "$FAIL"
[ "$FAIL" -eq 0 ]
