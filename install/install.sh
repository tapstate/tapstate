#!/bin/sh
#
# tapstate installer — downloads the prebuilt CLI bundle for this platform, verifies it, and installs
# a versioned CLI plus its sibling MCP sidecar behind a stable entry point. It never uses sudo, never
# edits a shell rc file, and never guesses: an
# unsupported platform (Windows shells, musl libc, or an unknown OS/arch) fails loudly before anything
# is downloaded, so nothing is left behind.
#
# Usage (download-then-run, or piped):
#   sh install.sh
#
# Environment seams:
#   TAPSTATE_VERSION       install a specific version (e.g. 0.1.0); default is the pinned release below.
#   TAPSTATE_INSTALL_DIR   where to place the stable entry and versioned bundles; default $HOME/.tapstate/bin. This is the seam the
#                          demo bootstrap reuses to install in place (TAPSTATE_INSTALL_DIR=.), so the
#                          binary never enters PATH and `rm -rf` of the demo directory removes it.
#   TAPSTATE_BASE_URL      release base URL; default https://github.com/tapstate/tapstate/releases
#                          (override for a mirror).
#
# POSIX sh, no bashisms. All work is inside main(); the final line calls it, so a truncated download can
# never execute a partial script.
set -eu

# The release this script installs by default. Discovering "the latest" sounds better but is not
# available to promise: /releases/latest names only full releases, and while the CLI ships as a
# prerelease that lookup finds nothing and a bare run would die on a clean machine. Pinning also makes
# the promise reproducible -- the same script installs the same build. TAPSTATE_VERSION overrides for
# a one-off; releases update this line, and the smoke fails the build if it drifts from pom.xml.
PINNED_VERSION="0.4.3"

die() {
    printf 'install: %s\n' "$1" >&2
    exit 1
}

# The platforms this installer can serve, one <os>-<arch> per line. Everything else derives from it:
# detect_platform checks membership here, and --print-platforms hands the list to the release manifest
# check, which fails a release whose assets do not match it. A platform added here without a matching
# release asset is caught there rather than by a user finding a 404.
SUPPORTED_PLATFORMS='darwin-arm64
darwin-x64
linux-arm64
linux-x64'

# True when this Linux uses musl (Alpine and similar): the binaries are glibc-linked, so musl is refused.
# musl ships its own dynamic loader, and its ldd banner names it; either signal alone is conclusive.
is_musl() {
    for f in /lib/ld-musl-*.so.1; do
        [ -e "$f" ] && return 0
    done
    if command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi musl; then
        return 0
    fi
    return 1
}

# Map uname to the release's <os>-<arch> tuple, or refuse. Runs before anything touches the filesystem.
detect_platform() {
    os="$(uname -s)"
    arch="$(uname -m)"
    case "$os" in
        Darwin) os_label=darwin ;;
        Linux)
            if is_musl; then
                die "musl libc (e.g. Alpine) is not supported; the binaries are built for glibc. Use a glibc-based system, or build from source."
            fi
            os_label=linux
            ;;
        MINGW* | MSYS* | CYGWIN*)
            die "Windows shells (Git Bash / MSYS2 / Cygwin) are not supported. Use WSL2, or build from source." ;;
        *)
            die "unsupported operating system '$os'. Supported: macOS (Darwin), Linux (glibc). Build from source for others." ;;
    esac
    case "$arch" in
        arm64 | aarch64) arch_label=arm64 ;;
        x86_64 | amd64) arch_label=x64 ;;
        *)
            die "unsupported CPU architecture '$arch'. Supported: arm64/aarch64, x86_64/amd64. Build from source for others." ;;
    esac
    platform="${os_label}-${arch_label}"
    # Membership against the one declaration, rather than a second list of pairs written out here. The
    # two cases above map uname onto labels; whether that combination is actually built is a different
    # question, and this is the only place it is answered.
    case "
$SUPPORTED_PLATFORMS
" in
        *"
$platform
"*) ;;
        *) die "no build is published for $platform. Supported: $(echo "$SUPPORTED_PLATFORMS" | tr '\n' ' ')" ;;
    esac
}

# Download $1 to the file $2 with whichever of curl / wget is present.
#
# Attempts resume the partial rather than restarting it -- a link that drops once on a large download
# tends to drop again, and restarting from zero each time can make no progress at all. The transfer
# lands in $2.part and is moved into place only once it has completed, so an interrupted one leaves
# nothing at the real path. The last attempt starts clean, which is the only way past a server that
# cannot serve ranges, or a .part that is already complete because a run was killed between the
# transfer and the move.
#
# The quickstart carries this function verbatim, deliberately: the two are halves of one documented
# install path, and a transfer that survives a dropped link in one half but not the other is the same
# dead install to whoever ran the one-liner.
#
# It reports and returns non-zero rather than exiting, so the caller decides: under `set -e` an
# unguarded call ends the run exactly as it did before, and a caller that can do without the file
# keeps its own `|| ...`.
fetch() {
    _part="$2.part"
    _try=0
    while [ "$_try" -lt 3 ]; do
        _try=$((_try + 1))
        [ "$_try" -lt 3 ] || rm -f "$_part"
        if command -v curl >/dev/null 2>&1; then
            curl -fsSL --retry 2 -C - "$1" -o "$_part" && { mv -f "$_part" "$2"; return 0; }
        elif command -v wget >/dev/null 2>&1; then
            wget -q -c "$1" -O "$_part" && { mv -f "$_part" "$2"; return 0; }
        else
            die "neither curl nor wget is available to download $1."
        fi
    done
    printf 'install: could not download %s -- %s attempts, the last from scratch.\n' "$1" "$_try" >&2
    # Only when there is one. A transfer that never opened leaves no .part, and promising a
    # resume of a file that is not there sends the reader looking for it.
    [ ! -f "$_part" ] || printf 'install: what did arrive is kept at %s, so a later run resumes it.\n' "$_part" >&2
    return 1
}

# The version to install: the caller's override, or the pin above. The /releases/latest redirect is
# deliberately not consulted -- it names only full releases, so it cannot see a prerelease at all,
# and a default that works or dies depending on how the newest release was flagged is not a default.
resolve_version() {
    version="${TAPSTATE_VERSION:-$PINNED_VERSION}"
    [ -n "$version" ] || die "no version to install; set TAPSTATE_VERSION or fix the PINNED_VERSION line."
}

# True when $1 is a dotted decimal version and nothing else, so the comparison below never feeds a word
# to an integer test. Anything else is treated as unparseable and leaves the check skipped.
is_dotted_number() {
    case "$1" in
        '' | *[!0-9.]*) return 1 ;;
        *) return 0 ;;
    esac
}

# True when dotted version $1 is at least $2, compared field by field as integers. A string comparison
# would put 26.1 below 15.0 and refuse exactly the machines a newer build generation targets. Absent
# fields count as zero, so 15 and 15.0.0 compare equal.
version_ge() {
    h="$1"
    n="$2"
    while [ -n "$h" ] || [ -n "$n" ]; do
        case "$h" in *.*) hf="${h%%.*}"; h="${h#*.}" ;; *) hf="$h"; h="" ;; esac
        case "$n" in *.*) nf="${n%%.*}"; n="${n#*.}" ;; *) nf="$n"; n="" ;; esac
        [ -n "$hf" ] || hf=0
        [ -n "$nf" ] || nf=0
        if [ "$hf" -gt "$nf" ]; then
            return 0
        fi
        if [ "$hf" -lt "$nf" ]; then
            return 1
        fi
    done
    return 0
}

# Say so when this machine is older than the release was built for, then install anyway. A native binary
# is tied to the machine that built it -- on macOS by the deployment target it is stamped with, on Linux
# by the newest glibc symbols it references -- and the hosted build machines move between releases, so
# the recommended version belongs to the release rather than to this script. It is read from the
# release's own platform-minimums.txt, whose lines are "<platform> <requirement> <version>"; the
# requirement names which check applies, so a release can add one this installer has never heard of and
# older installers simply pass it by.
#
# This is a notice, not a refusal, and the difference is deliberate. The platforms refused above have no
# binary at all -- there is nothing to install and no choice to make. Here there is one, and whether to
# try it belongs to whoever is installing it. What they should not have to do is work out on their own
# why it did not launch, because that failure arrives from the loader at launch, far from the install
# that caused it. It goes to stderr: the demo bootstrap drops this script's stdout, and a notice nobody
# sees is not a notice. Anything that cannot be checked -- a release that publishes no such file, a
# requirement this installer does not know, an unreadable version -- says nothing at all, because a
# check that did not happen must not masquerade as one that passed.
note_recommended_platform() {
    # Stderr is dropped rather than shown: this file is optional, every word fetch would say about
    # failing to get it describes a normal install of a release that does not publish one.
    fetch "${base_url}/download/v${version}/platform-minimums.txt" "$tmp/minimums" 2>/dev/null || return 0
    [ -s "$tmp/minimums" ] || return 0
    kind="$(awk -v p="$platform" '$1 == p { print $2; exit }' "$tmp/minimums")"
    need="$(awk -v p="$platform" '$1 == p { print $3; exit }' "$tmp/minimums")"
    [ -n "$kind" ] || return 0
    [ -n "$need" ] || return 0
    case "$kind" in
        macos)
            command -v sw_vers >/dev/null 2>&1 || return 0
            have="$(sw_vers -productVersion)"
            label=macOS
            ;;
        glibc)
            # Here ldd's own version is exactly what is wanted: this machine's glibc, which is what the
            # binary will be resolved against. (It says nothing about what the binary needs -- that is
            # measured from the binary at build time and is the number being compared to.)
            command -v ldd >/dev/null 2>&1 || return 0
            have="$(ldd --version 2>&1 | head -1 | awk '{ print $NF }')"
            label=glibc
            ;;
        *)
            return 0
            ;;
    esac
    is_dotted_number "$need" || return 0
    is_dotted_number "$have" || return 0
    if version_ge "$have" "$need"; then
        return 0
    fi
    printf 'install: this release is built for %s %s or newer; this machine has %s, where it may not launch. Installing anyway.\n' \
        "$label" "$need" "$have" >&2
}

# Refuse to install unless the download's sha256 matches its published checksum. No tool = refuse, never skip.
verify_sha256() {
    file="$1"
    sumfile="$2"
    if command -v sha256sum >/dev/null 2>&1; then
        actual="$(sha256sum "$file" | awk '{print $1}')"
    elif command -v shasum >/dev/null 2>&1; then
        actual="$(shasum -a 256 "$file" | awk '{print $1}')"
    else
        die "no sha256 tool (sha256sum or shasum) is available; refusing to install an unverified download."
    fi
    expected="$(awk '{print $1}' "$sumfile")"
    [ -n "$expected" ] || die "the checksum file is empty or malformed; refusing to install."
    if [ "$expected" != "$actual" ]; then
        die "sha256 checksum mismatch for $(basename "$file") (expected $expected, got $actual); refusing to install."
    fi
}

# Extract and install a complete bundle. The version directory is fully populated before the stable
# symlink changes, so an MCP host never observes a CLI without its sibling sidecar.
install_bundle() {
    tar -xzf "$tmp/$asset" -C "$tmp"
    bundle_root="$(find "$tmp" -mindepth 1 -maxdepth 1 -type d -name 'tapstate-cli-*' | head -n 1)"
    [ -n "$bundle_root" ] || die "the downloaded archive did not contain a tapstate CLI bundle."
    [ -x "$bundle_root/bin/tapstate" ] \
        || die "the downloaded bundle did not contain an executable bin/tapstate."
    if [ ! -x "$bundle_root/libexec/tapstate-mcp" ] \
       && [ ! -f "$bundle_root/libexec/tapstate-mcp.jar" ]; then
        die "the downloaded bundle did not contain an MCP sidecar."
    fi
    mkdir -p "$install_dir"
    mkdir -p "$install_dir/versions"
    staged="$install_dir/versions/.tapstate-$version.$$"
    mkdir "$staged"
    cp -R "$bundle_root/bin" "$staged/bin"
    cp -R "$bundle_root/libexec" "$staged/libexec"
    [ ! -f "$bundle_root/LICENSE" ] || cp "$bundle_root/LICENSE" "$staged/LICENSE"
    [ ! -f "$bundle_root/NOTICE" ] || cp "$bundle_root/NOTICE" "$staged/NOTICE"
    final="$install_dir/versions/$version"
    if [ -d "$final" ] \
       && [ -x "$final/bin/tapstate" ] \
       && { [ -x "$final/libexec/tapstate-mcp" ] || [ -f "$final/libexec/tapstate-mcp.jar" ]; }; then
        rm -rf "$staged"
    else
        if [ -e "$final" ] || [ -L "$final" ]; then
            rm -rf "$final"
        fi
        mv "$staged" "$final"
    fi
    staged=""
    staged_link="$install_dir/.tapstate.$$"
    ln -s "versions/$version/bin/tapstate" "$staged_link"
    mv -f "$staged_link" "$install_dir/tapstate"
    staged_link=""
    install_alias "$install_dir" "$version"
}

# `tap` is a convenience shortcut, never a second command: `tapstate` is what every document, message
# and completion says, and this only saves keystrokes. It is a link to the same target the stable entry
# points at, so an upgrade moves both together -- a copy would go on pointing at a version directory
# this script has already deleted.
#
# A `tap` that belongs to someone else (node-tap ships one) is left exactly as it is, and the skip is
# said out loud: a silent one leaves the user without the shortcut and without a reason, which reads as
# the installer having failed at something.
install_alias() {
    alias_dir="$1"
    alias_version="$2"

    # The local name is decided first, and on what it is rather than on what is on PATH. Two failures
    # come from asking PATH alone. A `tap` sitting in the install directory that belongs to someone
    # else is invisible to `command -v` whenever that directory is not on PATH -- and it was then
    # overwritten by the mv below. And an upgrade of our own alias was skipped whenever any other tap
    # happened to precede it on PATH, leaving the shortcut pointing into a version directory this
    # script is about to delete.
    ours=no
    if [ -L "$alias_dir/tap" ]; then
        # Ours by where it points, not by what the target is called: the link is written relative to
        # the install directory, so a target that stays inside it is one this script wrote.
        # A glob is not a path check: `versions/../../other/bin/tapstate` matches the pattern and
        # resolves outside this directory entirely, so a crafted link would be adopted and replaced.
        # Anything containing `..` is refused before the shape is even considered -- the links this
        # script writes never need one.
        alias_target="$(readlink "$alias_dir/tap")"
        case "$alias_target" in
            *..*) ours=no ;;
            versions/*/bin/tapstate | tapstate) ours=yes ;;
        esac
    fi
    if [ -e "$alias_dir/tap" ] || [ -L "$alias_dir/tap" ]; then
        if [ "$ours" = no ]; then
            # shellcheck disable=SC2016
            printf 'note: skipping the optional `tap` shortcut -- %s already exists and is not ours.\n' "$alias_dir/tap"
            # shellcheck disable=SC2016
            printf '      tapstate is installed and unaffected; remove that file and run `tapstate alias install` to reconsider.\n'
            return 0
        fi
    else
        # The name is free here, so PATH decides: a tap somewhere else on PATH would shadow the one
        # this would create, and a shortcut that resolves to someone else's command is worse than none.
        existing="$(command -v tap 2>/dev/null || true)"
        if [ -n "$existing" ]; then
            # shellcheck disable=SC2016
            printf 'note: skipping the optional `tap` shortcut -- a different tap is already on PATH at %s.\n' "$existing"
            # shellcheck disable=SC2016
            printf '      tapstate is installed and unaffected; run `tapstate alias install` later to reconsider.\n'
            return 0
        fi
    fi
    staged_alias="$alias_dir/.tap.$$"
    ln -s "versions/$alias_version/bin/tapstate" "$staged_alias"
    mv -f "$staged_alias" "$alias_dir/tap"
    staged_alias=""
}

main() {
    base_url="${TAPSTATE_BASE_URL:-https://github.com/tapstate/tapstate/releases}"
    install_dir="${TAPSTATE_INSTALL_DIR:-$HOME/.tapstate/bin}"

    # Before detect_platform: listing what is published is a question about the release, not about the
    # machine asking, and it has to be answerable from a machine no build is published for.
    if [ "${1:-}" = --print-platforms ]; then
        printf '%s\n' "$SUPPORTED_PLATFORMS"
        return
    fi
    detect_platform
    # Detect-only mode: print the <os>-<arch> tuple and stop. detect_platform has already refused any
    # unsupported platform above, so this doubles as a zero-side-effect platform gate -- the demo
    # bootstrap calls it before downloading anything, and shares this one copy of the mapping.
    if [ "${1:-}" = --print-platform ]; then
        printf '%s\n' "$platform"
        return
    fi
    resolve_version
    asset="tapstate-${version}-${platform}.tar.gz"
    url="${base_url}/download/v${version}/${asset}"

    tmp="$(mktemp -d)"
    staged=""
    staged_link=""
    # The alias is staged under a temporary name too, so it is cleaned up on the same terms as the other
    # two. Left out, an interrupt between its `ln -s` and its `mv` strands a dot-file link in a directory
    # the user keeps -- and unlike the work area below, nothing else ever removes it.
    staged_alias=""
    trap 'rm -rf "$tmp" ${staged:+"$staged"} ${staged_link:+"$staged_link"} ${staged_alias:+"$staged_alias"}' EXIT INT TERM

    note_recommended_platform
    fetch "$url" "$tmp/$asset"
    fetch "${url}.sha256" "$tmp/${asset}.sha256"
    verify_sha256 "$tmp/$asset" "$tmp/${asset}.sha256"
    install_bundle

    printf 'tapstate %s installed to %s/tapstate\n' "$version" "$install_dir"
    on_path=yes
    case ":${PATH}:" in
        *":${install_dir}:"*) : ;;
        *)
            on_path=no
            # The hint matches the user's shell: fish spells PATH additions its own way, and a pasted
            # line that errors teaches a user the installer is careless. Everything else POSIX-ish
            # shares the export form. $PATH stays literal — the user pastes this into their own shell.
            # shellcheck disable=SC2016
            case "$(basename "${SHELL:-sh}")" in
                fish) printf 'not on PATH; run it directly, or:  fish_add_path %s\n' "$install_dir" ;;
                *) printf 'not on PATH; run it directly, or:  export PATH="%s:$PATH"\n' "$install_dir" ;;
            esac
            ;;
    esac

    # Next steps, so the install ends at the start of something rather than at a file on disk. The
    # authoring loop below is offline and complete as installed; running a real pipeline needs the
    # server, which is the quickstart's business, so the pointer goes there instead of overpromising.
    if [ "$on_path" = yes ]; then run_as=tapstate; else run_as="$install_dir/tapstate"; fi
    printf '\nnext:\n'
    printf '  %s --version\n' "$run_as"
    printf '  %s new --kind source --id my_db --connector mysql   # scaffold, then validate:\n' "$run_as"
    printf '  %s validate\n' "$run_as"
    printf 'to run a real pipeline (server + databases): see docs/quickstart-online.md in the repository\n'
    printf 'to uninstall: rm -rf ~/.tapstate  (and drop the PATH line if you added one)\n'
}

main "$@"
