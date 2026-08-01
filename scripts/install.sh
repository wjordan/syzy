#!/bin/sh
# Install the syzy CLIs and SQLite loadable extension.
#
#   curl -fsSL https://github.com/wjordan/syzy/releases/latest/download/install.sh | sh
#
# Environment:
#   SYZY_VERSION   version to install (default: latest)
#   SYZY_PREFIX    install prefix (default: /usr/local)
#
# The SQLite CLI and extension are two halves of one build and refuse to
# talk to each other across versions, so this always installs both.
set -eu

REPO=wjordan/syzy
PREFIX=${SYZY_PREFIX:-/usr/local}
VERSION=${SYZY_VERSION:-latest}

die() { echo "install: $*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "$1 is required"; }
need uname
need tar

if command -v curl >/dev/null 2>&1; then
	fetch() { curl -fsSL "$1" -o "$2"; }
elif command -v wget >/dev/null 2>&1; then
	fetch() { wget -qO "$2" "$1"; }
else
	die "curl or wget is required"
fi

# coreutils vs BSD/perl; one of the two is present everywhere this runs.
if command -v sha256sum >/dev/null 2>&1; then
	sha256() { sha256sum "$1"; }
elif command -v shasum >/dev/null 2>&1; then
	sha256() { shasum -a 256 "$1"; }
else
	die "sha256sum or shasum is required to verify the download"
fi

case "$(uname -s)" in
	Linux)  os=linux;  ext_name=syzy.so ;;
	Darwin) os=darwin; ext_name=syzy.dylib ;;
	*) die "unsupported OS $(uname -s); syzy supports Linux and macOS" ;;
esac

case "$(uname -m)" in
	x86_64|amd64) arch=amd64 ;;
	arm64|aarch64) arch=arm64 ;;
	*) die "unsupported architecture $(uname -m)" ;;
esac

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT INT TERM

# The version is part of the archive name, so "latest" has to be
# resolved to a concrete tag before anything can be downloaded.
if [ "$VERSION" = latest ]; then
	need sed
	fetch "https://api.github.com/repos/$REPO/releases/latest" "$tmp/latest.json" ||
		die "could not reach the GitHub API; set SYZY_VERSION to install a specific version"
	VERSION=$(sed -n 's/.*"tag_name"[ ]*:[ ]*"\([^"]*\)".*/\1/p' "$tmp/latest.json" | head -1)
	[ -n "$VERSION" ] || die "could not determine the latest version; set SYZY_VERSION"
fi

base="https://github.com/$REPO/releases/download/$VERSION"
name="syzy_${VERSION}_${os}_${arch}"
echo "installing syzy $VERSION ($os/$arch) into $PREFIX"

fetch "$base/$name.tar.gz" "$tmp/$name.tar.gz" || die "no release asset for $os/$arch at $VERSION"

# One SHA256SUMS covers every platform's archive. Verification is not
# optional: this script is piped from the network into a shell, and a
# missing or unmatched checksum means the release is not what we think.
fetch "$base/SHA256SUMS" "$tmp/SHA256SUMS" || die "no SHA256SUMS published for $VERSION"
want=$(grep " $name.tar.gz\$" "$tmp/SHA256SUMS") || die "no checksum published for $name.tar.gz"
got=$(cd "$tmp" && sha256 "$name.tar.gz")
[ "${want%% *}" = "${got%% *}" ] || die "checksum mismatch for $name.tar.gz"

tar -C "$tmp" -xzf "$tmp/$name.tar.gz"

# Writing under /usr/local needs root; a prefix the user owns does not.
# $PREFIX often does not exist yet (SYZY_PREFIX=$HOME/.local on a fresh
# account), and testing -w on a missing directory always fails — which
# would escalate to sudo and leave root-owned files in the user's home.
# Ask the nearest existing ancestor instead, since that is what we
# actually have to create inside.
probe=$PREFIX
while [ ! -e "$probe" ]; do
	parent=$(dirname "$probe")
	[ "$parent" != "$probe" ] || break
	probe=$parent
done
if [ "$(id -u)" = 0 ] || [ -w "$probe" ]; then
	sudo=""
elif command -v sudo >/dev/null 2>&1; then
	sudo="sudo"
	echo "install: $PREFIX needs root; using sudo (set SYZY_PREFIX=\$HOME/.local to avoid)"
else
	die "$PREFIX is not writable and sudo is unavailable; set SYZY_PREFIX"
fi

$sudo mkdir -p "$PREFIX/bin" "$PREFIX/lib"
$sudo install -m 0755 "$tmp/$name/syzy" "$PREFIX/bin/syzy"
$sudo install -m 0755 "$tmp/$name/syzy-pg" "$PREFIX/bin/syzy-pg"
$sudo install -m 0644 "$tmp/$name/$ext_name" "$PREFIX/lib/$ext_name"
if [ -f "$tmp/$name/syzy-shim.so" ]; then
	$sudo install -m 0644 "$tmp/$name/syzy-shim.so" "$PREFIX/lib/syzy-shim.so"
fi

# `.load syzy` resolves through the dynamic loader, which on Linux reads
# a cache that a fresh file is not in yet.
if [ "$os" = linux ] && command -v ldconfig >/dev/null 2>&1; then
	$sudo ldconfig 2>/dev/null || true
fi

echo
"$PREFIX/bin/syzy" version
"$PREFIX/bin/syzy-pg" version

# The bare `.load syzy` in the docs only works when the loader searches
# the install prefix. Say so plainly rather than letting the first
# command in the README fail.
if [ "$PREFIX" != /usr/local ]; then
	echo
	echo "note: installed outside /usr/local, so the loader will not find the"
	echo "extension by name. Load it by path instead:"
	echo "    sqlite3 -cmd '.load $PREFIX/lib/syzy' app.db"
fi
case ":$PATH:" in
	*":$PREFIX/bin:"*) ;;
	*) echo; echo "note: $PREFIX/bin is not on your PATH." ;;
esac
