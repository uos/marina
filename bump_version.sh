#!/usr/bin/env bash
# Usage: ./bump_version.sh <new-version>
# Example: ./bump_version.sh 0.2.0

set -euo pipefail

NEW_VERSION="${1:-}"
if [[ -z "$NEW_VERSION" ]]; then
    echo "Usage: $0 <new-version>"
    echo "Example: $0 0.2.0"
    exit 1
fi

if ! [[ "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9._-]+)?(\+[a-zA-Z0-9._-]+)?$ ]]; then
    echo "Error: version must be semver format (e.g. 1.2.3 or 1.2.3-alpha.1)"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

DETECTED_VERSION="$(grep -m1 '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/')"
echo "Bumping: $DETECTED_VERSION -> $NEW_VERSION"

# Portable sed -i
sed_i() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i "" "$@"
    else
        sed -i "$@"
    fi
}

# Cargo.toml (workspace)
sed_i "s/^version = \".*\"/version = \"${NEW_VERSION}\"/" Cargo.toml

# bindings/python/Cargo.toml
sed_i "s/^version = \".*\"/version = \"${NEW_VERSION}\"/" bindings/python/Cargo.toml

# bindings/python/pyproject.toml
sed_i "s/^version = \".*\"/version = \"${NEW_VERSION}\"/" bindings/python/pyproject.toml

# package.xml
sed_i -E "s|<version>.*</version>|<version>${NEW_VERSION}</version>|" package.xml

# setup.cfg
sed_i "s/^version = .*/version = ${NEW_VERSION}/" setup.cfg

# packaging/arch/PKGBUILD
sed_i "s/^pkgver=.*/pkgver=${NEW_VERSION}/" packaging/arch/PKGBUILD

# packaging/arch-bin/PKGBUILD
sed_i "s/^pkgver=.*/pkgver=${NEW_VERSION}/" packaging/arch-bin/PKGBUILD

# packaging/homebrew/marina.rb
sed_i "s/version \".*\"/version \"${NEW_VERSION}\"/" packaging/homebrew/marina.rb

# docs/docs/installation/archives.md (Bash/Zsh: export MARINA_VERSION="x.y.z", Fish: set -gx MARINA_VERSION x.y.z)
sed_i 's/export MARINA_VERSION="[^"]*"/export MARINA_VERSION="'"${NEW_VERSION}"'"/' docs/docs/installation/archives.md
sed_i 's/set -gx MARINA_VERSION "*[0-9][^" ]*/set -gx MARINA_VERSION '"${NEW_VERSION}"'/' docs/docs/installation/archives.md

# flake.nix
sed_i 's/version = "[0-9]*\.[0-9]*\.[0-9]*";/version = "'"${NEW_VERSION}"'";/' flake.nix

echo ""
echo "Updated files:"
echo "  Cargo.toml"
echo "  bindings/python/Cargo.toml"
echo "  bindings/python/pyproject.toml"
echo "  package.xml"
echo "  setup.cfg"
echo "  packaging/arch/PKGBUILD"
echo "  packaging/arch-bin/PKGBUILD"
echo "  packaging/homebrew/marina.rb"
echo "  docs/docs/installation/archives.md"
echo "  flake.nix"
echo ""
echo "── git diff ──────────────────────────────────────────────────────────"
git diff
