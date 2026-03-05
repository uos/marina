#!/usr/bin/env bash
# Usage: ./bump-version.sh <new-version>
# Example: ./bump-version.sh 0.1.0

set -euo pipefail

NEW_VERSION="${1:-}"
if [[ -z "$NEW_VERSION" ]]; then
    echo "Usage: $0 <new-version>"
    echo "Example: $0 0.1.0"
    exit 1
fi

if ! [[ "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9._-]+)?(\+[a-zA-Z0-9._-]+)?$ ]]; then
    echo "Error: version must be semver format (e.g. 1.2.3 or 1.2.3-alpha.1)"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if sed --version 2>/dev/null | grep -q GNU; then
    SED_INPLACE=(-i)
else
    SED_INPLACE=(-i '')
fi

# binary
CURRENT_VERSION="$(grep -m1 '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/')"
echo "main:            $CURRENT_VERSION -> $NEW_VERSION"
sed "${SED_INPLACE[@]}" "s/^version = \"${CURRENT_VERSION}\"/version = \"${NEW_VERSION}\"/" Cargo.toml
sed "${SED_INPLACE[@]}" "s/^version = ${CURRENT_VERSION}$/version = ${NEW_VERSION}/" setup.cfg
sed "${SED_INPLACE[@]}" "s|<version>${CURRENT_VERSION}</version>|<version>${NEW_VERSION}</version>|" package.xml
sed "${SED_INPLACE[@]}" "s/^pkgver=${CURRENT_VERSION}$/pkgver=${NEW_VERSION}/" packaging/arch/PKGBUILD

# python
BINDINGS_CARGO="bindings/python/Cargo.toml"
BINDINGS_PYPROJECT="bindings/python/pyproject.toml"
CURRENT_BINDINGS_VERSION="$(grep -m1 '^version = ' "$BINDINGS_CARGO" | sed 's/version = "\(.*\)"/\1/')"
echo "python bindings: $CURRENT_BINDINGS_VERSION -> $NEW_VERSION"
sed "${SED_INPLACE[@]}" "s/^version = \"${CURRENT_BINDINGS_VERSION}\"/version = \"${NEW_VERSION}\"/" "$BINDINGS_CARGO"
sed "${SED_INPLACE[@]}" "s/^version = \"${CURRENT_BINDINGS_VERSION}\"/version = \"${NEW_VERSION}\"/" "$BINDINGS_PYPROJECT"

echo ""
echo "── git diff ──────────────────────────────────────────────────────────────"
git diff
