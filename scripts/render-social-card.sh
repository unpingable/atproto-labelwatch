#!/bin/sh
set -eu
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
browser=${CHROME:-google-chrome}
expected='Google Chrome 143.0.7499.109'
actual=$($browser --version | sed 's/[[:space:]]*$//')
test "$actual" = "$expected" || { echo "renderer mismatch: expected $expected, got $actual" >&2; exit 1; }
profile=$(mktemp -d)
trap 'rm -rf "$profile"' EXIT
"$browser" --headless=new --hide-scrollbars --disable-gpu --no-sandbox --user-data-dir="$profile" --window-size=1200,630 --screenshot="$root/src/labelwatch/_instruments/social-card-v1.png" "file://$root/assets/social-card.src.html"
test "$(file -b "$root/src/labelwatch/_instruments/social-card-v1.png")" = 'PNG image data, 1200 x 630, 8-bit/color RGB, non-interlaced'
sha256sum "$root/src/labelwatch/_instruments/social-card-v1.png"
