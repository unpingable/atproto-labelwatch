#!/usr/bin/env bash
# Install (never activate) the recent observatory units for one release.
#
#   sudo deploy/install-recent.sh --release-sha <sha> --admitted-root <dir> \
#        --pubdir <dir> --reserves '<json>' [--round-seconds 30] [--store-name epoch-2]
#
# Preconditions: /opt/labelwatch/releases/<sha>/source (git archive of <sha>)
# and /opt/labelwatch/releases/<sha>/venv (release/build-venv.sh) exist; the
# labelwatch user exists. The script installs exactly the labelwatch-recent-*
# units by name (never the legacy units that also live in deploy/), writes
# /etc/labelwatch-recent.env, creates the admitted root and the publication
# directory, runs daemon-reload and systemd-analyze verify. It creates no
# marker, enables nothing and starts nothing: activation is a separate owner
# act (deploy/README-recent.md "Hold and activation markers"). Re-running with
# a new sha replaces the unit files and env in place; the store is untouched.
set -euo pipefail
sha=""; root=""; pubdir=""; reserves=""; round=30; store_name=epoch-2
while [ $# -gt 0 ]; do
  case "$1" in
    --release-sha) sha=$2; shift 2;; --admitted-root) root=$2; shift 2;; --pubdir) pubdir=$2; shift 2;;
    --reserves) reserves=$2; shift 2;; --round-seconds) round=$2; shift 2;; --store-name) store_name=$2; shift 2;;
    *) echo "unknown argument $1" >&2; exit 2;;
  esac
done
[ -n "$sha" ] && [ -n "$root" ] && [ -n "$pubdir" ] && [ -n "$reserves" ] || { echo "usage: --release-sha --admitted-root --pubdir --reserves required" >&2; exit 2; }
[ "$(id -u)" = 0 ] || { echo "run as root" >&2; exit 2; }
rel=/opt/labelwatch/releases/$sha
[ -d "$rel/source/deploy" ] && [ -x "$rel/venv/bin/python" ] || { echo "release $rel incomplete (source/ and venv/ required)" >&2; exit 2; }
case "$root" in /*) ;; *) echo "admitted root must be absolute" >&2; exit 2;; esac
case "$pubdir" in /*) ;; *) echo "pubdir must be absolute" >&2; exit 2;; esac
id labelwatch >/dev/null 2>&1 || { echo "user labelwatch missing" >&2; exit 2; }
# The same system-prefix refusals the store applies at init, applied before anything is written.
for d in "$root" "$pubdir"; do
  case "$d" in
    /|/etc|/etc/*|/usr|/usr/*|/bin|/bin/*|/sbin|/sbin/*|/lib*|/boot|/boot/*|/proc|/proc/*|/sys|/sys/*|/dev|/dev/*|/run|/run/*|/root|/root/*|/srv|/srv/*|/var/lib/labelwatch/generations|/var/lib/labelwatch/generations/*|/var/lib/labelwatch/cold|/var/lib/labelwatch/cold/*|/home/*/.config|/home/*/.config/*)
      echo "refused system prefix: $d" >&2; exit 2;;
  esac
done
case "$root" in /var/www|/var/www/*) echo "admitted root may not live under /var/www" >&2; exit 2;; esac
[ "$root" != "$pubdir" ] || { echo "admitted root and pubdir must differ" >&2; exit 2; }
"$rel/venv/bin/python" -c "import json,sys; p=json.loads(sys.argv[1]); assert isinstance(p,list) and p and all(set(e)=={'mount','bytes'} for e in p)" "$reserves" || { echo "reserves must be a JSON list of {mount,bytes}" >&2; exit 2; }

# Dependency identity of the venv, exactly as the collect unit will check it.
gate=$(sed -n 's/^ExecStartPre=[^ ]* -c "\(.*\)"$/\1/p' "$rel/source/deploy/labelwatch-recent-collect.service")
[ -n "$gate" ] || { echo "collect unit has no ExecStartPre dependency gate" >&2; exit 3; }
"$rel/venv/bin/python" -c "$gate" || { echo "venv fails the unit dependency gate" >&2; exit 3; }

install -d -m 0750 -o labelwatch -g labelwatch "$root"
install -d -m 0755 -o labelwatch -g labelwatch "$pubdir"

sub() { sed -e "s#REPLACE_WITH_RELEASE_SHA#$sha#g" -e "s#/REPLACE_WITH_ADMITTED_RECENT_ROOT#$root#g" -e "s#/REPLACE_WITH_PUBLIC_OVERVIEW_DIR#$pubdir#g" "$1"; }
tmp=$(mktemp); trap 'rm -f "$tmp"' EXIT
sub "$rel/source/deploy/labelwatch-recent.env.example" \
  | sed -e "s#^LABELWATCH_RECENT_STORE=.*#LABELWATCH_RECENT_STORE=$root/$store_name#" \
        -e "s#^LABELWATCH_RECENT_RESERVES=.*#LABELWATCH_RECENT_RESERVES=$reserves#" \
        -e "s#^LABELWATCH_RECENT_ROUND_SECONDS=.*#LABELWATCH_RECENT_ROUND_SECONDS=$round#" > "$tmp"
grep -q REPLACE_WITH "$tmp" && { echo "unsubstituted placeholder in env" >&2; grep REPLACE_WITH "$tmp" >&2; exit 3; }
install -m 0640 -o root -g labelwatch "$tmp" /etc/labelwatch-recent.env

units="labelwatch-recent-collect.service labelwatch-recent-collect.timer labelwatch-recent-maintain.service labelwatch-recent-maintain.timer labelwatch-recent-overview.service labelwatch-recent-overview.timer labelwatch-recent-product.service"
stage=$(mktemp -d); trap 'rm -rf "$tmp" "$stage"' EXIT
for u in $units; do
  sub "$rel/source/deploy/$u" > "$stage/$u"
  grep -q REPLACE_WITH "$stage/$u" && { echo "unsubstituted placeholder in $u" >&2; exit 3; }
done
# All seven rendered; now place them together so a half-installed set never exists.
for u in $units; do install -m 0644 -o root -g root "$stage/$u" "/etc/systemd/system/$u"; done
systemctl daemon-reload
# Advisory: verify reports dependency/ordering remarks, not acceptance; failures above are the gate.
for u in $units; do systemd-analyze verify "/etc/systemd/system/$u" 2>&1 | grep -v "^$" | sed "s#^#verify (advisory) $u: #" | grep -v "Unit .* is not loaded properly\|Cannot add dependency" || true; done
for u in $units; do printf '%-40s %s %s\n' "$u" "$(systemctl show "$u" -p LoadState --value)" "$(systemctl show "$u" -p UnitFileState --value)"; done
echo "installed release $sha; no marker created, nothing enabled or started"
