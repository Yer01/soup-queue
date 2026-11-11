#!/usr/bin/env sh
# Run the chaos harness once per seed. Raw output of every run, passing or
# failing, stays in runs/<run-id>/ and one line per run is appended to
# runs/index.tsv. Nothing is ever deleted.
#
# Usage: scripts/chaos.sh [seeds...] [-- harness flags]
#   scripts/chaos.sh 1 2 3 4 5
#   scripts/chaos.sh 7 -- -fault nofence
#   scripts/chaos.sh 8 -- -rate 1000
set -u

seeds=""
while [ $# -gt 0 ] && [ "$1" != "--" ]; do
  seeds="$seeds $1"
  shift
done
[ "${1:-}" = "--" ] && shift
[ -z "$seeds" ] && seeds="1"

docker compose up -d --build --wait db api || exit 2
# `compose run` reuses an existing image and never rebuilds it, so build the
# harness image explicitly. (Without this line, eight runs used a harness
# binary older than the source; each report now prints the binaries' hashes.)
docker compose --profile chaos build chaos || exit 2
status=0
for s in $seeds; do
  # The harness runs its own workers; the compose ones would steal jobs.
  # Stop them before every seed, not just once: they were restarted in the
  # middle of a batch once, and four runs failed check W1. The harness also
  # refuses to start while any other worker is connected.
  docker compose stop worker >/dev/null 2>&1
  echo "=== seed $s $* ==="
  docker compose --profile chaos run --rm chaos -seed "$s" "$@" || status=1
done
exit $status
