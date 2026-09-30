#!/usr/bin/env bash
# Unit V demo (CAP / BASE): kill the MongoDB primary while a client is writing.
# The replica set chooses Consistency over Availability during the election:
# majority writes pause for a few seconds, then resume on the new primary - none are lost.
set -euo pipefail
cd "$(dirname "$0")/.."

C="docker compose"
step() { printf "\n\033[1;36m== %s\033[0m\n" "$*"; }
members() {
  for m in mongo1 mongo2 mongo3; do
    out=$($C exec -T "$m" mongosh --quiet --eval \
      'rs.status().members.map(x => x.name + " " + x.stateStr).join("\n")' 2>/dev/null) && { echo "$out"; return; }
  done
}
primary() { members | awk '$2=="PRIMARY"{split($1,a,":"); print a[1]}'; }

step "1. Replica set before"
members
OLD=$(primary)
echo "primary: $OLD"

step "2. Start a client writing every 100 ms with writeConcern=majority (45 s)"
docker rm -f argus-mongo-writer >/dev/null 2>&1 || true
$C run -d --no-deps --name argus-mongo-writer dashboard python scripts/mongo_writer.py --seconds 45 >/dev/null
sleep 8

step "3. Killing the primary ($OLD)"
t0=$(date +%s)
$C stop "$OLD"
until NEW=$(primary) && [ -n "$NEW" ] && [ "$NEW" != "$OLD" ]; do sleep 1; done
echo "new primary: $NEW (elected in ~$(( $(date +%s) - t0 ))s)"
members

step "4. Client's view of the outage"
docker wait argus-mongo-writer >/dev/null 2>&1 || true
docker logs argus-mongo-writer 2>&1 | tail -n 8
docker rm argus-mongo-writer >/dev/null

step "5. Restarting $OLD - it rejoins as SECONDARY, catches up, and (priority 2) may reclaim PRIMARY"
$C start "$OLD"
sleep 15
members
