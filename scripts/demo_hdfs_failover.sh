#!/usr/bin/env bash
# Unit II demo: HDFS keeps serving data when a DataNode dies, then heals itself.
set -euo pipefail
cd "$(dirname "$0")/.."

C="docker compose"
F=/argus/raw/accidents/accidents.csv
VICTIM=${VICTIM:-datanode2}
nn() { $C exec -T namenode "$@"; }
step() { printf "\n\033[1;36m== %s\033[0m\n" "$*"; }
live_dead() { nn hdfs dfsadmin -report 2>/dev/null | grep -E "^(Live|Dead) datanodes" | tr '\n' ' '; echo; }
under_rep() { nn hdfs fsck / 2>/dev/null | grep -i "under.replicated blocks" | head -1 | awk -F: '{print $2}' | awk '{print $1}'; }

step "1. Healthy cluster"
live_dead
step "2. Where do the blocks of $F live? (replication = 2)"
nn hdfs fsck "$F" -files -blocks -locations 2>/dev/null | grep -E "^[0-9]+\. BP" | head -5

step "3. Killing $VICTIM"
$C stop "$VICTIM"
echo "NameNode marks it dead after ~60s of missed heartbeats (dfs.heartbeat.interval=3s)..."
for _ in $(seq 1 30); do
  if nn hdfs dfsadmin -report 2>/dev/null | grep -qE "^Dead datanodes \([1-9]"; then break; fi
  sleep 5
done
live_dead

step "4. The file is still fully readable"
nn hdfs dfs -tail "$F" | tail -n 2 | cut -c1-120

step "5. Re-replication: under-replicated block count should fall to 0"
for i in $(seq 1 36); do
  n=$(under_rep); echo "  t+$((i*5))s under-replicated blocks: ${n:-?}"
  [ "${n:-1}" = "0" ] && break
  sleep 5
done

step "6. Bringing $VICTIM back"
$C start "$VICTIM"
sleep 10
live_dead
echo "Over-replicated copies are trimmed automatically by the NameNode."
