#!/usr/bin/env bash
set -euo pipefail

wait_for() {
  local host=$1 port=$2
  echo "[argus] waiting for ${host}:${port} ..."
  until nc -z "$host" "$port"; do sleep 2; done
}

case "${1:-}" in
  namenode)
    if [ ! -d /hadoop/dfs/name/current ]; then
      echo "[argus] formatting NameNode (first start)"
      hdfs namenode -format -nonInteractive -force -clusterId argus-cluster
    fi
    exec hdfs namenode
    ;;
  datanode)
    wait_for namenode 8020
    exec hdfs datanode
    ;;
  resourcemanager)
    wait_for namenode 8020
    exec yarn resourcemanager
    ;;
  nodemanager)
    wait_for resourcemanager 8031
    exec yarn nodemanager
    ;;
  historyserver)
    wait_for namenode 8020
    exec mapred historyserver
    ;;
  *)
    exec "$@"
    ;;
esac
