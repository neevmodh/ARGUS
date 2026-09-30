"""ARGUS batch layer, orchestrated daily.

build jar -> ingest to HDFS -> MapReduce x5 -> publish MR to MongoDB
                            -> Spark clean  -> Hive partition refresh
                                            -> 5 ML jobs (K-Means, Tree, LR, FP-Growth, SciPy)

Every task runs in a throwaway container from the project's own images (DockerOperator),
talking to Docker through a read-only socket proxy - Airflow never touches the raw socket.
Expects the cluster to be up: `make up-all`.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

ARGUS_HOME = os.environ["ARGUS_HOME"]
DOCKER_URL = os.environ.get("DOCKER_URL", "tcp://docker-proxy:2375")
NETWORK = "argus-net"
JAR = "/opt/argus/mapreduce/target/argus-mr.jar"
RAW = "/argus/raw/accidents"
MR = "/argus/mr"

code = Mount(source=ARGUS_HOME, target="/opt/argus", type="bind", read_only=True)
data = Mount(source=f"{ARGUS_HOME}/data", target="/data", type="bind", read_only=True)


def container(task_id, image, command, mounts=(code, data), **kw):
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        docker_url=DOCKER_URL,
        network_mode=NETWORK,
        mounts=list(mounts),
        mount_tmp_dir=False,
        auto_remove="success",
        working_dir="/opt/argus",
        **kw,
    )


def hadoop(task_id, command):
    return container(task_id, "argus/hadoop:3.4.1", ["bash", "-c", command])


def spark(task_id, job, *args):
    return container(
        task_id,
        "argus/spark:3.5.6",
        ["argus-submit", job, *args],
        environment={
            "ARGUS_HDFS": "hdfs://namenode:8020",
            "MLFLOW_TRACKING_URI": "http://mlflow:5000",
            "MLFLOW_HTTP_REQUEST_MAX_RETRIES": "2",
            "PYTHONPATH": "/opt/argus/src",
            # Two ML jobs can share the 4-core worker instead of queueing.
            "ARGUS_SPARK_OPTS": "--conf spark.cores.max=2 --executor-memory 1500m",
        },
    )


with DAG(
    dag_id="argus_batch_layer",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    params={"data_file": Param("sample/us_accidents_sample.csv", type="string",
                               description="CSV path under ./data (e.g. raw/US_Accidents_March23.csv)")},
    tags=["argus", "batch"],
    doc_md=__doc__,
) as dag:
    build_jar = container(
        "build_mapreduce_jar",
        "maven:3.9-eclipse-temurin-17",
        ["mvn", "-q", "-B", "-f", "/src/pom.xml", "package", "-DskipTests"],
        mounts=[Mount(source=f"{ARGUS_HOME}/mapreduce", target="/src", type="bind"),
                Mount(source="argus-m2", target="/root/.m2", type="volume")],
    )

    ingest = hadoop(
        "hdfs_ingest",
        f"hdfs dfs -mkdir -p {RAW} && "
        f"hdfs dfs -put -f /data/{{{{ params.data_file }}}} {RAW}/accidents.csv && "
        f"hdfs dfs -ls -h {RAW}",
    )

    mr_jobs = [
        hadoop("mr_state_hour", f"hadoop jar {JAR} state-hour {RAW} {MR}/state_hour"),
        hadoop("mr_top_streets", f"hadoop jar {JAR} top-streets {RAW} {MR}/top_streets"),
    ] + [
        hadoop(f"mr_severity_by_{g}",
               f"hadoop jar {JAR} severity-stats -Dargus.groupBy={g} {RAW} {MR}/severity_by_{g}")
        for g in ("weather", "state", "hour")
    ]
    publish_mr = spark("publish_mr_to_mongo", "jobs/publish/publish_mr.py")

    clean = spark("spark_ingest_clean", "jobs/batch/ingest_clean.py")
    hive_refresh = container(
        "hive_refresh_partitions",
        "apache/hive:4.0.1",
        ["-u", "jdbc:hive2://hiveserver2:10000/", "-e", "MSCK REPAIR TABLE argus.accidents;"],
        entrypoint=["/opt/hive/bin/beeline"],
        mounts=[],
    )
    ml = [
        spark("ml_hotspots_kmeans", "jobs/ml/hotspots_kmeans.py"),
        spark("ml_severity_tree", "jobs/ml/severity_tree.py"),
        spark("ml_daily_forecast_lr", "jobs/ml/daily_forecast_lr.py"),
        spark("ml_rules_fpgrowth", "jobs/ml/rules_fpgrowth.py"),
        spark("ml_stats_tests", "jobs/ml/stats_tests.py"),
    ]
    done = EmptyOperator(task_id="batch_views_ready", trigger_rule="all_done")

    build_jar >> ingest
    ingest >> mr_jobs >> publish_mr >> done
    ingest >> clean >> [hive_refresh, *ml]
    [hive_refresh, *ml] >> done
