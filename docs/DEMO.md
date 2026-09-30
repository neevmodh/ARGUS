# ARGUS: demo script and viva prep

## Before the presentation (about 15 minutes)

```bash
make up && make up-stream && make up-hive
make pipeline          # batch layer populated
make hive-init
make stream            # speed layer running
```
Open these browser tabs: dashboard (:8501), NameNode (:9870), YARN (:8088), MLflow (:5500) and Kafka UI (:8085).

## 10-minute flow

| Min | Show | Say |
|---|---|---|
| 0–1 | README architecture diagram | "Lambda architecture: a batch layer for accuracy, a speed layer for latency, and MongoDB serving both." |
| 1–2 | `make conventional` output | Unit I: a single machine is slow, memory-bound and has no fault tolerance. |
| 2–3 | NameNode UI → *Datanodes* and *Browse* | Unit II: 64 MB blocks and 2 replicas spread over 3 DataNodes. |
| 3–4 | `make mr` log (counters, map → shuffle → reduce) and the JobHistory UI | Unit III: combiner, custom partitioner, custom Writable, chained jobs with SequenceFiles. |
| 4–6 | MLflow runs, then the dashboard's *Insights* tab | Unit IV: all four algorithms, weighted classes, time-based split, FP-Growth lift. |
| 6–7 | `make produce`, then the dashboard's *Live* tab and Kafka UI | The speed layer: events scored within seconds, alerts on a second Kafka topic. |
| 7–8 | `make demo-mongo` with the *Cluster health* tab open | Unit V / CAP: the election pauses writes, then they resume with none lost. |
| 8–9 | `make demo-hdfs` | HDFS heals itself after losing a DataNode. |
| 9–10 | `results/benchmark.png` | MapReduce vs Hive vs Spark, and why Parquet wins. |

## Likely viva questions

**Why not just use MySQL?** A single node can't scale storage or compute horizontally, and it has no built-in replication across commodity machines. Schema-on-write also makes messy CSV data slow to load. Hadoop splits the data into blocks and moves computation to where the data lives (data locality).

**What happens in the shuffle?** Map output is partitioned by key (with our `StatePartitioner`), sorted, spilled to disk and merged. Reducers then fetch their partition from every mapper and merge-sort it. The combiner shrinks map output before it crosses the network.

**Why is `SeverityWritable` safe to use as a combiner?** Its merge is associative and commutative: it sums counts, sums of severity and severe counts. The average is only computed at output time. Averaging averages inside a combiner would give the wrong answer.

**RDD vs DataFrame?** RDDs are opaque Python functions, so Spark can't optimize them and every row is serialized to Python. DataFrames go through the Catalyst optimizer and Tungsten's code generation and stay in the JVM. The benchmark shows the difference.

**Why class weights in the decision tree?** Severity 2 is about 80% of the data. Accuracy alone would reward predicting "2" every time, which is why we report per-class recall and weighted F1.

**What does lift mean?** Lift = confidence / P(consequent). A lift above 1 means the conditions make a severe accident more likely than the baseline rate.

**Where is CAP in your system?** A MongoDB replica set with majority write concern is CP: during an election it refuses writes rather than risk two diverging primaries. Reads from secondaries give BASE-style eventual consistency. HDFS's NameNode is also a consistency-first design.

**Why Kafka between the producer and Spark?** It decouples them, buffers bursts, lets you replay from offsets, and lets many consumers read one stream (the scorer and Kafka UI both do).

**What is a watermark?** It's how long Spark waits for late events before finalizing a window: 2 minutes here. It bounds the state kept in memory for streaming aggregations.

**How would this scale to real production?** Add more DataNodes and NodeManagers, run Spark on YARN or Kubernetes, use a multi-broker Kafka with replication factor 3, shard MongoDB, and set up NameNode HA with ZooKeeper failover.
