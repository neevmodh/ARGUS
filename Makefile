SHELL := /bin/bash
.DEFAULT_GOAL := help

COMPOSE   := docker compose
DATA      ?= $(firstword $(wildcard data/raw/US_Accidents*.csv) data/sample/us_accidents_sample.csv)
ROWS      ?= 200000
RAW       := /argus/raw/accidents
MR        := /argus/mr
JAR       := /opt/argus/mapreduce/target/argus-mr.jar
MONGO_URI := mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0

HDFS   := $(COMPOSE) exec -T namenode
SUBMIT := $(COMPOSE) exec -T spark-master argus-submit
BATCH_PROFILES := --profile spark --profile mongo --profile app
ALL_PROFILES   := $(BATCH_PROFILES) --profile kafka --profile hive --profile airflow

##@ Setup
help: ## Show this help
	@awk 'BEGIN{FS=":.*##"; printf "\nUsage: make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*##/ {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2} /^##@/ {printf "\n\033[1m%s\033[0m\n", substr($$0, 5)}' $(MAKEFILE_LIST)

sample: ## Generate a synthetic dataset (ROWS=200000) in data/sample/
	python3 scripts/generate_sample.py --rows $(ROWS)

download: ## Download the real Kaggle US Accidents dataset (needs ~/.kaggle/kaggle.json)
	./scripts/download_dataset.sh

build: ## Build all project images
	$(COMPOSE) $(ALL_PROFILES) build

##@ Cluster
up-core: ## Start HDFS + YARN only
	$(COMPOSE) up -d

up: ## Start HDFS + YARN + Spark + MLflow + MongoDB + dashboard
	$(COMPOSE) $(BATCH_PROFILES) up -d
	@$(MAKE) --no-print-directory urls

up-stream: ## Also start Kafka + Kafka UI (speed layer)
	$(COMPOSE) $(BATCH_PROFILES) --profile kafka up -d kafka kafka-ui

up-hive: ## Also start HiveServer2
	$(COMPOSE) --profile hive up -d hiveserver2

up-airflow: ## Also start Airflow (+ docker socket proxy)
	$(COMPOSE) --profile airflow up -d

up-all: ## Start everything (needs ~12 GB for Docker)
	$(COMPOSE) $(ALL_PROFILES) up -d --scale producer=0
	@$(MAKE) --no-print-directory urls

down: ## Stop all containers (data volumes kept)
	$(COMPOSE) $(ALL_PROFILES) down

clean: ## Stop everything AND delete all volumes (HDFS, MongoDB, Kafka, MLflow data)
	$(COMPOSE) $(ALL_PROFILES) down -v

ps: ## Show container status
	$(COMPOSE) $(ALL_PROFILES) ps

urls: ## Print web UI addresses
	@echo "NameNode      http://localhost:9870    YARN       http://localhost:8088"
	@echo "JobHistory    http://localhost:19888   Spark      http://localhost:8090"
	@echo "MLflow        http://localhost:5500    Dashboard  http://localhost:8501"
	@echo "Kafka UI      http://localhost:8085    HiveServer http://localhost:10002"
	@echo "Airflow       http://localhost:8082  (admin / admin)"

##@ Batch layer
hdfs-put: ## Upload DATA (default: real dataset if present, else sample) to HDFS
	@test -f "$(DATA)" || { echo "No dataset at $(DATA). Run 'make sample' or 'make download'."; exit 1; }
	$(HDFS) hdfs dfs -mkdir -p $(RAW)
	$(HDFS) hdfs dfs -put -f /$(DATA) $(RAW)/accidents.csv
	$(HDFS) hdfs fsck $(RAW)/accidents.csv -files -blocks -locations | tail -n 25

hdfs-ls: ## List everything ARGUS stored in HDFS
	$(HDFS) hdfs dfs -ls -R -h /argus | grep -v '/_' | head -n 80

mr-build: ## Compile + unit-test the Java MapReduce jobs (in a Maven container)
	docker run --rm -v "$(CURDIR)/mapreduce:/src" -v argus-m2:/root/.m2 -w /src maven:3.9-eclipse-temurin-17 mvn -q -B package

mr: ## Run all MapReduce jobs on YARN
	$(HDFS) hadoop jar $(JAR) state-hour $(RAW) $(MR)/state_hour
	$(HDFS) hadoop jar $(JAR) top-streets $(RAW) $(MR)/top_streets
	for g in weather state hour; do \
	  $(HDFS) hadoop jar $(JAR) severity-stats -Dargus.groupBy=$$g $(RAW) $(MR)/severity_by_$$g || exit 1; \
	done
	$(HDFS) hdfs dfs -cat $(MR)/top_streets/part-r-00000 | head -n 10

publish-mr: ## Load MapReduce outputs into MongoDB
	$(SUBMIT) jobs/publish/publish_mr.py

spark-clean: ## Raw CSV -> cleaned Parquet (partitioned by state/year)
	$(SUBMIT) jobs/batch/ingest_clean.py

ml: ## Train all Spark ML models (K-Means, Decision Tree, Linear Regression, FP-Growth, SciPy)
	$(SUBMIT) jobs/ml/hotspots_kmeans.py
	$(SUBMIT) jobs/ml/severity_tree.py
	$(SUBMIT) jobs/ml/daily_forecast_lr.py
	$(SUBMIT) jobs/ml/rules_fpgrowth.py
	$(SUBMIT) jobs/ml/stats_tests.py

pipeline: hdfs-put mr publish-mr spark-clean ml ## Full batch layer end-to-end

##@ Speed layer
stream: ## Start the Spark Structured Streaming risk scorer (background)
	$(COMPOSE) exec -d spark-master bash -c "argus-submit jobs/streaming/live_risk.py > /tmp/live_risk.log 2>&1"
	@echo "streaming job started - logs: make stream-logs"

stream-logs: ## Tail the streaming job log
	$(COMPOSE) exec spark-master tail -f /tmp/live_risk.log

stream-stop: ## Stop the streaming job
	-$(COMPOSE) exec -T spark-master pkill -f live_risk.py

produce: ## Replay accidents into Kafka with live Open-Meteo weather
	$(COMPOSE) --profile kafka run --rm producer python streaming/producer.py --source /$(DATA) --rate 20 --live-weather

produce-historic: ## Replay with the dataset's historic weather (offline)
	$(COMPOSE) --profile kafka run --rm producer python streaming/producer.py --source /$(DATA) --rate 20

alerts: ## Watch the high-risk alerts topic
	$(COMPOSE) exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic accidents.alerts

##@ Hive / MongoDB
hive-init: ## Create Hive tables over HDFS
	$(COMPOSE) exec -T hiveserver2 beeline -u jdbc:hive2://localhost:10000/ -f /opt/argus-hive/ddl/01_tables.sql

hive-query: ## Run the Hive analytics queries
	$(COMPOSE) exec -T hiveserver2 beeline -u jdbc:hive2://localhost:10000/ -f /opt/argus-hive/queries/analytics.sql

beeline: ## Interactive Hive shell
	$(COMPOSE) exec hiveserver2 beeline -u jdbc:hive2://localhost:10000/argus

mongo-crud: ## Unit V walkthrough: data types, CRUD, aggregation, geo, explain
	$(COMPOSE) run --rm --no-deps mongo-init mongosh "$(MONGO_URI)" --quiet /scripts/crud_demo.js

mongo-shell: ## Interactive mongosh on the replica set
	$(COMPOSE) exec mongo1 mongosh "$(MONGO_URI)"

##@ Demos & evaluation
demo-hdfs: ## Kill a DataNode, keep reading, watch re-replication
	./scripts/demo_hdfs_failover.sh

demo-mongo: ## Kill the MongoDB primary under write load, watch the election (CAP)
	./scripts/demo_mongo_failover.sh

benchmark: ## Same query on MapReduce vs Hive vs Spark RDD/DataFrame/SQL/Parquet
	./scripts/benchmark.sh

conventional: ## Unit I: time a single-machine SQLite/pandas load of DATA
	$(COMPOSE) run --rm --no-deps dashboard python scripts/conventional_limits.py /$(DATA)

test: ## Python + Java unit tests
	python3 -m pytest -q tests
	$(MAKE) --no-print-directory mr-build

.PHONY: help sample download build up-core up up-stream up-hive up-airflow up-all down clean ps urls \
        hdfs-put hdfs-ls mr-build mr publish-mr spark-clean ml pipeline stream stream-logs stream-stop \
        produce produce-historic alerts hive-init hive-query beeline mongo-crud mongo-shell \
        demo-hdfs demo-mongo benchmark conventional test
