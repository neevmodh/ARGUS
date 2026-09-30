import os


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


HDFS = _env("ARGUS_HDFS", "hdfs://namenode:8020")
RAW_PATH = f"{HDFS}/argus/raw/accidents"
CURATED_PATH = f"{HDFS}/argus/curated/accidents"
MR_PATH = f"{HDFS}/argus/mr"
MODELS_PATH = f"{HDFS}/argus/models"
RESULTS_PATH = f"{HDFS}/argus/results"
CHECKPOINT_PATH = f"{HDFS}/argus/checkpoints"

SEVERITY_MODEL_PATH = f"{MODELS_PATH}/severity_tree"
KMEANS_MODEL_PATH = f"{MODELS_PATH}/hotspots_kmeans"
FORECAST_MODEL_PATH = f"{MODELS_PATH}/daily_forecast_lr"

KAFKA_BOOTSTRAP = _env("ARGUS_KAFKA", "kafka:9092")
KAFKA_TOPIC = _env("ARGUS_KAFKA_TOPIC", "accidents.live")

MONGO_URI = _env(
    "ARGUS_MONGO_URI",
    "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0",
)
MONGO_DB = _env("ARGUS_MONGO_DB", "argus")

MLFLOW_URI = _env("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT = _env("ARGUS_MLFLOW_EXPERIMENT", "argus")

NAMENODE_HTTP = _env("ARGUS_NAMENODE_HTTP", "http://namenode:9870")
