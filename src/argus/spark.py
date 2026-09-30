import contextlib
import json
import os
import tempfile

from pyspark.sql import SparkSession

from argus import config


def session(app: str) -> SparkSession:
    spark = SparkSession.builder.appName(f"argus-{app}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def mongo_db():
    from pymongo import MongoClient

    return MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=15000)[config.MONGO_DB]


@contextlib.contextmanager
def mlflow_run(name: str):
    """MLflow run if the tracking server is reachable; a no-op logger otherwise."""
    try:
        import mlflow

        mlflow.set_tracking_uri(config.MLFLOW_URI)
        mlflow.set_experiment(config.MLFLOW_EXPERIMENT)
    except Exception as exc:  # server down or mlflow missing: keep the job running
        print(f"[argus] MLflow unavailable ({type(exc).__name__}); metrics printed only")
        yield _PrintLogger()
        return
    with mlflow.start_run(run_name=name) as run:
        yield _MlflowLogger(mlflow, run.info.run_id)


class _MlflowLogger:
    def __init__(self, mlflow, run_id):
        self._m = mlflow
        self.run_id = run_id

    def params(self, **kw):
        self._m.log_params(kw)

    def metrics(self, step=None, **kw):
        self._m.log_metrics({k: float(v) for k, v in kw.items()}, step=step)

    def json(self, name, obj):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, name)
            with open(path, "w") as f:
                json.dump(obj, f, indent=2, default=str)
            self._m.log_artifact(path)

    def tag(self, key, value):
        self._m.set_tag(key, value)


class _PrintLogger:
    run_id = None

    def params(self, **kw):
        print("[params]", kw)

    def metrics(self, step=None, **kw):
        print("[metrics]", {"step": step, **kw})

    def json(self, name, obj):
        print(f"[artifact {name}]", json.dumps(obj, default=str)[:2000])

    def tag(self, key, value):
        print("[tag]", key, value)
