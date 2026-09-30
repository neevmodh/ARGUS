"""Feature engineering shared by batch training and the streaming scorer."""

from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, StringIndexer, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from argus.schema import CATEGORICAL_FEATURES, NUMERIC_FEATURES


def add_time_features(df: DataFrame, ts_col: str) -> DataFrame:
    ts = F.col(ts_col)
    return (
        df.withColumn("hour", F.hour(ts))
        .withColumn("day_of_week", F.dayofweek(ts))
        .withColumn("month", F.month(ts))
    )


def cast_numeric(df: DataFrame) -> DataFrame:
    for c in NUMERIC_FEATURES:
        df = df.withColumn(c, F.col(c).cast("double"))
    return df


def feature_stages():
    """StringIndexer -> Imputer -> VectorAssembler. Output column: `features`."""
    imputed = [f"{c}_imp" for c in NUMERIC_FEATURES]
    return [
        StringIndexer(
            inputCols=CATEGORICAL_FEATURES,
            outputCols=[f"{c}_idx" for c in CATEGORICAL_FEATURES],
            handleInvalid="keep",
        ),
        Imputer(inputCols=NUMERIC_FEATURES, outputCols=imputed, strategy="median"),
        VectorAssembler(
            inputCols=imputed + [f"{c}_idx" for c in CATEGORICAL_FEATURES],
            outputCol="features",
            handleInvalid="keep",
        ),
    ]


def feature_pipeline() -> Pipeline:
    return Pipeline(stages=feature_stages())
