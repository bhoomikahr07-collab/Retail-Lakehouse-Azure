"""Shared helpers: Delta-enabled Spark session creation and config loading."""
import os
import yaml

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "config", "config.yaml")


def load_config(env: str) -> dict:
    with open(CONFIG_PATH) as f:
        full_config = yaml.safe_load(f)
    if env not in full_config:
        raise ValueError(f"Unknown environment '{env}'. Expected one of: local, azure")
    return full_config[env]


def load_dq_rules() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)["data_quality"]


def get_spark_session(app_name: str):
    """Create a Delta-enabled local Spark session; Databricks provides one already."""
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )

    try:
        from delta import configure_spark_with_delta_pip
        return configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
        return builder.getOrCreate()
