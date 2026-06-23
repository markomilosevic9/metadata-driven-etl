import json
import os
from pathlib import Path
from typing import Dict

import yaml

from minio import Minio
from pyspark.sql import SparkSession


# core config loader
# exposes helpers that carry more complex logic
# all  other config/metadata values are accessed directly by callers e.g. config["storage"], config["spark"], metadata["dim_policy"] etc.


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def get_dim_column_list(dim_config: dict) -> list[str]:
    seen = set()
    ordered_columns = []
    for column_name in [
        dim_config["business_key"],
        *dim_config.get("non_tracked_fields", []),
        *dim_config.get("scd_type_1_fields", []),
        *dim_config.get("scd_type_2_fields", []),
        *dim_config.get("lineage_columns", []),
        *dim_config.get("metadata_columns", []),
    ]:
        if not column_name or column_name in seen:
            continue
        ordered_columns.append(column_name)
        seen.add(column_name)
    return ordered_columns


def load_config(config_path: str = None) -> dict:
    # load pipeline_config.yaml, substituting  env vars
    # validates only the storage and analytics_db keys that cannot be defaulted by callers
    if config_path is None:
        config_path = PROJECT_ROOT / "config" / "pipeline_config.yaml"

    with open(config_path, "r") as fh:
        raw_yaml = fh.read()

    config = yaml.safe_load(os.path.expandvars(raw_yaml))

    missing = []
    storage = config.get("storage", {})
    analytics_db = config.get("analytics_db", {})

    for key in ("endpoint", "access_key", "secret_key", "buckets"):
        if key not in storage:
            missing.append(f"storage.{key}")

    buckets = storage.get("buckets", {})
    for key in ("input", "output_valid", "output_invalid", "dimensions"):
        if key not in buckets:
            missing.append(f"storage.buckets.{key}")

    for key in ("host", "port", "database", "user", "password"):
        if key not in analytics_db:
            missing.append(f"analytics_db.{key}")

    if missing:
        raise ValueError(
            "pipeline_config.yaml is missing required keys: "
            + ", ".join(missing)
        )

    return config


def load_metadata(config: dict = None, metadata_path: str = None) -> dict:
    # load pipeline_metadata.json
    # resolves the path from pipeline_config.yaml when not provided explicitly
    if config is None:
        config = load_config()

    if metadata_path is None:
        metadata_path = (
            config.get("pipeline", {}).get("metadata_path", "config/pipeline_metadata.json")
        )

    metadata_path = Path(metadata_path)
    if not metadata_path.is_absolute():
        metadata_path = (PROJECT_ROOT / metadata_path).resolve()

    with open(metadata_path, "r") as fh:
        return json.load(fh)


def create_minio_client(storage_config: Dict) -> Minio:
    # build and return a minio client from a storage config dict
    # handles http(s):// stripping
    endpoint = (
        storage_config["endpoint"]
        .replace("http://", "")
        .replace("https://", "")
    )
    return Minio(
        endpoint=endpoint,
        access_key=storage_config["access_key"],
        secret_key=storage_config["secret_key"],
        secure=storage_config.get("secure", False),
    )


def create_spark_session(
    app_name: str,
    config: dict = None,
    event_log_enabled: bool = None,
) -> SparkSession:
    # create and return a SparkSession wired for minio/s3a access
    if config is None:
        config = load_config()

    spark_config = config.get("spark", {})
    storage = config.get("storage", {})
    spark_master = os.getenv(
        "SPARK_MASTER_URL",
        spark_config.get("master_url", "local[*]"),
    )

    builder = SparkSession.builder.master(spark_master).appName(app_name)

    if event_log_enabled is not None:
        builder = builder.config(
            "spark.eventLog.enabled", str(event_log_enabled).lower()
        )

    spark = builder.getOrCreate()

    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", storage["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", storage["secret_key"])
    hadoop_conf.set("fs.s3a.endpoint", storage["endpoint"])
    hadoop_conf.set("fs.s3a.path.style.access", str(storage.get("path_style_access", True)).lower())
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", str(storage.get("secure", False)).lower())

    return spark

