import json
import os
import tempfile
from datetime import datetime

import psycopg2
import pyarrow.parquet as pq
import pytest

from pipeline.core.config_loader import (
    create_minio_client,
    load_config,
    load_metadata,
)
from utils.minio_helper import parse_s3a_path


def bucket_name(config, key):
    return config["storage"]["buckets"][key]


def date_str(value) -> str:
    if value is None:
        return ""
    return str(value)[:10]


def parse_timestamp(ts_str: str) -> datetime:
    return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ")


def prefix_before_glob(prefix: str) -> str:
    return prefix.split("*", 1)[0].rstrip("/") or "batch-"


@pytest.fixture(scope="session")
def config():
    return load_config()


@pytest.fixture(scope="session")
def metadata(config):
    return load_metadata(config)


@pytest.fixture(scope="session")
def resolve_dimension_config(metadata):
    defaults = metadata.get("dimension_defaults", {})

    def resolve_dimension(label: str) -> dict:
        return {**defaults, **metadata.get(label, {})}

    return resolve_dimension


@pytest.fixture(scope="session")
def minio_client(config):
    return create_minio_client(config.get("storage", {}))


@pytest.fixture(scope="session")
def read_json_from_minio(minio_client):
    def read_records(bucket: str, prefix: str) -> list:
        records = []
        for obj in minio_client.list_objects(bucket, prefix=prefix, recursive=True):
            if not obj.object_name.endswith(".json"):
                continue
            response = minio_client.get_object(bucket, obj.object_name)
            try:
                content = response.read().decode("utf-8")
                for line in content.splitlines():
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))
            finally:
                response.close()
                response.release_conn()
        return records

    return read_records


@pytest.fixture(scope="session")
def read_parquet_from_minio(minio_client):
    def read_rows(bucket: str, prefix: str) -> list:
        with tempfile.TemporaryDirectory() as tmp_dir:
            downloaded = 0
            for obj in minio_client.list_objects(bucket, prefix=prefix, recursive=True):
                if not obj.object_name.endswith(".parquet"):
                    continue
                local_name = obj.object_name.replace("/", "_")
                local_path = os.path.join(tmp_dir, local_name)
                minio_client.fget_object(bucket, obj.object_name, local_path)
                downloaded += 1

            if downloaded == 0:
                return []

            table = pq.read_table(tmp_dir)
            return table.to_pandas().to_dict("records")

    return read_rows


@pytest.fixture(scope="function")
def dim_policy_rows(read_parquet_from_minio, metadata):
    bucket, prefix = parse_s3a_path(metadata["dim_policy"]["dimension_output_path"])
    return read_parquet_from_minio(bucket=bucket, prefix=prefix)


@pytest.fixture(scope="session")
def postgres_connection(config):
    try:
        connection_kwargs = {
            key: value
            for key, value in config["analytics_db"].items()
            if key != "driver"
        }
        conn = psycopg2.connect(**connection_kwargs)
        conn.autocommit = False
        yield conn
        conn.close()
    except psycopg2.OperationalError as exc:
        if os.getenv("PYTEST_ALLOW_PG_SKIP") == "1":
            pytest.skip(f"Postgres unavailable in PYTEST_ALLOW_PG_SKIP mode: {exc}")
        pytest.fail(
            f"Cannot connect to Postgres analytics database: {exc}. "
            "Set PYTEST_ALLOW_PG_SKIP=1 to allow skip locally."
        )
