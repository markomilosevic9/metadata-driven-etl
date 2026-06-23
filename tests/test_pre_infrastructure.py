import json

import pytest
from minio.error import S3Error

from generate_sample_data import DATA_GENERATION_CONFIG
from pipeline.core.config_loader import load_config, load_metadata
from utils.minio_helper import parse_s3a_path


def get_expected_input_object_prefixes() -> tuple[str, list[tuple[str, str]]]:
    metadata = load_metadata(load_config())
    input_pattern = metadata.get("ingestion", {}).get("source", {}).get("path")
    assert input_pattern, "Metadata missing ingestion.source.path"

    bucket_name, key_pattern = parse_s3a_path(input_pattern)
    configured_batch_dates = DATA_GENERATION_CONFIG.get("batch_dates", [])
    assert configured_batch_dates, "Sample-data generation config missing batch_dates"

    prefixes = [
        (batch_date, key_pattern.replace("{date}", batch_date).split("*", 1)[0])
        for batch_date in configured_batch_dates
    ]
    return bucket_name, prefixes


@pytest.mark.pre_pipeline
def test_input_data_exists_and_valid(minio_client):
    try:
        input_bucket, expected_prefixes = get_expected_input_object_prefixes()
        total_files_found = 0

        for batch_date, object_prefix in expected_prefixes:
            batch_objects = list(minio_client.list_objects(input_bucket, prefix=object_prefix, recursive=True))
            batch_files = [obj for obj in batch_objects if not obj.object_name.endswith("/")]
            if not batch_files:
                pytest.fail(f"Batch '{batch_date}' has no input files for prefix '{object_prefix}'.")

            total_files_found += len(batch_files)
            for obj in batch_files:
                if not (obj.object_name.endswith(".json") or obj.object_name.endswith(".jsonl")):
                    continue
                if obj.size == 0:
                    pytest.fail(f"File '{obj.object_name}' in batch '{batch_date}' is empty.")
                try:
                    response = minio_client.get_object(input_bucket, obj.object_name)
                    first_chunk = response.read(1024).decode("utf-8")
                    response.close()
                    response.release_conn()
                    first_line = first_chunk.split("\n")[0].strip()
                    if first_line:
                        json.loads(first_line)
                except json.JSONDecodeError:
                    pytest.fail(f"File '{obj.object_name}' is not valid JSON.")
                except Exception as exc:
                    pytest.fail(f"Error reading '{obj.object_name}': {exc}")

        assert total_files_found > 0, "No input files found for any configured batch."
    except S3Error as exc:
        pytest.fail(f"Error accessing input data: {exc}")
