from datetime import datetime

import pytest

from tests.conftest import bucket_name, parse_timestamp


@pytest.mark.post_pipeline
def test_valid_records_have_expected_columns_and_no_validation_errors(
    read_json_from_minio,
    config,
    metadata,
):
    valid_records = read_json_from_minio(
        bucket=bucket_name(config, "output_valid"),
        prefix="batch-",
    )
    assert valid_records, "No valid records found"

    source = metadata["ingestion"]["source"]
    source_fields = [field["name"] for field in source.get("schema", {}).get("fields", [])]
    expected_valid_columns = set(
        source_fields + ["source_batch_date", "processed_run_id", "ingestion_dt"]
    )
    actual_cols = set()
    for record in valid_records:
        actual_cols.update(record.keys())

    missing = expected_valid_columns - actual_cols
    assert not missing, f"Valid records missing expected columns: {missing}"
    assert not [record for record in valid_records if "validation_errors" in record]


@pytest.mark.post_pipeline
def test_valid_records_have_valid_lineage(read_json_from_minio, config):
    valid_records = read_json_from_minio(
        bucket=bucket_name(config, "output_valid"),
        prefix="batch-",
    )
    assert valid_records, "No valid records found"

    def is_valid_date(value) -> bool:
        if not isinstance(value, str):
            return False
        try:
            datetime.strptime(value, "%Y-%m-%d")
            return True
        except ValueError:
            return False

    def is_valid_timestamp(value) -> bool:
        if not isinstance(value, str):
            return False
        try:
            parse_timestamp(value)
            return True
        except ValueError:
            return False

    assert not [record for record in valid_records if record.get("ingestion_dt") is None]
    assert not [record for record in valid_records if not is_valid_timestamp(record.get("ingestion_dt"))]
    assert not [record for record in valid_records if record.get("source_batch_date") is None]
    assert not [
        record for record in valid_records if not is_valid_date(record.get("source_batch_date"))
    ]
    assert not [record for record in valid_records if record.get("processed_run_id") is None]
    assert not [record for record in valid_records if "source_batch_id" in record]


@pytest.mark.post_pipeline
def test_invalid_records_have_expected_error_structure(
    read_json_from_minio,
    config,
    metadata,
):
    invalid_records = read_json_from_minio(
        bucket=bucket_name(config, "output_invalid"),
        prefix="batch-",
    )
    assert invalid_records, "No invalid records found"

    assert not [record for record in invalid_records if "validation_errors" not in record]
    assert not [record for record in invalid_records if record.get("validation_errors") is None]
    assert not [record for record in invalid_records if record.get("source_batch_date") is None]
    assert not [record for record in invalid_records if record.get("processed_run_id") is None]
    assert not [record for record in invalid_records if "source_batch_id" in record]

    all_error_fields = set()
    for record in invalid_records:
        validation_errors = record.get("validation_errors")
        if isinstance(validation_errors, dict):
            all_error_fields.update(validation_errors.keys())

    assert all_error_fields
    validated_fields = [
        validation["field"]
        for validation in metadata.get("ingestion", {}).get("validations", [])
    ]
    unexpected = all_error_fields - set(validated_fields)
    assert not unexpected, f"Unexpected invalid error fields: {unexpected}"


@pytest.mark.post_pipeline
def test_dq_breakdown_contains_each_rule_type(postgres_connection):
    cursor = postgres_connection.cursor()
    cursor.execute(
        """
        SELECT invalid_error_breakdown
        FROM dq_run_metrics
        ORDER BY measured_at DESC
        LIMIT 1;
        """
    )
    row = cursor.fetchone()
    cursor.close()

    assert row is not None
    breakdown = row[0]
    assert isinstance(breakdown, dict)

    error_codes = {
        error_code
        for field_errors in breakdown.values()
        for error_code, count in field_errors.items()
        if int(count) > 0
    }
    assert "notNull" in error_codes
    assert "notEmpty" in error_codes
    assert any(code.startswith("regex:") for code in error_codes)
    assert any(code.startswith("minValue:") for code in error_codes)
    assert any(code.startswith("maxValue:") for code in error_codes)
