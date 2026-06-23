from collections import defaultdict
from datetime import datetime, timedelta

import pytest

from pipeline.core.config_loader import get_dim_column_list
from tests.conftest import date_str, prefix_before_glob
from utils.minio_helper import parse_s3a_path


@pytest.mark.post_pipeline
def test_dimension_has_expected_columns_and_no_removed_fields(
    dim_policy_rows,
    resolve_dimension_config,
):
    assert dim_policy_rows, "dim_policy Parquet output is empty"

    columns = set(dim_policy_rows[0].keys())
    dim_config = resolve_dimension_config("dim_policy")
    expected_dimension_columns = set(get_dim_column_list(dim_config))
    missing_columns = expected_dimension_columns - columns
    assert not missing_columns, f"Dimension missing required columns: {missing_columns}"


@pytest.mark.post_pipeline
def test_scd2_versioning_produces_historical_rows(dim_policy_rows):
    assert dim_policy_rows, "dim_policy Parquet output is empty"

    version_counts = defaultdict(int)
    for row in dim_policy_rows:
        version_counts[row["source_policy_id"]] += 1

    assert [policy for policy, count in version_counts.items() if count > 1]
    assert [row for row in dim_policy_rows if not row.get("is_current")]


@pytest.mark.post_pipeline
def test_exactly_one_current_row_per_policy(dim_policy_rows):
    assert dim_policy_rows, "dim_policy Parquet output is empty"

    current_counts = defaultdict(int)
    for row in dim_policy_rows:
        if row.get("is_current"):
            current_counts[row["source_policy_id"]] += 1

    violations = {
        source_policy_id: count
        for source_policy_id, count in current_counts.items()
        if count != 1
    }
    assert not violations, f"Policies with != 1 current row: {violations}"


@pytest.mark.post_pipeline
def test_current_row_has_latest_start_date_per_policy(dim_policy_rows):
    assert dim_policy_rows, "dim_policy Parquet output is empty"

    max_start = {}
    current_start = {}
    for row in dim_policy_rows:
        source_policy_id = row["source_policy_id"]
        start_date = date_str(row.get("start_date"))
        if start_date > max_start.get(source_policy_id, ""):
            max_start[source_policy_id] = start_date
        if row.get("is_current"):
            current_start[source_policy_id] = start_date

    mismatches = {
        source_policy_id
        for source_policy_id, start_date in current_start.items()
        if start_date != max_start.get(source_policy_id)
    }
    assert not mismatches


@pytest.mark.post_pipeline
def test_no_duplicate_versions_for_same_policy_and_start_date(dim_policy_rows):
    assert dim_policy_rows, "dim_policy Parquet output is empty"

    seen = set()
    duplicates = set()
    for row in dim_policy_rows:
        key = (row["source_policy_id"], date_str(row.get("start_date")))
        if key in seen:
            duplicates.add(key)
        seen.add(key)

    assert not duplicates, f"Duplicate (source_policy_id, start_date) pairs found: {duplicates}"


@pytest.mark.post_pipeline
def test_dimension_start_dates_equal_source_batch_date(dim_policy_rows):
    assert dim_policy_rows, "dim_policy Parquet output is empty"

    mismatches = {
        row["source_policy_id"]: (
            date_str(row.get("start_date")),
            date_str(row.get("source_batch_date")),
        )
        for row in dim_policy_rows
        if date_str(row.get("start_date")) != date_str(row.get("source_batch_date"))
    }
    assert not mismatches, f"start_date != source_batch_date for: {mismatches}"


@pytest.mark.post_pipeline
def test_dimension_end_dates_use_open_row_sentinel_and_are_never_null(dim_policy_rows):
    assert dim_policy_rows, "dim_policy Parquet output is empty"

    assert not [row for row in dim_policy_rows if row.get("end_date") is None]
    open_without_sentinel = [
        row
        for row in dim_policy_rows
        if row.get("is_current") and date_str(row.get("end_date")) != "2099-12-31"
    ]
    assert not open_without_sentinel


@pytest.mark.post_pipeline
def test_scd2_close_end_dates_align_with_next_start_minus_one(dim_policy_rows):
    assert dim_policy_rows, "dim_policy Parquet output is empty"

    versions_by_policy = defaultdict(list)
    for row in dim_policy_rows:
        versions_by_policy[row["source_policy_id"]].append(row)

    checked = 0
    for versions in versions_by_policy.values():
        if len(versions) < 2:
            continue
        ordered = sorted(versions, key=lambda row: date_str(row.get("start_date")))
        for index in range(len(ordered) - 1):
            current_end = datetime.strptime(
                date_str(ordered[index].get("end_date")),
                "%Y-%m-%d",
            ).date()
            next_start = datetime.strptime(
                date_str(ordered[index + 1].get("start_date")),
                "%Y-%m-%d",
            ).date()
            assert current_end == next_start - timedelta(days=1)
            checked += 1

    assert checked > 0


@pytest.mark.post_pipeline
def test_scd2_field_changes_create_versions(dim_policy_rows, metadata):
    assert dim_policy_rows, "dim_policy Parquet output is empty"

    scd2_fields = metadata["dim_policy"].get("scd_type_2_fields", [])
    version_counts = defaultdict(list)
    for row in dim_policy_rows:
        version_counts[row["source_policy_id"]].append(row)

    multi_version_policy = next((versions for versions in version_counts.values() if len(versions) > 1), None)
    if multi_version_policy is None:
        pytest.skip("No multi-version policies found")

    versions = sorted(multi_version_policy, key=lambda row: date_str(row.get("start_date")))
    changes_found = any(
        versions[index].get(field) != versions[index + 1].get(field)
        for index in range(len(versions) - 1)
        for field in scd2_fields
    )
    assert changes_found


@pytest.mark.post_pipeline
def test_scd1_field_change_does_not_create_new_dim_driver_version(read_parquet_from_minio, metadata):
    bucket, prefix = parse_s3a_path(metadata["dim_driver"]["dimension_output_path"])
    rows = read_parquet_from_minio(bucket=bucket, prefix=prefix)
    assert rows, "dim_driver Parquet output is empty"

    source_driver_id_counts = defaultdict(int)
    for row in rows:
        source_driver_id_counts[row["source_driver_id"]] += 1

    duplicates = {
        source_driver_id: count
        for source_driver_id, count in source_driver_id_counts.items()
        if count > 1
    }
    assert not duplicates


@pytest.mark.post_pipeline
def test_non_tracked_field_change_does_not_create_new_dim_policy_version(
    read_parquet_from_minio,
    read_json_from_minio,
    metadata,
    resolve_dimension_config,
):
    bucket, prefix = parse_s3a_path(metadata["dim_policy"]["dimension_output_path"])
    dim_rows = read_parquet_from_minio(bucket=bucket, prefix=prefix)
    assert dim_rows, "dim_policy Parquet output is empty"

    valid_bucket, valid_prefix = parse_s3a_path(
        resolve_dimension_config("dim_policy")["input_pattern"]
    )
    valid_records = read_json_from_minio(
        bucket=valid_bucket,
        prefix=prefix_before_glob(valid_prefix),
    )
    assert valid_records, "No valid records found"

    version_counts = defaultdict(int)
    for row in dim_rows:
        version_counts[row["source_policy_id"]] += 1
    single_version_policies = {policy for policy, count in version_counts.items() if count == 1}

    batch_counts = defaultdict(set)
    for record in valid_records:
        policy_number = record.get("policy_number")
        batch_date = record.get("source_batch_date")
        if policy_number and batch_date:
            batch_counts[policy_number].add(batch_date)
    multi_batch_policies = {policy for policy, dates in batch_counts.items() if len(dates) > 1}

    assert single_version_policies & multi_batch_policies


@pytest.mark.post_pipeline
def test_dim_driver_refresh_preserves_first_introduced_batch_date(
    read_parquet_from_minio,
    read_json_from_minio,
    metadata,
    resolve_dimension_config,
):
    driver_bucket, driver_prefix = parse_s3a_path(metadata["dim_driver"]["dimension_output_path"])
    driver_rows = read_parquet_from_minio(bucket=driver_bucket, prefix=driver_prefix)
    assert driver_rows, "dim_driver Parquet output is empty"

    valid_bucket, valid_prefix = parse_s3a_path(
        resolve_dimension_config("dim_policy")["input_pattern"]
    )
    valid_records = read_json_from_minio(
        bucket=valid_bucket,
        prefix=prefix_before_glob(valid_prefix),
    )
    assert valid_records, "No valid records found"

    driver_first_seen = {}
    driver_batch_history = defaultdict(set)
    for record in valid_records:
        license_number = record.get("license_number")
        batch_date = record.get("source_batch_date")
        if not license_number or not batch_date:
            continue
        driver_batch_history[license_number].add(batch_date)
        driver_first_seen[license_number] = min(batch_date, driver_first_seen.get(license_number, batch_date))

    refreshed_drivers = {license_number for license_number, batch_dates in driver_batch_history.items() if len(batch_dates) > 1}
    assert refreshed_drivers

    mismatches = {
        row["source_driver_id"]: (
            date_str(row.get("source_batch_date")),
            driver_first_seen.get(row["source_driver_id"]),
        )
        for row in driver_rows
        if row.get("source_driver_id") in refreshed_drivers
        and date_str(row.get("source_batch_date"))
        != driver_first_seen.get(row["source_driver_id"])
    }
    assert not mismatches


@pytest.mark.post_pipeline
def test_fact_date_matches_first_policy_introduction_for_single_version_refreshes(
    postgres_connection,
    read_json_from_minio,
    metadata,
    resolve_dimension_config,
):
    valid_bucket, valid_prefix = parse_s3a_path(
        resolve_dimension_config("dim_policy")["input_pattern"]
    )
    valid_records = read_json_from_minio(
        bucket=valid_bucket,
        prefix=prefix_before_glob(valid_prefix),
    )
    assert valid_records, "No valid records found"

    policy_first_seen = {}
    policy_batch_history = defaultdict(set)
    for record in valid_records:
        policy_number = record.get("policy_number")
        batch_date = record.get("source_batch_date")
        if not policy_number or not batch_date:
            continue
        policy_batch_history[policy_number].add(batch_date)
        policy_first_seen[policy_number] = min(batch_date, policy_first_seen.get(policy_number, batch_date))

    cursor = postgres_connection.cursor()
    cursor.execute(
        """
        SELECT p.source_policy_id, p.source_batch_date::text AS dimension_batch_date, d.date::text AS fact_date
        FROM dim_policy p
        JOIN fact_policy f ON f.policy_id = p.policy_id
        JOIN dim_date d ON d.date_id = f.date_id
        WHERE p.source_policy_id IN (
            SELECT source_policy_id
            FROM dim_policy
            GROUP BY source_policy_id
            HAVING COUNT(*) = 1
        );
        """
    )
    rows = cursor.fetchall()
    cursor.close()
    assert rows, "No single-version policies found in warehouse"

    checked = 0
    mismatches = {}
    for source_policy_id, dimension_batch_date, fact_date in rows:
        if len(policy_batch_history.get(source_policy_id, set())) <= 1:
            continue
        checked += 1
        expected_date = policy_first_seen[source_policy_id]
        if dimension_batch_date != expected_date or fact_date != expected_date:
            mismatches[source_policy_id] = {
                "dimension_batch_date": dimension_batch_date,
                "fact_date": fact_date,
                "expected_date": expected_date,
            }

    assert checked > 0
    assert not mismatches
