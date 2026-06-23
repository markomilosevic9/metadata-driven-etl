import pytest
from psycopg2.extras import RealDictCursor

from utils.minio_helper import parse_s3a_path


def comparable_value(value):
    if hasattr(value, "date") and hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "item"):
        value = value.item()
    if value is None:
        return None
    return str(value)


@pytest.mark.post_pipeline
def test_star_schema_tables_and_view_exist(postgres_connection):
    cursor = postgres_connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = 'public'
          AND table_name IN (
              'v_policy_current',
              'mart_active_policies_by_coverage',
              'mart_premium_by_month_country',
              'mart_policy_version_history'
          );
        """
    )
    views = {row["table_name"] for row in cursor.fetchall()}
    cursor.close()

    assert {
        "v_policy_current",
        "mart_active_policies_by_coverage",
        "mart_premium_by_month_country",
        "mart_policy_version_history",
    } <= views


@pytest.mark.post_pipeline
def test_dq_run_metrics_written(postgres_connection):
    cursor = postgres_connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        """
        SELECT
            run_id,
            valid_records,
            invalid_records,
            input_records,
            total_records,
            reconciliation_difference,
            per_batch_valid,
            per_batch_invalid
        FROM dq_run_metrics
        ORDER BY measured_at DESC
        LIMIT 1;
        """
    )
    row = cursor.fetchone()
    cursor.close()

    assert row is not None
    assert row["run_id"]
    assert row["valid_records"] is not None
    assert row["invalid_records"] is not None
    assert row["input_records"] is not None
    assert isinstance(row["per_batch_valid"], dict)
    assert isinstance(row["per_batch_invalid"], dict)
    assert row["reconciliation_difference"] == 0, (
        f"reconciliation_difference must be 0; got {row['reconciliation_difference']}"
    )
    assert sum(int(v) for v in row["per_batch_valid"].values()) == row["valid_records"]
    assert sum(int(v) for v in row["per_batch_invalid"].values()) == row["invalid_records"]
    assert row["valid_records"] + row["invalid_records"] == row["total_records"]


@pytest.mark.post_pipeline
def test_star_schema_watermarks_written(postgres_connection):
    cursor = postgres_connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE warehouse_loaded_at IS NOT NULL) AS watermark_rows,
            COUNT(DISTINCT dimension_name) FILTER (WHERE warehouse_loaded_at IS NOT NULL)
                AS dimensions_covered
        FROM etl_scd_batches;
        """
    )
    row = cursor.fetchone()
    cursor.close()

    assert row["watermark_rows"] > 0
    assert row["dimensions_covered"] == 4


@pytest.mark.post_pipeline
def test_warehouse_columns_follow_dimensional_naming_standard(postgres_connection):
    cursor = postgres_connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        """
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN (
              'dim_policy',
              'dim_driver',
              'dim_vehicle',
              'dim_coverage',
              'dim_date',
              'fact_policy'
          );
        """
    )
    columns_by_table = {}
    for row in cursor.fetchall():
        columns_by_table.setdefault(row["table_name"], set()).add(row["column_name"])
    cursor.close()

    assert "license_number" not in columns_by_table["dim_policy"]
    assert "policy_number" not in columns_by_table["dim_policy"]
    assert "license_number" not in columns_by_table["dim_driver"]
    assert "plate_number" not in columns_by_table["dim_vehicle"]
    assert "coverage_type" not in columns_by_table["dim_coverage"]
    assert "date_sk" not in columns_by_table["dim_date"]
    assert "date_sk" not in columns_by_table["fact_policy"]
    assert "policy_count" not in columns_by_table["fact_policy"]


@pytest.mark.post_pipeline
def test_postgres_dimension_matches_parquet_staging(postgres_connection, read_parquet_from_minio, metadata):
    bucket, prefix = parse_s3a_path(metadata["dim_policy"]["dimension_output_path"])
    rows = read_parquet_from_minio(bucket=bucket, prefix=prefix)
    assert rows, "dim_policy Parquet output is empty"

    parquet_total = len(rows)
    parquet_current = sum(1 for row in rows if row.get("is_current"))
    parquet_natural_keys = len(
        {(row["source_policy_id"], str(row.get("start_date"))[:10]) for row in rows}
    )

    cursor = postgres_connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(CASE WHEN is_current = true THEN 1 END) AS current_rows,
            COUNT(DISTINCT (source_policy_id, start_date)) AS distinct_natural_keys
        FROM dim_policy;
        """
    )
    stats = cursor.fetchone()
    cursor.close()

    assert stats["total_rows"] == parquet_total
    assert stats["current_rows"] == parquet_current
    assert stats["distinct_natural_keys"] == parquet_natural_keys


@pytest.mark.post_pipeline
def test_dim_driver_field_equivalence_with_parquet(
    postgres_connection,
    read_parquet_from_minio,
    metadata,
):
    bucket, prefix = parse_s3a_path(metadata["dim_driver"]["dimension_output_path"])
    parquet_rows = read_parquet_from_minio(bucket=bucket, prefix=prefix)
    assert parquet_rows, "dim_driver Parquet output is empty"

    cursor = postgres_connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM dim_driver")
    pg_rows_by_key = {row["source_driver_id"]: row for row in cursor.fetchall()}
    cursor.close()

    compared_columns = [
        column
        for column in parquet_rows[0]
        if column not in {"driver_id", "updated_at"}
    ]
    mismatches = {}
    for parquet_row in parquet_rows:
        key = parquet_row["source_driver_id"]
        pg_row = pg_rows_by_key.get(key)
        if pg_row is None:
            mismatches[key] = "missing in Postgres"
            continue
        for column in compared_columns:
            if comparable_value(parquet_row.get(column)) != comparable_value(pg_row.get(column)):
                mismatches.setdefault(key, {})[column] = {
                    "parquet": comparable_value(parquet_row.get(column)),
                    "postgres": comparable_value(pg_row.get(column)),
                }

    assert not mismatches


@pytest.mark.post_pipeline
def test_dim_vehicle_field_equivalence_with_parquet(
    postgres_connection,
    read_parquet_from_minio,
    metadata,
):
    bucket, prefix = parse_s3a_path(metadata["dim_vehicle"]["dimension_output_path"])
    parquet_rows = read_parquet_from_minio(bucket=bucket, prefix=prefix)
    assert parquet_rows, "dim_vehicle Parquet output is empty"

    cursor = postgres_connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM dim_vehicle")
    pg_rows_by_key = {row["source_vehicle_id"]: row for row in cursor.fetchall()}
    cursor.close()

    compared_columns = [
        column
        for column in parquet_rows[0]
        if column not in {"vehicle_id", "updated_at"}
    ]
    mismatches = {}
    for parquet_row in parquet_rows:
        key = parquet_row["source_vehicle_id"]
        pg_row = pg_rows_by_key.get(key)
        if pg_row is None:
            mismatches[key] = "missing in Postgres"
            continue
        for column in compared_columns:
            if comparable_value(parquet_row.get(column)) != comparable_value(pg_row.get(column)):
                mismatches.setdefault(key, {})[column] = {
                    "parquet": comparable_value(parquet_row.get(column)),
                    "postgres": comparable_value(pg_row.get(column)),
                }

    assert not mismatches


@pytest.mark.post_pipeline
def test_dim_coverage_parquet_matches_postgres(postgres_connection, read_parquet_from_minio, metadata):
    bucket, prefix = parse_s3a_path(metadata["dim_coverage"]["dimension_output_path"])
    rows = read_parquet_from_minio(bucket=bucket, prefix=prefix)

    parquet_count = len(rows)
    parquet_types = {row["source_coverage_id"] for row in rows}

    cursor = postgres_connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        """
        SELECT COUNT(*) AS total, array_agg(source_coverage_id) AS types
        FROM dim_coverage;
        """
    )
    pg = cursor.fetchone()
    cursor.close()

    pg_types = set(pg["types"]) if pg["types"] else set()
    assert pg["total"] == parquet_count
    assert pg_types == parquet_types


@pytest.mark.post_pipeline
def test_fact_measures_are_populated_and_reasonable(postgres_connection):
    cursor = postgres_connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(annual_premium_amount) AS premium_populated,
            MIN(annual_premium_amount) AS min_premium,
            MAX(annual_premium_amount) AS max_premium
        FROM fact_policy;
        """
    )
    stats = cursor.fetchone()
    cursor.close()
    assert stats["premium_populated"] == stats["total_rows"]


@pytest.mark.post_pipeline
def test_fact_dates_match_dim_policy_introduction_dates(postgres_connection):
    cursor = postgres_connection.cursor(cursor_factory=RealDictCursor)
    cursor.execute(
        """
        SELECT DISTINCT source_batch_date::text AS batch_date
        FROM dim_policy
        ORDER BY batch_date;
        """
    )
    expected_dates = {row["batch_date"] for row in cursor.fetchall()}

    cursor.execute(
        """
        SELECT DISTINCT d.date::text AS batch_date
        FROM fact_policy f
        JOIN dim_date d ON f.date_id = d.date_id
        ORDER BY batch_date;
        """
    )
    actual_dates = {row["batch_date"] for row in cursor.fetchall()}
    cursor.close()
    assert actual_dates == expected_dates


