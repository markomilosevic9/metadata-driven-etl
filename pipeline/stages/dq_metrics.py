import json
import logging
import os
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Tuple

from psycopg2.extras import Json

from pipeline.core.config_loader import create_minio_client, load_config, load_metadata
from utils.logging_setup import setup_logging
from utils.minio_helper import discover_batch_dates
from utils.postgres_helper import connect_postgres, query_dict, query_scalar


logger = logging.getLogger(__name__)


def stream_json_records_from_s3(
    storage_config: dict,
    bucket: str,
    prefix: str,
) -> Iterable[Tuple[str | None, Dict[str, Any]]]:
    client = create_minio_client(storage_config)
    for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
        if not obj.object_name.endswith((".json", ".jsonl")):
            continue
        batch_folder = obj.object_name.split("/", 1)[0]
        batch_date = batch_folder[len("batch-"):] if batch_folder.startswith("batch-") else None
        response = client.get_object(bucket, obj.object_name)
        try:
            for raw_line in response.read().decode("utf-8").splitlines():
                line = raw_line.strip()
                if line:
                    yield batch_date, json.loads(line)
        finally:
            response.close()
            response.release_conn()


def finalize_metrics_accumulator(
    state: Dict[str, Any],
    dataset_type: str,
) -> Dict[str, Any]:
    total_records = state["total_records"]
    if total_records == 0:
        return {"status": "skipped", "reason": "No records found"}

    duplicate_count = total_records - len(state["uniqueness_seen"])
    metrics = {
        "total_records": total_records,
        "duplicate_rate": round((duplicate_count / total_records) * 100, 2),
    }
    if dataset_type == "invalid_data":
        metrics["error_breakdown"] = {
            field: dict(sorted(errors.items()))
            for field, errors in sorted(state["error_breakdown"].items())
        }
    return metrics


def compute_dataset_metrics(
    storage_config: dict,
    bucket: str,
    dataset_type: str,
    uniqueness_key_columns: List[str],
    batch_dates: List[str],
) -> Dict[str, Any]:
    try:
        aggregated_state = {
            "total_records": 0,
            "uniqueness_seen": set(),
        }
        if dataset_type == "invalid_data":
            aggregated_state["error_breakdown"] = defaultdict(lambda: defaultdict(int))
        per_batch_states = {}
        batch_set = set(batch_dates)
        for batch_date, record in stream_json_records_from_s3(
            storage_config,
            bucket,
            "batch-",
        ):
            if batch_date is None or batch_date not in batch_set:
                continue
            batch_state = per_batch_states.setdefault(
                batch_date,
                {
                    "total_records": 0,
                    "uniqueness_seen": set(),
                    **(
                        {
                            "error_breakdown": defaultdict(lambda: defaultdict(int)),
                        }
                        if dataset_type == "invalid_data"
                        else {}
                    ),
                },
            )

            for state in (aggregated_state, batch_state):
                state["total_records"] += 1
                if dataset_type == "invalid_data":
                    validation_errors = record.get("validation_errors")
                    if isinstance(validation_errors, dict):
                        for field, errors in validation_errors.items():
                            normalized_errors = (
                                errors
                                if isinstance(errors, list)
                                else ([] if errors is None else [errors])
                            )
                            for error_type in normalized_errors:
                                state["error_breakdown"][field][str(error_type)] += 1

                uniqueness_key = tuple(
                    record.get(column) for column in uniqueness_key_columns
                )
                state["uniqueness_seen"].add(uniqueness_key)

        return {
            "aggregated": finalize_metrics_accumulator(
                aggregated_state,
                dataset_type,
            ),
            "per_batch": {
                batch_date: finalize_metrics_accumulator(
                    batch_state,
                    dataset_type,
                )
                for batch_date, batch_state in sorted(per_batch_states.items())
            },
        }
    except Exception:
        logger.exception("Error computing %s metrics", dataset_type)
        raise


def calculate_input_output_reconciliation(
    analytics_db: Dict[str, Any],
    valid_count: int,
    invalid_count: int,
    batch_dates: List[str] | None = None,
) -> Dict[str, Any]:
    try:
        if batch_dates:
            input_count = query_scalar(
                """
                SELECT COALESCE(SUM(input_record_count), 0)
                FROM etl_ingestion_batches
                WHERE batch_date::text = ANY(%s)
                """,
                analytics_db,
                (sorted(batch_dates),),
            )
        else:
            input_count = query_scalar(
                "SELECT COALESCE(SUM(input_record_count), 0) FROM etl_ingestion_batches",
                analytics_db,
            )

        input_count = int(input_count or 0)
        output_total = valid_count + invalid_count
        return {
            "input_records": input_count,
            "valid_records": valid_count,
            "invalid_records": invalid_count,
            "output_total": output_total,
            "difference": input_count - output_total,
        }
    except Exception:
        logger.exception("Error computing input/output reconciliation metrics")
        raise


def calculate_dimension_metrics_sql(
    analytics_db: Dict[str, Any],
    dim_name: str,
    is_scd2: bool = False,
) -> Dict[str, Any]:
    try:
        if is_scd2:
            stats = query_dict(
                f"""
                SELECT
                    COUNT(*) AS total_dimension_rows,
                    COUNT(*) FILTER (WHERE is_current = TRUE) AS current_version_count,
                    COUNT(*) FILTER (WHERE is_current = FALSE) AS historical_version_count
                FROM {dim_name}
                """,
                analytics_db,
            )
            total = int(stats.get("total_dimension_rows", 0) or 0)
            if total == 0:
                return {"status": "skipped", "reason": "No rows in dimension dataset"}

            return {
                "total_dimension_rows": total,
                "current_version_count": int(stats.get("current_version_count", 0) or 0),
                "historical_version_count": int(
                    stats.get("historical_version_count", 0) or 0
                ),
            }

        total = int(query_scalar(f"SELECT COUNT(*) FROM {dim_name}", analytics_db) or 0)
        if total == 0:
            return {"status": "skipped", "reason": "No rows in dimension dataset"}

        return {"total_dimension_rows": total}
    except Exception:
        logger.exception("Error computing %s metrics", dim_name)
        raise


def get_fact_row_count(analytics_db: Dict[str, Any]) -> int:
    return int(query_scalar("SELECT COUNT(*) FROM fact_policy", analytics_db) or 0)


def write_dq_metrics_to_postgres(
    dq_row: dict,
    analytics_db: dict,
) -> None:
    json_columns = {
        "per_batch_valid",
        "per_batch_invalid",
        "invalid_error_breakdown",
    }
    row_params = {
        key: Json(value) if key in json_columns else value
        for key, value in dq_row.items()
    }
    columns = list(dq_row.keys())
    assignments = ",\n                        ".join(
        f"{column} = EXCLUDED.{column}"
        for column in columns
        if column != "run_id"
    )

    with connect_postgres(analytics_db) as conn:
        try:
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    INSERT INTO dq_run_metrics (
                        {', '.join(columns)}
                    )
                    VALUES (
                        {', '.join(f'%({column})s' for column in columns)}
                    )
                    ON CONFLICT (run_id) DO UPDATE SET
                        {assignments},
                        measured_at = CURRENT_TIMESTAMP
                    """,
                    row_params,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    logger.info("Wrote DQ run metrics to Postgres for run_id=%s", dq_row["run_id"])


def compute_and_persist_dq_metrics(run_id: str = None) -> None:
    setup_logging()
    if not run_id:
        run_id = os.getenv("RUN_ID")
        if not run_id:
            raise ValueError("RUN_ID environment variable not set")

    logger.info("Stage started | stage=dq_metrics run_id=%s", run_id)

    config = load_config()
    metadata = load_metadata(config)
    storage_config = config.get("storage", {})
    analytics_db = config["analytics_db"]
    buckets = storage_config.get("buckets", {})
    dimension_registry = metadata.get("dimension_registry", [])
    valid_bucket = buckets.get("output_valid", "data-valid")
    invalid_bucket = buckets.get("output_invalid", "data-invalid")
    input_bucket = buckets.get("input", "input-data")
    uniqueness_key_columns = (
        metadata.get("dq", {})
        .get("uniqueness", {})
        .get("key_columns", ["policy_number"])
    )

    batch_dates = discover_batch_dates(bucket=input_bucket, storage_config=storage_config)
    if not batch_dates:
        logger.info("No input batches discovered for DQ")
        return

    valid_metrics = compute_dataset_metrics(
        storage_config=storage_config,
        bucket=valid_bucket,
        dataset_type="valid_data",
        uniqueness_key_columns=uniqueness_key_columns,
        batch_dates=batch_dates,
    )
    invalid_metrics = compute_dataset_metrics(
        storage_config=storage_config,
        bucket=invalid_bucket,
        dataset_type="invalid_data",
        uniqueness_key_columns=uniqueness_key_columns,
        batch_dates=batch_dates,
    )
    aggregated_valid = valid_metrics["aggregated"]
    aggregated_invalid = invalid_metrics["aggregated"]
    per_batch_metrics = {
        f"batch-{batch_date}": {
            "valid_records": valid_metrics["per_batch"].get(
                batch_date,
                {"status": "skipped", "reason": "No records found"},
            ),
            "invalid_records": invalid_metrics["per_batch"].get(
                batch_date,
                {"status": "skipped", "reason": "No records found"},
            ),
        }
        for batch_date in batch_dates
    }

    dimension_metrics = {}
    for label in dimension_registry:
        dim_config_raw = metadata.get(label, {})
        dimension_metrics[label] = calculate_dimension_metrics_sql(
            analytics_db=analytics_db,
            dim_name=dim_config_raw.get("target_table", label),
            is_scd2=bool(dim_config_raw.get("scd_type_2_fields")),
        )

    try:
        fact_rows = get_fact_row_count(analytics_db)
    except Exception:
        logger.exception("Error computing fact_row_count metrics")
        raise

    valid_all_count = (
        0
        if aggregated_valid.get("status") == "skipped"
        else int(aggregated_valid.get("total_records", 0) or 0)
    )
    invalid_all_count = (
        0
        if aggregated_invalid.get("status") == "skipped"
        else int(aggregated_invalid.get("total_records", 0) or 0)
    )
    total_records = valid_all_count + invalid_all_count
    reconciliation_result = calculate_input_output_reconciliation(
        analytics_db=analytics_db,
        valid_count=valid_all_count,
        invalid_count=invalid_all_count,
        batch_dates=batch_dates,
    )
    dq_row = {
        "run_id": run_id,
        "input_records": reconciliation_result.get("input_records"),
        "valid_records": valid_all_count,
        "invalid_records": invalid_all_count,
        "total_records": total_records,
        "valid_rate": round((valid_all_count / total_records) * 100, 2) if total_records > 0 else 0,
        "invalid_rate": round((invalid_all_count / total_records) * 100, 2)
        if total_records > 0 else 0,
        "reconciliation_difference": reconciliation_result.get("difference"),
        "batch_count": len(batch_dates),
        "per_batch_valid": {
            batch_date: (
                0
                if per_batch_metrics[f"batch-{batch_date}"]["valid_records"].get("status")
                == "skipped"
                else int(
                    per_batch_metrics[f"batch-{batch_date}"]["valid_records"].get(
                        "total_records",
                        0,
                    )
                    or 0
                )
            )
            for batch_date in batch_dates
        },
        "per_batch_invalid": {
            batch_date: (
                0
                if per_batch_metrics[f"batch-{batch_date}"]["invalid_records"].get("status")
                == "skipped"
                else int(
                    per_batch_metrics[f"batch-{batch_date}"]["invalid_records"].get(
                        "total_records",
                        0,
                    )
                    or 0
                )
            )
            for batch_date in batch_dates
        },
        "valid_duplicate_rate": aggregated_valid.get("duplicate_rate"),
        "invalid_duplicate_rate": aggregated_invalid.get("duplicate_rate"),
        "invalid_error_breakdown": aggregated_invalid.get("error_breakdown"),
        "dim_policy_total_rows": dimension_metrics["dim_policy"].get("total_dimension_rows"),
        "dim_policy_current_rows": dimension_metrics["dim_policy"].get("current_version_count"),
        "dim_policy_historical_rows": dimension_metrics["dim_policy"].get(
            "historical_version_count"
        ),
        "dim_driver_total_rows": dimension_metrics["dim_driver"].get("total_dimension_rows"),
        "dim_vehicle_total_rows": dimension_metrics["dim_vehicle"].get("total_dimension_rows"),
        "dim_coverage_total_rows": dimension_metrics["dim_coverage"].get(
            "total_dimension_rows"
        ),
        "fact_rows": fact_rows,
    }
    write_dq_metrics_to_postgres(
        dq_row=dq_row,
        analytics_db=analytics_db,
    )

    logger.info("DQ metrics calculation completed successfully")


if __name__ == "__main__":
    compute_and_persist_dq_metrics()
