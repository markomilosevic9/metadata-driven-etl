import logging
import os
from datetime import date
from typing import Any, Dict, List, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType, StructField, TimestampType
from pyspark.storagelevel import StorageLevel

from pipeline.core.config_loader import get_dim_column_list
from pipeline.stages.schema_enforcer import build_spark_schema
from pipeline.stages.warehouse_loader import ensure_dim_date_rows, load_fact_from_dim_policy
from utils.minio_helper import discover_batch_dates, parse_s3a_path, path_exists
from utils.postgres_helper import (
    execute_sql,
    get_latest_dimension_contract_version,
    get_pending_star_batches,
    get_scd_batch_run_ids,
    get_unprocessed_ingestion_batches,
    get_unprocessed_scd_batches,
    get_jdbc_config,
    list_completed_scd_batches,
    mark_scd_batch_done,
    mark_scd_batch_warehouse_loaded,
)


logger = logging.getLogger(__name__)


def check_and_reset_dimension_version(
    analytics_db: Dict[str, Any],
    dim_config: dict,
    dimension_name: str,
) -> bool:
    # detect a dimension change and reset
    # returns true when the dimension should replay all discovered batches
    current_version = dim_config.get("contract_version", "1.0")
    previous_version = get_latest_dimension_contract_version(analytics_db, dimension_name)
    applied_batches = list_completed_scd_batches(analytics_db, dimension_name)
    force_rebuild = bool(applied_batches) and previous_version != current_version

    if force_rebuild:
        execute_sql(
            """
            UPDATE etl_scd_batches
            SET warehouse_loaded_at = NULL
            WHERE dimension_name = %s
            """,
            analytics_db,
            (dimension_name,),
        )

    return force_rebuild


def reorder_dimension_columns(df: DataFrame, dim_config: Dict[str, Any]) -> DataFrame:
    column_order = get_dim_column_list(dim_config)
    for column_name in column_order:
        if column_name not in df.columns:
            df = df.withColumn(column_name, F.lit(None))
    return df.select(*column_order)


def build_scd_input_schema(source_schema: Dict[str, Any]):
    scd_input_schema = build_spark_schema(source_schema)
    existing_field_names = set(scd_input_schema.names)
    for field in (
        StructField("source_batch_date", DateType(), True),
        StructField("processed_run_id", StringType(), True),
        StructField("ingestion_dt", TimestampType(), True),
    ):
        if field.name not in existing_field_names:
            scd_input_schema.add(field)
            existing_field_names.add(field.name)
    return scd_input_schema


def deduplicate_batch_records(
    spark: SparkSession,
    df_batch: DataFrame,
    dedup_config: Dict[str, Any],
) -> DataFrame:
    if not dedup_config.get("enabled", False):
        return df_batch

    key_column = dedup_config.get("key_column", "policy_number")

    df_batch.createOrReplaceTempView("scd_batch_input")
    columns_str = ", ".join([f"`{col}`" for col in df_batch.columns])

    return spark.sql(
        f"""
        SELECT {columns_str}
        FROM (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY `{key_column}`
                    ORDER BY `ingestion_dt` DESC,
                             `processed_run_id` DESC,
                             `{key_column}` ASC
                ) AS row_num
            FROM scd_batch_input
        ) ranked
        WHERE row_num = 1
        """
    )


def detect_scd2_changes(
    spark: SparkSession,
    df_new: DataFrame,
    df_current: DataFrame,
    scd2_fields: list,
    business_key: str,
) -> Tuple[DataFrame, DataFrame]:
    if not scd2_fields:
        empty_df = spark.createDataFrame([], df_current.schema)
        return empty_df, spark.createDataFrame([], df_new.schema)

    df_new.createOrReplaceTempView("scd2_new_data")
    df_current.filter(F.col("is_current") == True).createOrReplaceTempView(
        "scd2_current_dim"
    )

    scd2_conditions = [
        f"(scd2_new_data.`{field}` IS NOT NULL AND "
        f"(scd2_current_dim.`{field}` IS NULL OR "
        f"scd2_new_data.`{field}` != scd2_current_dim.`{field}`))"
        for field in scd2_fields
    ]

    changed_keys = spark.sql(
        f"""
        SELECT DISTINCT scd2_new_data.`{business_key}`
        FROM scd2_new_data
        INNER JOIN scd2_current_dim
            ON scd2_new_data.`{business_key}` = scd2_current_dim.`{business_key}`
        WHERE {' OR '.join(scd2_conditions)}
        """
    )

    records_to_close = df_current.filter(F.col("is_current") == True).join(
        changed_keys,
        business_key,
        "inner",
    )
    records_to_insert = df_new.join(changed_keys, business_key, "inner")
    return records_to_close, records_to_insert


def process_single_batch(
    spark: SparkSession,
    df_batch: DataFrame,
    batch_record_count: int,
    df_dimension_state: DataFrame,
    dim_config: Dict[str, Any],
    run_id: str,
) -> Tuple[DataFrame, Dict[str, Any]]:
    business_key = dim_config["business_key"]
    scd2_fields = dim_config.get("scd_type_2_fields", [])
    batch_order_column = dim_config.get("batch_order_column", "source_batch_date")
    sentinel_dates = dim_config.get("scd_sentinel_dates", {})
    open_end_date = F.lit(
        sentinel_dates.get("end_date_open", "2099-12-31")
    ).cast("date")
    refresh_keys = None
    df_to_close = None
    df_new_versions = None

    for derived in dim_config.get("derived_fields", []):
        df_batch = df_batch.withColumn(derived["name"], F.expr(derived["expression"]))

    if df_dimension_state is None:
        df_final = (
            df_batch
            .withColumn("start_date", F.col(batch_order_column).cast("date"))
            .withColumn("end_date", open_end_date)
            .withColumn("is_current", F.lit(True))
            .withColumn("last_updated_run_id", F.lit(run_id))
        )
        batch_mode = "initial_batch_load"
    else:
        if "is_current" in df_dimension_state.columns:
            df_historical = df_dimension_state.filter(F.col("is_current") == False)
            df_current = df_dimension_state.filter(F.col("is_current") == True)
        else:
            df_historical = spark.createDataFrame([], df_dimension_state.schema)
            df_current = df_dimension_state

        refresh_fields = [
            field
            for field in dim_config.get("scd_type_1_fields", [])
            if field and field in df_batch.columns and field in df_current.columns
        ]

        # stage 1: separate new business keys from existing ones
        df_existing_keys = df_current.select(business_key).distinct()
        df_new_rows = (
            df_batch.join(df_existing_keys, business_key, "left_anti")
            .withColumn("start_date", F.col(batch_order_column).cast("date"))
            .withColumn("end_date", open_end_date)
            .withColumn("is_current", F.lit(True))
            .withColumn("last_updated_run_id", F.lit(run_id))
        )
        df_existing_rows = df_batch.join(df_existing_keys, business_key, "inner")

        # stage 2: close current scd2 rows and open replacement versions
        df_to_close, df_new_versions = detect_scd2_changes(
            spark,
            df_existing_rows,
            df_current,
            scd2_fields,
            business_key,
        )
        if scd2_fields:
            df_to_close = df_to_close.persist(StorageLevel.MEMORY_AND_DISK)
            df_new_versions = df_new_versions.persist(StorageLevel.MEMORY_AND_DISK)

        df_closed_keys = df_to_close.select(business_key).distinct()
        df_closed_versions = (
            df_to_close
            .join(
                df_new_versions.select(
                    business_key,
                    F.col(batch_order_column).cast("date").alias("new_start_date"),
                ),
                business_key,
                "inner",
            )
            .withColumn("end_date", F.date_sub(F.col("new_start_date"), 1))
            .withColumn("is_current", F.lit(False))
            .withColumn("last_updated_run_id", F.lit(run_id))
            .drop("new_start_date")
        )
        df_open_versions = (
            df_new_versions
            .withColumn("start_date", F.col(batch_order_column).cast("date"))
            .withColumn("end_date", open_end_date)
            .withColumn("is_current", F.lit(True))
            .withColumn("last_updated_run_id", F.lit(run_id))
        )

        # stage 3: refresh current rows in place when only scd1 fields changed
        df_current_without_scd2 = df_current.join(df_closed_keys, business_key, "left_anti")
        df_updates_without_scd2 = df_existing_rows.join(
            df_closed_keys,
            business_key,
            "left_anti",
        )
        if refresh_fields:
            refresh_keys = df_updates_without_scd2.select(business_key).distinct().persist(
                StorageLevel.MEMORY_AND_DISK
            )
            df_current_to_refresh = df_current_without_scd2.join(
                refresh_keys,
                business_key,
                "inner",
            )
            df_current_unchanged = df_current_without_scd2.join(
                refresh_keys,
                business_key,
                "left_anti",
            )
            refresh_set = set(refresh_fields)
            df_refreshed_current = (
                df_current_to_refresh.alias("curr")
                .join(df_updates_without_scd2.alias("new"), business_key, "inner")
                .select(
                    *[
                        F.coalesce(
                            F.col(f"new.{column_name}"),
                            F.col(f"curr.{column_name}"),
                        ).alias(column_name)
                        if column_name in refresh_set
                        else F.col(f"curr.{column_name}").alias(column_name)
                        for column_name in df_current_to_refresh.columns
                    ],
                )
            )
            df_refreshed_current = df_refreshed_current.withColumn(
                "last_updated_run_id",
                F.lit(run_id),
            )
            df_current_rows = df_current_unchanged.unionByName(
                df_refreshed_current,
                allowMissingColumns=True,
            )
        else:
            df_current_rows = df_current_without_scd2

        # stage 4: pass historical rows through unchanged
        # stage 5: union the full batch result, align and checkpoint
        df_final = (
            df_historical
            .unionByName(df_closed_versions, allowMissingColumns=True)
            .unionByName(df_open_versions, allowMissingColumns=True)
            .unionByName(df_current_rows, allowMissingColumns=True)
            .unionByName(df_new_rows, allowMissingColumns=True)
        )
        batch_mode = "incremental_batch_load"

    df_final = reorder_dimension_columns(df_final, dim_config).localCheckpoint(eager=True)

    if df_to_close is not None:
        df_to_close.unpersist()
    if df_new_versions is not None:
        df_new_versions.unpersist()
    if refresh_keys is not None:
        refresh_keys.unpersist()

    return df_final, {
        "mode": batch_mode,
        "batch_records": batch_record_count,
    }


def process_dimension_batches(
    spark: SparkSession,
    analytics_db: Dict[str, Any],
    dim_config: Dict[str, Any],
    source_schema: Dict[str, Any],
    run_id: str,
    label: str,
    storage_config: Dict[str, Any],
    force_full_rebuild: bool = False,
) -> Tuple[DataFrame | None, Dict[str, Any]]:
    input_pattern = dim_config["input_pattern"]
    dimension_output_path = dim_config["dimension_output_path"]
    batch_order_column = dim_config.get("batch_order_column", "source_batch_date")
    dedup_config = dim_config.get("deduplication", {})
    contract_version = dim_config.get("contract_version", "1.0")

    valid_bucket, _ = parse_s3a_path(input_pattern)
    discovered_batch_dates = discover_batch_dates(valid_bucket, storage_config)
    missing_ingestion_state = get_unprocessed_ingestion_batches(
        discovered_batch_dates,
        analytics_db,
    )
    ingestion_batches = set(discovered_batch_dates) - set(missing_ingestion_state)

    if discovered_batch_dates and not ingestion_batches:
        raise ValueError(f"[{label}] No ingestion batches recorded; run ingestion first")

    if missing_ingestion_state:
        raise ValueError(
            f"[{label}] Valid batches missing ingestion state: {missing_ingestion_state}"
        )

    all_batch_dates = [
        batch_date for batch_date in discovered_batch_dates if batch_date in ingestion_batches
    ]
    if not all_batch_dates:
        return None, {
            "status": "skipped",
            "mode": "incremental_pending_batches",
            "reason": f"[{label}] No valid batch records found",
        }

    pending_batches = (
        all_batch_dates
        if force_full_rebuild
        else get_unprocessed_scd_batches(all_batch_dates, analytics_db, label)
    )
    applied_batches = list_completed_scd_batches(analytics_db, label)
    if applied_batches and pending_batches:
        max_applied = max(applied_batches)
        min_pending = min(pending_batches)
        if min_pending < max_applied:
            logger.info(
                "[%s] Late-arriving batch %s precedes max applied %s; forcing rebuild",
                label,
                min_pending,
                max_applied,
            )
            force_full_rebuild = True
            pending_batches = all_batch_dates
            execute_sql(
                """
                UPDATE etl_scd_batches
                SET warehouse_loaded_at = NULL
                WHERE dimension_name = %s
                """,
                analytics_db,
                (label,),
            )
    logger.info(
        "[%s] Discovered %s batch(es); pending: %s",
        label,
        len(all_batch_dates),
        pending_batches,
    )

    if force_full_rebuild or not path_exists(storage_config, dimension_output_path):
        df_dimension_state = None
    else:
        df_dimension_state = spark.read.format("parquet").load(dimension_output_path)

    if not pending_batches:
        existing_count = df_dimension_state.count() if df_dimension_state is not None else 0
        return df_dimension_state, {
            "status": "skipped",
            "mode": "incremental_pending_batches",
            "reason": f"[{label}] No pending batches",
            "dimension_records": existing_count,
            "batches_replayed": [],
            "full_rebuild_triggered": force_full_rebuild,
        }

    pending_paths = [
        input_pattern.replace("batch-*", f"batch-{batch_date}", 1)
        for batch_date in pending_batches
    ]
    scd_input_schema = build_scd_input_schema(source_schema)
    df_pending = (
        spark.read.format("json").schema(scd_input_schema).load(pending_paths).cache()
    )

    for batch_date in pending_batches:
        df_batch_raw = df_pending.filter(F.col(batch_order_column) == batch_date)
        df_batch = deduplicate_batch_records(spark, df_batch_raw, dedup_config)
        for old_name, new_name in dim_config.get("field_renames", {}).items():
            df_batch = df_batch.withColumnRenamed(old_name, new_name)
        deduped_count = df_batch.count()

        if deduped_count == 0:
            logger.info("[%s] Batch %s empty after deduplication", label, batch_date)
            continue

        df_dimension_state, batch_results = process_single_batch(
            spark=spark,
            df_batch=df_batch,
            batch_record_count=deduped_count,
            df_dimension_state=df_dimension_state,
            dim_config=dim_config,
            run_id=run_id,
        )
        logger.info(
            "[%s] Applied batch %s with mode=%s rows=%s",
            label,
            batch_date,
            batch_results["mode"],
            deduped_count,
        )

    if df_dimension_state is None:
        df_pending.unpersist()
        return None, {
            "status": "skipped",
            "mode": "incremental_pending_batches",
            "reason": f"[{label}] No records after per-batch deduplication",
        }

    df_dimension_state = reorder_dimension_columns(df_dimension_state, dim_config)
    df_dimension_state.write.format("parquet").mode("overwrite").save(dimension_output_path)
    final_count = df_dimension_state.count()
    df_pending.unpersist()

    for batch_date in pending_batches:
        mark_scd_batch_done(
            batch_date=batch_date,
            dimension_name=label,
            run_id=run_id,
            contract_version=contract_version,
            analytics_db=analytics_db,
        )

    return df_dimension_state, {
        "status": "success",
        "mode": "incremental_pending_batches",
        "batches_replayed": pending_batches,
        "dimension_records": final_count,
        "full_rebuild_triggered": force_full_rebuild,
    }


def upsert_dimension_to_postgres(
    df_dimension_state: DataFrame,
    dim_name: str,
    target_table: str,
    columns: List[str],
    natural_key_columns: List[str],
    jdbc_config: Dict[str, Any],
    analytics_db: Dict[str, Any],
    pending_batch_dates: List[str],
    run_id: str,
) -> int:
    if not pending_batch_dates:
        logger.info("[%s] No pending warehouse batches", dim_name)
        return 0

    natural_key_set = set(natural_key_columns)
    # filter to rows touched by the run(s) that wrote the pending warehouse batches
    # this covers the normal current-run path and the recovery path where a previous run wrote parquet plus etl_scd_batches, then failed before warehouse upsert 
    # the fallback is defensive: pending_star_batches should always imply at least one etl_scd_batches row
    pending_run_ids = get_scd_batch_run_ids(
        analytics_db=analytics_db,
        dimension_name=dim_name,
        batch_dates=pending_batch_dates,
    ) or [run_id]
    df_filtered = df_dimension_state.filter(
        F.col("last_updated_run_id").isin(pending_run_ids)
    )

    df_selected = df_filtered.select(*columns)
    total_count = df_selected.count()
    distinct_count = df_selected.select(*natural_key_columns).distinct().count()
    if total_count != distinct_count:
        duplicate_count = total_count - distinct_count
        raise ValueError(
            f"[{dim_name}] Found {duplicate_count} duplicate natural key(s) on "
            f"({', '.join(natural_key_columns)})."
        )

    staging_table = f"stg_{dim_name}_upsert"
    df_selected.write.jdbc(
        url=jdbc_config["url"],
        table=staging_table,
        mode="overwrite",
        properties=jdbc_config["properties"],
    )

    set_clause = ",\n                ".join(
        f"{column} = EXCLUDED.{column}"
        for column in columns
        if column not in natural_key_set
    ) + ",\n                updated_at = CURRENT_TIMESTAMP"
    select_clause = ", ".join(
        (
            f"{column}::date AS {column}"
            if column == "source_batch_date"
            else f"{column}::timestamp AS {column}"
            if column == "ingestion_dt"
            else column
        )
        for column in columns
    )

    execute_sql(
        f"""
        INSERT INTO {target_table} ({', '.join(columns)})
        SELECT {select_clause}
        FROM {staging_table}
        ON CONFLICT ({', '.join(natural_key_columns)}) DO UPDATE
        SET
            {set_clause};
        """,
        analytics_db,
    )
    execute_sql(f"DROP TABLE IF EXISTS {staging_table};", analytics_db)

    logger.info(
        "[%s] Upserted %s dimension rows for batches %s",
        dim_name,
        total_count,
        pending_batch_dates,
    )
    return total_count


def run_dimension_and_fact_pipeline(
    spark: SparkSession,
    config: Dict[str, Any],
    metadata: Dict[str, Any],
    run_id: str = None,
) -> None:
    if not run_id:
        run_id = os.getenv("RUN_ID")
        if not run_id:
            raise ValueError("RUN_ID environment variable not set")

    logger.info("Stage started | stage=scd_and_warehouse run_id=%s", run_id)

    storage_config = config.get("storage", {})
    analytics_db = config["analytics_db"]
    scd_sentinel_dates = metadata.get("scd_defaults", {}).get("scd_sentinel_dates", {})
    source_schema = metadata.get("ingestion", {}).get("source", {}).get("schema", {})
    dimension_defaults = metadata.get("dimension_defaults", {})
    dimension_registry = metadata.get("dimension_registry", [])
    if not dimension_registry:
        raise ValueError("Metadata missing dimension_registry configuration")

    fact_config = ((metadata.get("star_schema", {}).get("facts")) or [None])[0]
    if fact_config is None:
        raise ValueError("Metadata missing star_schema.facts configuration")

    fact_annual_premium_source = (
        fact_config.get("measures", {})
        .get("annual_premium_amount", {})
        .get("source", "premium_amount")
    )
    jdbc_config = get_jdbc_config(config)

    dimension_results: Dict[str, Any] = {}
    dimension_counts: Dict[str, int] = {}
    pending_fact_batches: List[str] = []

    for label in dimension_registry:
        dim_config_raw = metadata.get(label)
        if dim_config_raw is None:
            raise ValueError(f"Metadata missing '{label}' dimension configuration")

        dim_config = {
            **dimension_defaults,
            **dim_config_raw,
            "scd_sentinel_dates": scd_sentinel_dates,
        }
        force_rebuild = check_and_reset_dimension_version(
            analytics_db,
            dim_config,
            label,
        )
        if force_rebuild:
            logger.info(
                "[%s] Contract version changed; forcing full SCD and warehouse replay",
                label,
            )

        df_dimension_state, dimension_result = process_dimension_batches(
            spark=spark,
            analytics_db=analytics_db,
            dim_config=dim_config,
            source_schema=source_schema,
            run_id=run_id,
            label=label,
            storage_config=storage_config,
            force_full_rebuild=force_rebuild,
        )

        pending_star_batches = get_pending_star_batches(analytics_db, label)
        dimension_result["pending_star_batches"] = pending_star_batches
        dimension_results[label] = dimension_result

        if not pending_star_batches:
            dimension_counts[label] = 0
            logger.info("[%s] No pending warehouse batches", label)
            continue

        if df_dimension_state is None:
            if not path_exists(storage_config, dim_config["dimension_output_path"]):
                raise ValueError(
                    f"[{label}] Warehouse batches pending but no dimension parquet exists"
                )
            df_dimension_state = spark.read.format("parquet").load(
                dim_config["dimension_output_path"]
            )

        dimension_counts[label] = upsert_dimension_to_postgres(
            df_dimension_state=df_dimension_state,
            dim_name=label,
            target_table=dim_config["target_table"],
            columns=get_dim_column_list(dim_config),
            natural_key_columns=dim_config["natural_key_columns"],
            jdbc_config=jdbc_config,
            analytics_db=analytics_db,
            pending_batch_dates=pending_star_batches,
            run_id=run_id,
        )

        if label == "dim_policy":
            pending_fact_batches = pending_star_batches
        else:
            for batch_date in pending_star_batches:
                mark_scd_batch_warehouse_loaded(
                    batch_date=batch_date,
                    dimension_name=label,
                    analytics_db=analytics_db,
                )

    date_range = None
    fact_count = 0
    if pending_fact_batches:
        batch_dates = [date.fromisoformat(batch_date) for batch_date in pending_fact_batches]
        date_range = ensure_dim_date_rows(analytics_db=analytics_db, batch_dates=batch_dates)
        fact_count = load_fact_from_dim_policy(
            fact_table=fact_config["target_table"],
            annual_premium_source=fact_annual_premium_source,
            analytics_db=analytics_db,
            pending_batch_dates=pending_fact_batches,
        )
        for batch_date in pending_fact_batches:
            mark_scd_batch_warehouse_loaded(
                batch_date=batch_date,
                dimension_name="dim_policy",
                analytics_db=analytics_db,
            )
    else:
        logger.info("No pending fact batches")

    logger.info(
        "SCD and warehouse load completed successfully | dims=%s fact=%s date_range=%s results=%s",
        dimension_counts,
        fact_count,
        date_range,
        dimension_results,
    )
