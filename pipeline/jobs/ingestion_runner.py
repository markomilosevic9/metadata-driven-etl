import logging
import os
from typing import Any, Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipeline.stages.field_validator import split_valid_invalid
from pipeline.stages.schema_enforcer import build_spark_schema
from utils.postgres_helper import (
    get_unprocessed_ingestion_batches,
    mark_ingestion_batch_done,
)
from utils.minio_helper import discover_batch_dates


logger = logging.getLogger(__name__)


def read_source(spark, source_config: dict, batch_date: str) -> DataFrame | None:
    # load the configured ingestion source for one batch
    source_path = source_config["path"].replace("{date}", batch_date)
    required = source_config.get("required", True)
    reader = (
        spark.read
        .format(source_config.get("format", "json"))
        .options(**source_config.get("options", {}))
    )

    schema_def = source_config.get("schema")
    if schema_def and source_config.get("schema_enforcement", {}).get("enabled", False):
        reader = reader.schema(build_spark_schema(schema_def))
    else:
        reader = reader.option("mode", "PERMISSIVE")

    try:
        return reader.load(source_path)
    except Exception as exc:
        if required:
            raise
        logger.warning("Skipping optional source for batch %s: %s", batch_date, exc)
        return None


def ingest_batch(
    spark: SparkSession,
    source_config: dict,
    validations: list,
    output_paths: dict,
    batch_date: str,
    run_id: str,
) -> int:
    df_source = read_source(spark, source_config, batch_date)
    if df_source is None:
        logger.info("No source records found for batch %s", batch_date)
        return 0

    df_with_metadata = (
        df_source
        .withColumn("source_batch_date", F.lit(batch_date).cast("date"))
        .withColumn("processed_run_id", F.lit(run_id))
    )
    input_record_count = df_source.count()

    df_valid, df_invalid = split_valid_invalid(spark, df_with_metadata, validations)
    df_valid = df_valid.withColumn("ingestion_dt", F.current_timestamp())
    df_invalid = df_invalid.withColumn("ingestion_dt", F.current_timestamp())

    (
        df_valid.write
        .format("json")
        .mode("overwrite")
        .save(output_paths["valid_path"].replace("{date}", batch_date))
    )
    (
        df_invalid.write
        .format("json")
        .mode("overwrite")
        .save(output_paths["invalid_path"].replace("{date}", batch_date))
    )
    return input_record_count


def run_ingestion(
    spark: SparkSession,
    config: Dict[str, Any],
    metadata: Dict[str, Any],
    run_id: str = None,
) -> None:
    if not run_id:
        run_id = os.getenv("RUN_ID")
        if not run_id:
            raise ValueError("RUN_ID environment variable not set")

    logger.info("Stage started | stage=ingestion run_id=%s", run_id)

    analytics_db = config["analytics_db"]
    storage_config = config["storage"]
    bucket_config = storage_config.get("buckets", {})
    input_bucket = bucket_config.get("input", "input-data")
    output_paths = {
        "valid_path": (
            f"s3a://{bucket_config.get('output_valid', 'data-valid')}/batch-{{date}}/output"
        ),
        "invalid_path": (
            f"s3a://{bucket_config.get('output_invalid', 'data-invalid')}/batch-{{date}}/output"
        ),
    }
    ingestion_config = metadata["ingestion"]

    all_batches = discover_batch_dates(
        bucket=input_bucket,
        storage_config=storage_config,
    )
    new_batches = get_unprocessed_ingestion_batches(all_batches, analytics_db)

    if not new_batches:
        logger.info("No new ingestion batches to process")
        return

    for batch_date in new_batches:
        logger.info("Processing ingestion batch %s", batch_date)
        input_record_count = ingest_batch(
            spark=spark,
            source_config=ingestion_config["source"],
            validations=ingestion_config.get("validations", []),
            output_paths=output_paths,
            batch_date=batch_date,
            run_id=run_id,
        )
        mark_ingestion_batch_done(
            batch_date=batch_date,
            run_id=run_id,
            analytics_db=analytics_db,
            input_record_count=input_record_count,
        )

    logger.info("Ingestion completed for batches: %s", new_batches)
