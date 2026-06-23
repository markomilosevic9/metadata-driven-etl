import logging
import os

from pipeline.core.config_loader import create_spark_session, load_config, load_metadata
from pipeline.jobs.ingestion_runner import run_ingestion
from pipeline.stages.scd_and_warehouse import run_dimension_and_fact_pipeline
from utils.logging_setup import setup_logging
from utils.postgres_helper import mark_run_finished, mark_run_started


logger = logging.getLogger(__name__)


def main() -> None:
    setup_logging()
    run_id = os.getenv("RUN_ID")
    if not run_id:
        raise ValueError("RUN_ID environment variable not set")

    logger.info("Stage started | stage=run_pipeline run_id=%s", run_id)

    config = load_config()
    metadata = load_metadata(config)
    analytics_db = config["analytics_db"]
    mark_run_started(run_id, analytics_db)
    spark = None

    try:
        spark = create_spark_session(
            config.get("spark", {}).get("app_name", "MotorPolicyPipeline"),
            config=config,
        )
        run_ingestion(
            run_id=run_id,
            spark=spark,
            config=config,
            metadata=metadata,
        )
        run_dimension_and_fact_pipeline(
            run_id=run_id,
            spark=spark,
            config=config,
            metadata=metadata,
        )
        mark_run_finished(run_id, "success", analytics_db)
    except Exception as exc:
        mark_run_finished(run_id, "failed", analytics_db, error_text=str(exc)[:4000])
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
