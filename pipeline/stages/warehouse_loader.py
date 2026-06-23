import logging
from datetime import date
from typing import Any, Dict, List

from utils.postgres_helper import execute_sql, query_scalar


logger = logging.getLogger(__name__)


def ensure_dim_date_rows(
    analytics_db: Dict[str, Any],
    batch_dates: List[date],
) -> Dict[str, str]:
    if not batch_dates:
        raise ValueError("No batch dates provided for dim_date population")

    min_year = min(batch_dates).year
    max_year = max(batch_dates).year
    range_start = f"{min_year}-01-01"
    range_end = f"{max_year}-12-31"

    execute_sql(
        """
        INSERT INTO dim_date (
            date_id, date, year, quarter, month, month_name, day,
            day_of_week, day_name, week_of_year,
            is_weekend, is_month_start, is_month_end,
            is_quarter_start, is_quarter_end
        )
        SELECT
            TO_CHAR(d, 'YYYYMMDD')::INTEGER                              AS date_id,
            d                                                             AS date,
            EXTRACT(YEAR    FROM d)::INTEGER                              AS year,
            EXTRACT(QUARTER FROM d)::INTEGER                              AS quarter,
            EXTRACT(MONTH   FROM d)::INTEGER                              AS month,
            TRIM(TO_CHAR(d, 'Month'))                                     AS month_name,
            EXTRACT(DAY     FROM d)::INTEGER                              AS day,
            EXTRACT(DOW     FROM d)::INTEGER                              AS day_of_week,
            TRIM(TO_CHAR(d, 'Day'))                                       AS day_name,
            EXTRACT(WEEK    FROM d)::INTEGER                              AS week_of_year,
            EXTRACT(DOW FROM d) IN (0, 6)                                AS is_weekend,
            EXTRACT(DAY FROM d) = 1                                       AS is_month_start,
            d = (DATE_TRUNC('month', d) + INTERVAL '1 month'
                                        - INTERVAL '1 day')::DATE         AS is_month_end,
            EXTRACT(DAY FROM d) = 1
                AND EXTRACT(MONTH FROM d) IN (1, 4, 7, 10)               AS is_quarter_start,
            d = (DATE_TRUNC('quarter', d) + INTERVAL '3 months'
                                          - INTERVAL '1 day')::DATE       AS is_quarter_end
        FROM generate_series(%s::DATE, %s::DATE, '1 day'::INTERVAL) AS d
        ON CONFLICT (date_id) DO NOTHING;
        """,
        analytics_db,
        (range_start, range_end),
    )

    logger.info("Ensured dim_date coverage for %s to %s", range_start, range_end)
    return {"range_start": range_start, "range_end": range_end}


def load_fact_from_dim_policy(
    fact_table: str,
    annual_premium_source: str,
    analytics_db: Dict[str, Any],
    pending_batch_dates: List[str],
) -> int:
    if not pending_batch_dates:
        logger.info("No pending fact batches")
        return 0

    execute_sql(
        f"""
        INSERT INTO {fact_table} (
            policy_id,
            driver_id,
            vehicle_id,
            coverage_id,
            date_id,
            annual_premium_amount
        )
        SELECT
            p.policy_id,
            dd.driver_id,
            dv.vehicle_id,
            dc.coverage_id,
            TO_CHAR(p.source_batch_date, 'YYYYMMDD')::INTEGER AS date_id,
            p.{annual_premium_source}                         AS annual_premium_amount
        FROM dim_policy p
        JOIN dim_driver dd ON dd.source_driver_id = p.source_driver_id
        JOIN dim_vehicle dv ON dv.source_vehicle_id = p.source_vehicle_id
        JOIN dim_coverage dc ON dc.source_coverage_id = p.source_coverage_id
        WHERE p.source_batch_date::text = ANY(%s)
        ON CONFLICT (policy_id) DO UPDATE
        SET
            driver_id             = EXCLUDED.driver_id,
            vehicle_id            = EXCLUDED.vehicle_id,
            coverage_id           = EXCLUDED.coverage_id,
            date_id               = EXCLUDED.date_id,
            annual_premium_amount = EXCLUDED.annual_premium_amount,
            loaded_at             = CURRENT_TIMESTAMP;
        """,
        analytics_db,
        params=(sorted(pending_batch_dates),),
    )

    fact_count = query_scalar(f"SELECT COUNT(*) FROM {fact_table};", analytics_db)
    logger.info("Loaded fact_policy for batches %s", pending_batch_dates)
    return fact_count
