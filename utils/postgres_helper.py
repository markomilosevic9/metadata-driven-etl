import logging
from contextlib import contextmanager
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

# all pipeline code that talks to postgres imports from here

def open_connection(analytics_db: Dict[str, Any]):
    # open a raw connection from an analytics_db config dict
    return psycopg2.connect(
        host=analytics_db["host"],
        port=analytics_db["port"],
        database=analytics_db["database"],
        user=analytics_db["user"],
        password=analytics_db["password"],
    )


@contextmanager
def connect_postgres(analytics_db: Dict[str, Any]):
    # context manager that yields an open connection
    conn = open_connection(analytics_db)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def cursor_context(
    analytics_db: Dict[str, Any],
    cursor_factory=psycopg2.extras.RealDictCursor,
):
    conn = open_connection(analytics_db)
    try:
        with conn.cursor(cursor_factory=cursor_factory) as cur:
            yield cur
    finally:
        conn.close()


def get_jdbc_config(config: Dict[str, Any]) -> Dict[str, Any]:
    # build spark jdbc connection settings from the pipeline config
    analytics_db = config["analytics_db"]
    jdbc_url = (
        f"jdbc:postgresql://{analytics_db['host']}:{analytics_db['port']}"
        f"/{analytics_db['database']}"
    )
    return {
        "url": jdbc_url,
        "properties": {
            "user": analytics_db["user"],
            "password": analytics_db["password"],
            "driver": analytics_db.get("driver", "org.postgresql.Driver"),
        },
    }


def execute_sql(
    sql: str,
    analytics_db: Dict[str, Any],
    params: Optional[tuple] = None,
) -> None:
    # execute a single write statement with auto commit/rollback
    # opens a fresh connection, executes the statement, commits on success, rolls back and re-raises on any exception
    conn = open_connection(analytics_db)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def query_scalar(
    sql: str,
    analytics_db: Dict[str, Any],
    params: Optional[tuple] = None,
) -> Any:
    # execute a read query that returns a single scalar value
    with cursor_context(analytics_db) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return next(iter(row.values())) if row else None


def query_dict(
    sql: str,
    analytics_db: Dict[str, Any],
    params: Optional[tuple] = None,
) -> Dict[str, Any]:
    # execute a read query that returns a single row as a plain dict
    with cursor_context(analytics_db) as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else {}


def query_list(
    sql: str,
    analytics_db: Dict[str, Any],
    params: Optional[tuple] = None,
) -> List[Any]:
    # execute a read query that returns the first column of every row
    with cursor_context(analytics_db, cursor_factory=None) as cur:
        cur.execute(sql, params)
        return [row[0] for row in cur.fetchall()]


def filter_unprocessed_batches(
    available_batch_dates: List[str],
    completed_batch_dates: List[str],
) -> List[str]:
    completed = set(completed_batch_dates)
    normalized = sorted({str(batch_date)[:10] for batch_date in available_batch_dates})
    return [batch_date for batch_date in normalized if batch_date not in completed]


def get_unprocessed_ingestion_batches(
    available_batch_dates: List[str],
    analytics_db: Dict[str, Any],
) -> List[str]:
    completed_batches = query_list(
        """
        SELECT batch_date::text
        FROM etl_ingestion_batches
        ORDER BY batch_date
        """,
        analytics_db,
    )
    return filter_unprocessed_batches(available_batch_dates, completed_batches)


def mark_ingestion_batch_done(
    batch_date: str,
    run_id: str,
    analytics_db: Dict[str, Any],
    input_record_count: Optional[int] = None,
) -> None:
    execute_sql(
        """
        INSERT INTO etl_ingestion_batches (batch_date, run_id, processed_at, input_record_count)
        VALUES (%s, %s, CURRENT_TIMESTAMP, %s)
        ON CONFLICT (batch_date) DO UPDATE
        SET
            run_id = EXCLUDED.run_id,
            processed_at = CURRENT_TIMESTAMP,
            input_record_count = EXCLUDED.input_record_count
        """,
        analytics_db,
        (batch_date, run_id, input_record_count),
    )


def mark_run_started(run_id: str, analytics_db: Dict[str, Any]) -> None:
    execute_sql(
        """
        INSERT INTO etl_runs (run_id, started_at, status)
        VALUES (%s, CURRENT_TIMESTAMP, 'running')
        ON CONFLICT (run_id) DO UPDATE
        SET
            started_at = CURRENT_TIMESTAMP,
            status = 'running',
            completed_at = NULL,
            error_text = NULL
        """,
        analytics_db,
        (run_id,),
    )


def mark_run_finished(
    run_id: str,
    status: str,
    analytics_db: Dict[str, Any],
    error_text: Optional[str] = None,
) -> None:
    execute_sql(
        """
        UPDATE etl_runs
        SET
            completed_at = CURRENT_TIMESTAMP,
            status = %s,
            error_text = %s
        WHERE run_id = %s
        """,
        analytics_db,
        (status, error_text, run_id),
    )


def list_completed_scd_batches(
    analytics_db: Dict[str, Any],
    dimension_name: str,
) -> List[str]:
    return query_list(
        """
        SELECT batch_date::text
        FROM etl_scd_batches
        WHERE dimension_name = %s
        ORDER BY batch_date
        """,
        analytics_db,
        (dimension_name,),
    )


def get_unprocessed_scd_batches(
    available_batch_dates: List[str],
    analytics_db: Dict[str, Any],
    dimension_name: str,
) -> List[str]:
    return filter_unprocessed_batches(
        available_batch_dates,
        list_completed_scd_batches(analytics_db, dimension_name),
    )


def get_pending_star_batches(
    analytics_db: Dict[str, Any],
    dimension_name: str,
) -> List[str]:
    return query_list(
        """
        SELECT batch_date::text
        FROM etl_scd_batches
        WHERE dimension_name = %s
          AND warehouse_loaded_at IS NULL
        ORDER BY batch_date
        """,
        analytics_db,
        (dimension_name,),
    )


def get_scd_batch_run_ids(
    analytics_db: Dict[str, Any],
    dimension_name: str,
    batch_dates: List[str],
) -> List[str]:
    if not batch_dates:
        return []
    return query_list(
        """
        SELECT DISTINCT run_id
        FROM etl_scd_batches
        WHERE dimension_name = %s
          AND batch_date::text = ANY(%s)
          AND warehouse_loaded_at IS NULL
        ORDER BY run_id
        """,
        analytics_db,
        (dimension_name, sorted(batch_dates)),
    )


def get_latest_dimension_contract_version(
    analytics_db: Dict[str, Any],
    dimension_name: str,
) -> Optional[str]:
    return query_scalar(
        """
        SELECT contract_version
        FROM etl_scd_batches
        WHERE dimension_name = %s
          AND contract_version IS NOT NULL
        ORDER BY processed_at DESC, batch_date DESC
        LIMIT 1
        """,
        analytics_db,
        (dimension_name,),
    )

def mark_scd_batch_done(
    batch_date: str,
    dimension_name: str,
    run_id: str,
    contract_version: str,
    analytics_db: Dict[str, Any],
) -> None:
    execute_sql(
        """
        INSERT INTO etl_scd_batches (
            batch_date,
            dimension_name,
            contract_version,
            run_id,
            processed_at,
            warehouse_loaded_at
        )
        VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, NULL)
        ON CONFLICT (batch_date, dimension_name) DO UPDATE
        SET
            contract_version = EXCLUDED.contract_version,
            run_id = EXCLUDED.run_id,
            processed_at = CURRENT_TIMESTAMP,
            warehouse_loaded_at = NULL
        """,
        analytics_db,
        (batch_date, dimension_name, contract_version, run_id),
    )


def mark_scd_batch_warehouse_loaded(
    batch_date: str,
    dimension_name: str,
    analytics_db: Dict[str, Any],
) -> None:
    execute_sql(
        """
        UPDATE etl_scd_batches
        SET
            warehouse_loaded_at = CURRENT_TIMESTAMP
        WHERE batch_date = %s
          AND dimension_name = %s
        """,
        analytics_db,
        (batch_date, dimension_name),
    )
