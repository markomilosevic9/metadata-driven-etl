import logging
from datetime import datetime

from minio.error import S3Error

from pipeline.core.config_loader import create_minio_client


logger = logging.getLogger(__name__)


def discover_batch_dates(
    bucket: str,
    storage_config: dict,
) -> list[str]:
    # discover distinct batch dates from a minio bucket by inspecting object keys
    client = create_minio_client(storage_config)
    batch_dates = set()

    for obj in client.list_objects(bucket, prefix="batch-", recursive=False):
        object_name = obj.object_name.rstrip("/")
        if not object_name.startswith("batch-"):
            continue
        batch_date = object_name[len("batch-"):]
        try:
            datetime.strptime(batch_date, "%Y-%m-%d")
            batch_dates.add(batch_date)
        except ValueError:
            continue

    return sorted(batch_dates)


def parse_s3a_path(path: str) -> tuple[str, str]:
    without_scheme = path.replace("s3a://", "", 1)
    bucket, _, prefix = without_scheme.partition("/")
    return bucket, prefix.rstrip("/")


def path_exists(storage_config: dict, path: str) -> bool:
    bucket, prefix = parse_s3a_path(path)
    try:
        client = create_minio_client(storage_config)
        return any(
            True
            for _ in client.list_objects(
                bucket,
                prefix=prefix.rstrip("/"),
                recursive=False,
            )
        )
    except S3Error:
        logger.warning("path_exists check failed for %s", path)
        return False
