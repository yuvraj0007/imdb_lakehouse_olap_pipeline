import logging
import time

import clickhouse_connect

from src.config import (
    CLICKHOUSE_DB,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
)

logger = logging.getLogger(__name__)


def get_client():
    logger.info(f"Connecting to ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}")

    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )

    version = client.query("SELECT version()").result_rows[0][0]
    logger.info(f"  Connected to ClickHouse {version}")
    return client


def wait_for_clickhouse(max_retries: int = 30, delay: int = 2) -> bool:
    logger.info("Waiting for ClickHouse to be ready...")

    for attempt in range(1, max_retries + 1):
        try:
            client = get_client()
            client.close()
            logger.info("  ClickHouse is ready!")
            return True
        except Exception as e:
            if attempt < max_retries:
                logger.info(f"  Attempt {attempt}/{max_retries}: {e}. Retrying in {delay}s...")
                time.sleep(delay)
            else:
                logger.error(f"ClickHouse not available after {max_retries} attempts")
                return False
    return False
