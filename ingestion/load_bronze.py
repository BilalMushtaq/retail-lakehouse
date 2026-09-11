import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import List

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "retail")
DB_USER = os.getenv("DB_USER", "rl_user")
DB_PASS = os.getenv("DB_PASS", "rl_pass")

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2

BATCH_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )


def load_csv_to_table(conn, csv_path: str, table: str, cols: List[str]) -> None:
    """Load a single CSV into a table, retrying on transient DB errors."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Expected input file not found: {csv_path}")

    df = pd.read_csv(csv_path)
    df["batch_id"] = BATCH_ID
    df = df[cols]  # enforce column order for the insert

    tuples = [tuple(x) for x in df.to_numpy()]
    insert_sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s"

    attempt = 0
    while True:
        attempt += 1
        try:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, tuples, page_size=1000)
            conn.commit()
            logger.info("Loaded %d rows into %s (batch_id=%s)", len(df), table, BATCH_ID)
            return
        except psycopg2.OperationalError as exc:
            conn.rollback()
            if attempt >= MAX_RETRIES:
                logger.error("Giving up on %s after %d attempts: %s", table, attempt, exc)
                raise
            wait = RETRY_BACKOFF_SECONDS * attempt
            logger.warning(
                "Transient error loading %s (attempt %d/%d): %s — retrying in %ds",
                table, attempt, MAX_RETRIES, exc, wait,
            )
            time.sleep(wait)
        except Exception:
            # Non-transient errors (bad SQL, schema mismatch, etc.) shouldn't retry.
            conn.rollback()
            logger.exception("Non-retryable error loading %s", table)
            raise


def main() -> int:
    conn = None
    try:
        conn = get_conn()
        logger.info("Connected to %s:%s/%s", DB_HOST, DB_PORT, DB_NAME)

        load_csv_to_table(
            conn, "customers.csv", "bronze.customers_raw",
            ["customer_id", "first_name", "last_name", "email", "phone",
             "city", "state", "created_at", "batch_id"],
        )
        load_csv_to_table(
            conn, "products.csv", "bronze.products_raw",
            ["product_id", "sku", "product_name", "category", "brand",
             "unit_cost", "unit_price", "active", "batch_id"],
        )
        load_csv_to_table(
            conn, "pos_sales.csv", "bronze.pos_sales_raw",
            ["sale_id", "sale_ts", "store_id", "customer_id", "product_id",
             "quantity", "unit_price", "discount", "payment_method", "batch_id"],
        )
        load_csv_to_table(
            conn, "ecom_orders.csv", "bronze.ecom_orders_raw",
            ["order_id", "order_ts", "customer_id", "channel", "order_total",
             "shipping_fee", "tax", "status", "batch_id"],
        )
        load_csv_to_table(
            conn, "inventory.csv", "bronze.inventory_raw",
            ["snapshot_ts", "store_id", "product_id", "on_hand_qty", "batch_id"],
        )
        load_csv_to_table(
            conn, "ecom_order_items.csv", "bronze.ecom_order_items_raw",
            ["order_id", "product_id", "quantity", "unit_price", "line_total", "batch_id"],
        )

    except Exception:
        logger.exception("Bronze load failed")
        return 1
    finally:
        if conn is not None:
            conn.close()
            logger.info("Database connection closed")

    logger.info("Bronze load complete (batch_id=%s)", BATCH_ID)
    return 0


if __name__ == "__main__":
    sys.exit(main())
