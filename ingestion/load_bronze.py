import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "retail")
DB_USER = os.getenv("DB_USER", "rl_user")
DB_PASS = os.getenv("DB_PASS", "rl_pass")

BATCH_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def get_conn():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )

def load_csv_to_table(csv_path: str, table: str, cols: list[str]):
    df = pd.read_csv(csv_path)
    df["batch_id"] = BATCH_ID

    # Ensure correct column ordering for insert
    df = df[cols]

    tuples = [tuple(x) for x in df.to_numpy()]

    insert_sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s"

    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, tuples, page_size=1000)
        conn.commit()

    print(f"Loaded {len(df)} rows into {table} (batch_id={BATCH_ID})")

if __name__ == "__main__":
    # customers_raw columns (excluding record_id, loaded_at which are auto)
    load_csv_to_table(
        "customers.csv",
        "bronze.customers_raw",
        ["customer_id","first_name","last_name","email","phone","city","state","created_at","batch_id"]
    )

    # products_raw columns (excluding record_id, loaded_at which are auto)
    load_csv_to_table(
        "products.csv",
        "bronze.products_raw",
        ["product_id","sku","product_name","category","brand","unit_cost","unit_price","active","batch_id"]
    )

    # POS Sales
    load_csv_to_table(
        "pos_sales.csv",
        "bronze.pos_sales_raw",
        ["sale_id", "sale_ts", "store_id", "customer_id", "product_id", "quantity", "unit_price", "discount", "payment_method", "batch_id"]
    )

    # E-commerce Orders
    load_csv_to_table(
        "ecom_orders.csv",
        "bronze.ecom_orders_raw",
        ["order_id", "order_ts", "customer_id", "channel", "order_total", "shipping_fee", "tax", "status", "batch_id"]
    )

    # Inventory
    load_csv_to_table(
        "inventory.csv",
        "bronze.inventory_raw",
        ["snapshot_ts", "store_id", "product_id", "on_hand_qty", "batch_id"]
    )

    # E-commerce Order Items
    load_csv_to_table(
        "ecom_order_items.csv",
        "bronze.ecom_order_items_raw",
        ["order_id", "product_id", "quantity", "unit_price", "line_total", "batch_id"]
    )

