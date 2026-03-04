-- Raw POS sales (line-level)
CREATE TABLE IF NOT EXISTS bronze.pos_sales_raw (
  record_id       BIGSERIAL PRIMARY KEY,
  batch_id        TEXT NOT NULL,
  loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  sale_id         TEXT,
  sale_ts         TIMESTAMPTZ,
  store_id        TEXT,
  customer_id     TEXT,
  product_id      TEXT,
  quantity        INT,
  unit_price      NUMERIC(12,2),
  discount        NUMERIC(12,2),
  payment_method  TEXT
);

-- Raw e-commerce orders (order-level)
CREATE TABLE IF NOT EXISTS bronze.ecom_orders_raw (
  record_id       BIGSERIAL PRIMARY KEY,
  batch_id        TEXT NOT NULL,
  loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  order_id        TEXT,
  order_ts        TIMESTAMPTZ,
  customer_id     TEXT,
  channel         TEXT,
  order_total     NUMERIC(12,2),
  shipping_fee    NUMERIC(12,2),
  tax             NUMERIC(12,2),
  status          TEXT
);

-- Raw customers
CREATE TABLE IF NOT EXISTS bronze.customers_raw (
  record_id       BIGSERIAL PRIMARY KEY,
  batch_id        TEXT NOT NULL,
  loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  customer_id     TEXT,
  first_name      TEXT,
  last_name       TEXT,
  email           TEXT,
  phone           TEXT,
  city            TEXT,
  state           TEXT,
  created_at      TIMESTAMPTZ
);

-- Raw products
CREATE TABLE IF NOT EXISTS bronze.products_raw (
  record_id       BIGSERIAL PRIMARY KEY,
  batch_id        TEXT NOT NULL,
  loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  product_id      TEXT,
  sku             TEXT,
  product_name    TEXT,
  category        TEXT,
  brand           TEXT,
  unit_cost       NUMERIC(12,2),
  unit_price      NUMERIC(12,2),
  active          BOOLEAN
);

-- Raw inventory snapshots
CREATE TABLE IF NOT EXISTS bronze.inventory_raw (
  record_id       BIGSERIAL PRIMARY KEY,
  batch_id        TEXT NOT NULL,
  loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  snapshot_ts     TIMESTAMPTZ,
  store_id        TEXT,
  product_id      TEXT,
  on_hand_qty     INT
);
