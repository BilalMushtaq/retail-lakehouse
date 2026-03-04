import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta
import uuid

fake = Faker()

# Configuration
NUM_CUSTOMERS = 200
NUM_PRODUCTS = 100
NUM_STORES = 5
DAYS_BACK = 30
AVG_POS_LINES_PER_DAY = 120
AVG_ECOM_ORDERS_PER_DAY = 35
INVENTORY_SNAPSHOTS_PER_DAY = 1


def generate_customers(n=NUM_CUSTOMERS):
    customers = []
    for _ in range(n):
        customer_id = str(uuid.uuid4())
        customers.append({
            "customer_id": customer_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "created_at": fake.date_time_between(start_date="-2y", end_date="now")
        })
    return pd.DataFrame(customers)


def generate_products(n=NUM_PRODUCTS):
    categories = ["Electronics", "Clothing", "Home", "Beauty", "Sports"]
    brands = ["BrandA", "BrandB", "BrandC", "BrandD"]

    products = []
    for i in range(n):
        product_id = f"P{i+1:04d}"
        unit_cost = round(random.uniform(5, 50), 2)
        unit_price = round(unit_cost * random.uniform(1.2, 2.5), 2)

        products.append({
            "product_id": product_id,
            "sku": fake.ean13(),
            "product_name": fake.word().capitalize(),
            "category": random.choice(categories),
            "brand": random.choice(brands),
            "unit_cost": unit_cost,
            "unit_price": unit_price,
            "active": random.choice([True, True, True, False])
        })
    return pd.DataFrame(products)

def generate_pos_sales(customers_df, products_df):
    store_ids = [f"S{i+1:03d}" for i in range(NUM_STORES)]
    payment_methods = ["cash", "card", "apple_pay", "google_pay"]

    rows = []
    start_date = datetime.now() - timedelta(days=DAYS_BACK)

    sale_counter = 1
    for day in range(DAYS_BACK):
        date = start_date + timedelta(days=day)
        # vary daily volume
        daily_lines = max(20, int(random.gauss(AVG_POS_LINES_PER_DAY, 25)))

        for _ in range(daily_lines):
            sale_id = f"POS{sale_counter:08d}"
            sale_counter += 1

            cust = customers_df.sample(1).iloc[0]
            prod = products_df.sample(1).iloc[0]
            qty = random.randint(1, 4)
            unit_price = float(prod["unit_price"])
            discount = round(random.choice([0, 0, 0, 0.5, 1.0, 2.0]), 2)

            rows.append({
                "sale_id": sale_id,
                "sale_ts": (date + timedelta(minutes=random.randint(0, 1439))).isoformat(),
                "store_id": random.choice(store_ids),
                "customer_id": cust["customer_id"],
                "product_id": prod["product_id"],
                "quantity": qty,
                "unit_price": unit_price,
                "discount": discount,
                "payment_method": random.choice(payment_methods),
            })

    return pd.DataFrame(rows)


def generate_ecom_orders(customers_df, products_df):
    statuses = ["placed", "shipped", "delivered", "cancelled"]
    rows = []
    start_date = datetime.now() - timedelta(days=DAYS_BACK)

    order_counter = 1
    for day in range(DAYS_BACK):
        date = start_date + timedelta(days=day)
        daily_orders = max(5, int(random.gauss(AVG_ECOM_ORDERS_PER_DAY, 10)))

        for _ in range(daily_orders):
            cust = customers_df.sample(1).iloc[0]

            # order total is simulated (not line-level here)
            subtotal = round(random.uniform(15, 250), 2)
            shipping_fee = round(random.choice([0, 4.99, 7.99, 9.99]), 2)
            tax = round(subtotal * 0.0825, 2)  # TX-ish sales tax
            order_total = round(subtotal + shipping_fee + tax, 2)

            status = random.choices(statuses, weights=[55, 20, 20, 5])[0]

            rows.append({
                "order_id": f"ECOM{order_counter:08d}",
                "order_ts": (date + timedelta(minutes=random.randint(0, 1439))).isoformat(),
                "customer_id": cust["customer_id"],
                "channel": "web",
                "order_total": order_total,
                "shipping_fee": shipping_fee,
                "tax": tax,
                "status": status,
            })
            order_counter += 1

    return pd.DataFrame(rows)


def generate_inventory(products_df):
    store_ids = [f"S{i+1:03d}" for i in range(NUM_STORES)]
    rows = []
    start_date = datetime.now() - timedelta(days=DAYS_BACK)

    for day in range(DAYS_BACK):
        date = start_date + timedelta(days=day)

        # one snapshot per day
        snapshot_ts = (date + timedelta(hours=23, minutes=59)).isoformat()

        for _, prod in products_df.iterrows():
            for store in store_ids:
                rows.append({
                    "snapshot_ts": snapshot_ts,
                    "store_id": store,
                    "product_id": prod["product_id"],
                    "on_hand_qty": random.randint(0, 200),
                })

    return pd.DataFrame(rows)


def generate_ecom_order_items(ecom_df, products_df):
    rows = []

    for _, order in ecom_df.iterrows():
        num_items = random.randint(1, 5)
        sampled_products = products_df.sample(num_items)

        remaining_total = order["order_total"]

        for i, (_, product) in enumerate(sampled_products.iterrows()):
            if i == num_items - 1:
                line_total = remaining_total
            else:
                line_total = round(random.uniform(5, remaining_total / 2), 2)
                remaining_total -= line_total

            quantity = random.randint(1, 3)
            unit_price = round(line_total / quantity, 2)

            rows.append({
                "order_id": order["order_id"],
                "product_id": product["product_id"],
                "quantity": quantity,
                "unit_price": unit_price,
                "line_total": round(quantity * unit_price, 2)
            })

    return pd.DataFrame(rows)


if __name__ == "__main__":
    customers_df = generate_customers()
    products_df = generate_products()

    customers_df.to_csv("customers.csv", index=False)
    products_df.to_csv("products.csv", index=False)

    print("Generated:")
    print(f"- {len(customers_df)} customers → customers.csv")
    print(f"- {len(products_df)} products → products.csv")

    pos_df = generate_pos_sales(customers_df, products_df)
    ecom_df = generate_ecom_orders(customers_df, products_df)
    inv_df = generate_inventory(products_df)

    pos_df.to_csv("pos_sales.csv", index=False)
    ecom_df.to_csv("ecom_orders.csv", index=False)
    inv_df.to_csv("inventory.csv", index=False)

    print(f"- {len(pos_df)} POS sale lines → pos_sales.csv")
    print(f"- {len(ecom_df)} ecom orders → ecom_orders.csv")
    print(f"- {len(inv_df)} inventory rows → inventory.csv")

    ecom_items_df = generate_ecom_order_items(ecom_df, products_df)
    ecom_items_df.to_csv("ecom_order_items.csv", index=False)

    print(f"- {len(ecom_items_df)} ecom order items → ecom_order_items.csv")
