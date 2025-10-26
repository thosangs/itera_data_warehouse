import csv
import os
import random
from datetime import UTC, datetime, timedelta

RAW_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "raw"))


def ensure_dirs() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)


def generate_customers(num_customers: int = 50):
    random.seed(42)
    cities = [
        "Jakarta",
        "Bandung",
        "Surabaya",
        "Medan",
        "Yogyakarta",
        "Denpasar",
    ]
    now = datetime.now(UTC)
    rows = []
    for cid in range(1, num_customers + 1):
        name = f"Customer {cid:03d}"
        city = random.choice(cities)
        updated_at = now - timedelta(minutes=random.randint(0, 10_000))
        rows.append(
            {
                "customer_id": cid,
                "name": name,
                "city": city,
                "updated_at": updated_at.isoformat(),
            }
        )

    path = os.path.join(RAW_DIR, "customers.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return path, len(rows)


def generate_orders(num_orders: int = 200, max_customer_id: int = 50):
    random.seed(99)
    statuses = ["new", "processing", "shipped", "cancelled"]
    now = datetime.now(UTC)
    rows = []
    for oid in range(1, num_orders + 1):
        customer_id = random.randint(1, max_customer_id)
        amount = round(random.uniform(10, 1000), 2)
        status = random.choice(statuses)
        updated_at = now - timedelta(minutes=random.randint(0, 10_000))
        rows.append(
            {
                "order_id": oid,
                "customer_id": customer_id,
                "amount": f"{amount:.2f}",
                "status": status,
                "updated_at": updated_at.isoformat(),
            }
        )

    path = os.path.join(RAW_DIR, "orders.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return path, len(rows)


def main():
    ensure_dirs()
    customers_path, c_rows = generate_customers()
    orders_path, o_rows = generate_orders()
    print(
        {
            "customers_path": customers_path,
            "customers_rows": c_rows,
            "orders_path": orders_path,
            "orders_rows": o_rows,
        }
    )


if __name__ == "__main__":
    main()
