import csv
import os
import random
from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Tuple

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")


def ensure_dirs() -> None:
    os.makedirs(RAW_DIR, exist_ok=True)


@dataclass
class Product:
    product_id: int
    product_name: str
    category: str
    subcategory: str


@dataclass
class Customer:
    customer_id: int
    customer_name: str
    segment: str
    city: str
    country: str


@dataclass
class Store:
    store_id: int
    store_name: str
    city: str
    region: str
    country: str


@dataclass
class Channel:
    channel_id: int
    channel_name: str


@dataclass
class Salesperson:
    salesperson_id: int
    salesperson_name: str
    team: str


@dataclass
class Promotion:
    promotion_id: int
    promotion_name: str
    promo_type: str


def daterange(start: date, end: date) -> List[date]:
    days = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def write_csv(path: str, rows: List[dict]) -> Tuple[str, int]:
    if not rows:
        return path, 0
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    return path, len(rows)


def generate_dim_date(start: date, end: date) -> Tuple[str, int]:
    rows = []
    for d in daterange(start, end):
        date_id = int(d.strftime("%Y%m%d"))
        rows.append(
            {
                "date_id": date_id,
                "date": d.isoformat(),
                "year": d.year,
                "quarter": (d.month - 1) // 3 + 1,
                "month": d.month,
                "day_of_week": d.isoweekday(),
            }
        )
    return write_csv(os.path.join(RAW_DIR, "dim_date.csv"), rows)


def generate_dim_product(n: int = 150) -> Tuple[str, int]:
    random.seed(11)
    categories = {
        "Electronics": ["Phones", "Laptops", "Accessories"],
        "Home": ["Furniture", "Kitchen", "Decor"],
        "Sports": ["Outdoor", "Fitness", "Apparel"],
    }
    rows = []
    for pid in range(1, n + 1):
        category = random.choice(list(categories.keys()))
        subcategory = random.choice(categories[category])
        name = f"{subcategory[:-1] if subcategory.endswith('s') else subcategory} Model {pid:03d}"
        rows.append(
            {
                "product_id": pid,
                "product_name": name,
                "category": category,
                "subcategory": subcategory,
            }
        )
    return write_csv(os.path.join(RAW_DIR, "dim_product.csv"), rows)


def generate_dim_customer(n: int = 300) -> Tuple[str, int]:
    random.seed(12)
    segments = ["Consumer", "Corporate", "Home Office", "Small Business"]
    cities = [
        ("Jakarta", "Indonesia"),
        ("Bandung", "Indonesia"),
        ("Surabaya", "Indonesia"),
        ("Singapore", "Singapore"),
        ("Kuala Lumpur", "Malaysia"),
        ("Bangkok", "Thailand"),
        ("Manila", "Philippines"),
    ]
    rows = []
    for cid in range(1, n + 1):
        city, country = random.choice(cities)
        rows.append(
            {
                "customer_id": cid,
                "customer_name": f"Customer {cid:04d}",
                "segment": random.choice(segments),
                "city": city,
                "country": country,
            }
        )
    return write_csv(os.path.join(RAW_DIR, "dim_customer.csv"), rows)


def generate_dim_store(n: int = 80) -> Tuple[str, int]:
    random.seed(13)
    regions = {
        "Indonesia": [
            ("Jakarta", "Java"),
            ("Bandung", "Java"),
            ("Surabaya", "Java"),
            ("Medan", "Sumatra"),
            ("Denpasar", "Bali"),
        ],
        "Singapore": [("Singapore", "Central")],
        "Malaysia": [("Kuala Lumpur", "Klang Valley"), ("Penang", "Penang")],
        "Thailand": [("Bangkok", "Central"), ("Chiang Mai", "North")],
        "Philippines": [("Manila", "NCR"), ("Cebu", "Central Visayas")],
    }
    rows = []
    for sid in range(1, n + 1):
        country = random.choice(list(regions.keys()))
        city, region = random.choice(regions[country])
        rows.append(
            {
                "store_id": sid,
                "store_name": f"Store {sid:03d}",
                "city": city,
                "region": region,
                "country": country,
            }
        )
    return write_csv(os.path.join(RAW_DIR, "dim_store.csv"), rows)


def generate_dim_channel() -> Tuple[str, int]:
    rows = []
    channels = ["Online", "Retail", "Wholesale", "Marketplace", "Partner"]
    for i, ch in enumerate(channels, start=1):
        rows.append({"channel_id": i, "channel_name": ch})
    return write_csv(os.path.join(RAW_DIR, "dim_channel.csv"), rows)


def generate_dim_salesperson(n: int = 120) -> Tuple[str, int]:
    random.seed(14)
    teams = ["Alpha", "Bravo", "Charlie", "Delta", "Echo"]
    rows = []
    for spid in range(1, n + 1):
        rows.append(
            {
                "salesperson_id": spid,
                "salesperson_name": f"Sales {spid:03d}",
                "team": random.choice(teams),
            }
        )
    return write_csv(os.path.join(RAW_DIR, "dim_salesperson.csv"), rows)


def generate_dim_promotion(n: int = 24) -> Tuple[str, int]:
    random.seed(15)
    promo_types = ["Discount", "Bundle", "BOGO", "Coupon", "Seasonal"]
    rows = []
    for pid in range(1, n + 1):
        rows.append(
            {
                "promotion_id": pid,
                "promotion_name": f"Promo {pid:02d}",
                "promo_type": random.choice(promo_types),
            }
        )
    return write_csv(os.path.join(RAW_DIR, "dim_promotion.csv"), rows)


def generate_fact_sales(
    num_rows: int,
    date_ids: List[int],
    max_product_id: int,
    max_customer_id: int,
    max_store_id: int,
    max_channel_id: int,
    max_salesperson_id: int,
    max_promotion_id: int,
) -> Tuple[str, int]:
    random.seed(16)
    rows = []
    for sid in range(1, num_rows + 1):
        date_id = random.choice(date_ids)
        product_id = random.randint(1, max_product_id)
        customer_id = random.randint(1, max_customer_id)
        store_id = random.randint(1, max_store_id)
        channel_id = random.randint(1, max_channel_id)
        salesperson_id = random.randint(1, max_salesperson_id)
        # Promotions are not always present
        promotion_id = (
            random.randint(1, max_promotion_id) if random.random() < 0.35 else 0
        )
        quantity = max(1, int(random.expovariate(1 / 3)) + 1)
        unit_price = round(random.uniform(5.0, 1200.0), 2)
        discount_rate = round(
            random.choice([0, 0.05, 0.1, 0.15, 0.2]) if random.random() < 0.5 else 0.0,
            2,
        )
        gross = quantity * unit_price
        discount_amount = gross * discount_rate
        net_sales = round(gross - discount_amount, 2)
        cost_amount = round(gross * random.uniform(0.4, 0.8), 2)
        rows.append(
            {
                "sales_id": sid,
                "date_id": date_id,
                "product_id": product_id,
                "customer_id": customer_id,
                "store_id": store_id,
                "channel_id": channel_id,
                "salesperson_id": salesperson_id,
                "promotion_id": promotion_id,
                "quantity": quantity,
                "unit_price": f"{unit_price:.2f}",
                "discount_rate": f"{discount_rate:.2f}",
                "sales_amount": f"{net_sales:.2f}",
                "cost_amount": f"{cost_amount:.2f}",
            }
        )
    return write_csv(os.path.join(RAW_DIR, "fact_sales.csv"), rows)


def main():
    ensure_dirs()

    # Date range for the dataset
    end = date.today()
    start = end - timedelta(days=180)

    # Generate dimensions
    _, d_date = generate_dim_date(start, end)
    _, d_prod = generate_dim_product(180)
    _, d_cust = generate_dim_customer(400)
    _, d_store = generate_dim_store(100)
    _, d_chan = generate_dim_channel()
    _, d_sales = generate_dim_salesperson(150)
    _, d_promo = generate_dim_promotion(30)

    # Gather date IDs
    date_ids = []
    with open(os.path.join(RAW_DIR, "dim_date.csv"), "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            date_ids.append(int(row["date_id"]))

    # Generate fact table
    _, f_rows = generate_fact_sales(
        num_rows=20000,
        date_ids=date_ids,
        max_product_id=d_prod,
        max_customer_id=d_cust,
        max_store_id=d_store,
        max_channel_id=d_chan,
        max_salesperson_id=d_sales,
        max_promotion_id=d_promo,
    )

    print(
        {
            "dim_date": d_date,
            "dim_product": d_prod,
            "dim_customer": d_cust,
            "dim_store": d_store,
            "dim_channel": d_chan,
            "dim_salesperson": d_sales,
            "dim_promotion": d_promo,
            "fact_sales": f_rows,
            "raw_dir": RAW_DIR,
        }
    )


if __name__ == "__main__":
    main()
