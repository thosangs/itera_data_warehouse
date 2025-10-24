#!/usr/bin/env python3
import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[1] / "data"


def month_start(d: date) -> date:
    return d.replace(day=1)


def random_month_date(start_year: int, end_year: int, rng: random.Random) -> date:
    """Return a date representing the first day of a random month in [start_year, end_year]."""
    year = rng.randint(start_year, end_year)
    month = rng.randint(1, 12)
    return date(year, month, 1)


def write_clubs(
    n_rows: int, start_year: int, end_year: int, rng: random.Random
) -> None:
    filenames = [
        ("clubs_orchestra.csv", "Orchestra"),
        ("clubs_business.csv", "Business"),
        ("clubs_japanese.csv", "Japanese"),
    ]
    for filename, club_name in filenames:
        with (DATA_DIR / filename).open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["ClubName", "Year", "Month", "Date", "TotalIncome", "TotalExpenditure"]
            )
            for _ in range(n_rows):
                month_date = random_month_date(start_year, end_year, rng)
                y = month_date.year
                m = month_date.month
                base = 20000 + (y - start_year) * 500 + m * 100
                variance = rng.randint(-2000, 3000)
                income = max(1000, base + variance)
                expenditure = max(500, int(income * rng.uniform(0.6, 0.95)))
                writer.writerow(
                    [
                        club_name,
                        y,
                        month_date.isoformat(),
                        month_date.isoformat(),
                        f"{income:.2f}",
                        f"{expenditure:.2f}",
                    ]
                )


def write_property(
    n_rows: int, start_year: int, end_year: int, rng: random.Random
) -> None:
    suburbs = ["Richmond", "Fitzroy", "Carlton", "Brunswick"]
    # Generate a shared key list of exactly n (Suburb, Month) pairs for alignment across files
    keys = []
    while len(keys) < n_rows:
        keys.append((rng.choice(suburbs), random_month_date(start_year, end_year, rng)))

    with (
        (DATA_DIR / "property_inspection.csv").open("w", newline="") as fi,
        (DATA_DIR / "property_auction.csv").open("w", newline="") as fa,
        (DATA_DIR / "property_sales.csv").open("w", newline="") as fs,
    ):
        wi = csv.writer(fi)
        wa = csv.writer(fa)
        ws = csv.writer(fs)
        wi.writerow(["Suburb", "Month", "NumInspections"])
        wa.writerow(["Suburb", "Month", "NumAuctions", "NumSuccessfulAuctions"])
        ws.writerow(["Suburb", "Month", "NumNewListings", "NumSold", "TotalSoldPrice"])
        for suburb, month in keys:
            inspections = rng.randint(20, 300)
            auctions = rng.randint(0, 80)
            success = rng.randint(0, auctions)
            new_list = rng.randint(10, 120)
            sold = rng.randint(0, new_list)
            avg_price = rng.randint(400_000, 1_600_000)
            total_price = sold * avg_price
            wi.writerow([suburb, month.isoformat(), inspections])
            wa.writerow([suburb, month.isoformat(), auctions, success])
            ws.writerow(
                [suburb, month.isoformat(), new_list, sold, f"{float(total_price):.2f}"]
            )


def write_events(
    n_rows: int, start_year: int, end_year: int, rng: random.Random
) -> None:
    events_dir = DATA_DIR / "events"
    events_dir.mkdir(parents=True, exist_ok=True)
    names = ["app_open", "view_item", "add_to_cart", "signup", "purchase", "search"]
    devices = ["ios", "android", "web"]
    countries = ["AU", "NZ", "ID", "MY", "SG"]
    app_versions = ["1.2.0", "2.0.1", "2.1.0", "web-5.1", "web-5.2"]
    start_dt = datetime(start_year, 1, 1)
    end_dt = datetime(end_year, 12, 31, 23, 59, 59)
    span_seconds = int((end_dt - start_dt).total_seconds())

    with (events_dir / "events.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "EventID",
                "UserID",
                "EventTime",
                "EventName",
                "Device",
                "Country",
                "AppVersion",
            ]
        )
        for _ in range(n_rows):
            user_id = rng.randint(100, 500)
            ts = start_dt + timedelta(seconds=rng.randint(0, span_seconds))
            w.writerow(
                [
                    "",
                    user_id,
                    ts.isoformat(timespec="seconds"),
                    rng.choice(names),
                    rng.choice(devices),
                    rng.choice(countries),
                    rng.choice(app_versions),
                ]
            )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate synthetic MIOD datasets for day 8"
    )
    parser.add_argument(
        "--n",
        type=int,
        default=1000,
        help="Number of rows per output CSV (each file)",
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--start-year", type=int, default=2020)
    parser.add_argument("--end-year", type=int, default=2024)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    write_clubs(args.n, args.start_year, args.end_year, rng)
    write_property(args.n, args.start_year, args.end_year, rng)
    write_events(args.n, args.start_year, args.end_year, rng)


if __name__ == "__main__":
    main()
