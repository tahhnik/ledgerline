"""Provided helper for Lab 04: local SQLite sink (WAL mode) and summary CLI."""
from __future__ import annotations

import argparse
import os
import sqlite3
from decimal import Decimal
from pathlib import Path

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS payments_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    card_id TEXT NOT NULL,
    amount_cents INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS payments_dedup (
    event_id TEXT PRIMARY KEY,
    card_id TEXT NOT NULL,
    amount_cents INTEGER NOT NULL,
    partition INTEGER NOT NULL,
    offset INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS card_totals (
    card_id TEXT PRIMARY KEY,
    payment_count INTEGER NOT NULL,
    amount_cents INTEGER NOT NULL
);
"""


def get_bootstrap() -> str:
    if "BOOTSTRAP" in os.environ:
        return os.environ["BOOTSTRAP"]
    env_file = Path("aws.env")
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line.startswith("BOOTSTRAP="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return "localhost:9092"


def amount_to_cents(amount: str | Decimal) -> int:
    return int((Decimal(str(amount)) * 100).to_integral_value())


def connect_db(path: str | Path = "sink.db") -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.executescript(SCHEMA_SQL)
    return conn


def reset_db(path: str | Path = "sink.db") -> sqlite3.Connection:
    conn = connect_db(path)
    with conn:
        conn.execute("DELETE FROM payments_raw")
        conn.execute("DELETE FROM payments_dedup")
        conn.execute("DELETE FROM card_totals")
    return conn


def summary(path: str | Path = "sink.db", table: str = "payments_dedup") -> dict[str, int]:
    if table not in ("payments_raw", "payments_dedup"):
        raise ValueError(f"unknown table: {table}")
    conn = connect_db(path)
    try:
        rows, unique_events = conn.execute(
            f"SELECT COUNT(*), COUNT(DISTINCT event_id) FROM {table}"
        ).fetchone()
        total_count, total_cents = conn.execute(
            "SELECT COALESCE(SUM(payment_count), 0), COALESCE(SUM(amount_cents), 0) FROM card_totals"
        ).fetchone()
        return {
            "rows": int(rows),
            "unique_events": int(unique_events),
            "total_count": int(total_count),
            "total_cents": int(total_cents),
        }
    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description="Inspect the Lab 04 SQLite sink.")
    ap.add_argument("--db", default="sink.db")
    ap.add_argument("--table", default="payments_dedup", choices=["payments_raw", "payments_dedup"])
    ap.add_argument("--reset", action="store_true")
    a = ap.parse_args()
    if a.reset:
        reset_db(a.db).close()
    s = summary(a.db, a.table)
    print(
        f"rows={s['rows']} unique_events={s['unique_events']} "
        f"total_count={s['total_count']} total_cents={s['total_cents']}"
    )


if __name__ == "__main__":
    main()
