"""
pipeline.py — Storemesh Data Engineer Test
ETL pipeline orchestrated with Prefect 3.x

Steps:
    1. Extract   — read raw data from SQLite views into DataFrames
    2. Transform — clean and enrich the raw data          (next commit)
    3. Load      — write cleaned data to output database  (next commit)
"""

import sqlite3
from pathlib import Path

import pandas as pd
from prefect import flow, task

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_PATH = Path(r"D:\storemesh-data-engineer-test\shopdata.db")

# ---------------------------------------------------------------------------
# STEP 1 — EXTRACT
# ---------------------------------------------------------------------------


@task(name="extract_customers", log_prints=True)
def extract_customers(db_path: Path = DB_PATH) -> pd.DataFrame:
    """Extract all rows from vw_raw_customers.

    Args:
        db_path: Absolute path to the SQLite database file.

    Returns:
        pd.DataFrame with columns:
            customer_id (int), full_name (str), email (str),
            phone (str), signup_date (str)
    """
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query("SELECT * FROM vw_raw_customers", conn)
    print(f"[extract_customers]     {len(df)} rows loaded.")
    return df


@task(name="extract_orders", log_prints=True)
def extract_orders(db_path: Path = DB_PATH) -> pd.DataFrame:
    """Extract all rows from vw_raw_orders.

    Args:
        db_path: Absolute path to the SQLite database file.

    Returns:
        pd.DataFrame with columns:
            order_id (int), customer_id (int), order_date (str),
            total_amount (float), currency (str), status (str)
    """
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query("SELECT * FROM vw_raw_orders", conn)
    print(f"[extract_orders]        {len(df)} rows loaded.")
    return df


@task(name="extract_exchange_rates", log_prints=True)
def extract_exchange_rates(db_path: Path = DB_PATH) -> pd.DataFrame:
    """Extract all rows from vw_exchange_rates.

    Args:
        db_path: Absolute path to the SQLite database file.

    Returns:
        pd.DataFrame with columns:
            currency (str), rate_to_usd (float), date (str)
    """
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query("SELECT * FROM vw_exchange_rates", conn)
    print(f"[extract_exchange_rates] {len(df)} rows loaded.")
    return df


# ---------------------------------------------------------------------------
# FLOW — entry point (will be extended in Steps 2 & 3)
# ---------------------------------------------------------------------------


@flow(name="storemesh-etl-pipeline")
def run_pipeline(db_path: Path = DB_PATH) -> None:
    """Main Prefect flow. Currently executes the Extract step only."""
    # Step 1 — Extract
    raw_customers = extract_customers(db_path)
    raw_orders = extract_orders(db_path)
    exchange_rates = extract_exchange_rates(db_path)

    # Steps 2 (Transform) and 3 (Load) will be added in the next commits.


if __name__ == "__main__":
    run_pipeline()
