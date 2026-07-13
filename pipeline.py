"""
pipeline.py — Storemesh Data Engineer Test
ETL pipeline orchestrated with Prefect 3.x

Steps:
    1. Extract   — read raw data from SQLite views into DataFrames
    2. Transform — clean and enrich the raw data
    3. Load      — write cleaned data to output database  (next commit)
"""

import re
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
# STEP 2 — TRANSFORM CUSTOMERS
# ---------------------------------------------------------------------------


@task(name="transform_customers", log_prints=True)
def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and standardise the raw customers DataFrame.

    Cleaning rules applied (in order):
        1. Deduplicate on ``customer_id`` — keep the row with the most
           recent ``signup_date`` (latest = most up-to-date record).
        2. Standardise ``phone`` — strip every non-numeric character so
           '+1 (555) 123-4567' becomes '15551234567'.
        3. Fill missing ``email`` values with 'unknown@domain.com'.

    Args:
        df: Raw customers DataFrame produced by :func:`extract_customers`.

    Returns:
        Cleaned pd.DataFrame with the same columns as the input.
    """
    original_rows = len(df)

    # --- Rule 1: Deduplicate on customer_id, keep latest signup_date -------
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")
    df = (
        df.sort_values("signup_date", ascending=False)
        .drop_duplicates(subset="customer_id", keep="first")
        .reset_index(drop=True)
    )
    removed = original_rows - len(df)
    print(f"[transform_customers] Deduplication removed {removed} duplicate row(s). {len(df)} records remain.")

    # --- Rule 2: Standardise phone — keep digits only ----------------------
    df["phone"] = df["phone"].apply(
        lambda v: re.sub(r"\D", "", str(v)) if pd.notna(v) else v
    )

    # --- Rule 3: Fill missing email ----------------------------------------
    missing_email = df["email"].isna().sum()
    df["email"] = df["email"].fillna("unknown@domain.com")
    print(f"[transform_customers] Filled {missing_email} missing email(s) with 'unknown@domain.com'.")

    return df

# ---------------------------------------------------------------------------
# FLOW — entry point (will be extended in Steps 2 & 3)
# ---------------------------------------------------------------------------


@flow(name="storemesh-etl-pipeline")
def run_pipeline(db_path: Path = DB_PATH) -> None:
    """Main Prefect flow. Executes Extract and Transform (customers) steps."""
    # Step 1 — Extract
    raw_customers = extract_customers(db_path)
    raw_orders = extract_orders(db_path)
    exchange_rates = extract_exchange_rates(db_path)

    # Step 2 — Transform
    clean_customers = transform_customers(raw_customers)

    # Step 3 (Load) will be added in the next commit.


if __name__ == "__main__":
    run_pipeline()
