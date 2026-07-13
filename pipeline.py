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
# STEP 3 — TRANSFORM ORDERS
# ---------------------------------------------------------------------------


@task(name="transform_orders", log_prints=True)
def transform_orders(
    orders_df: pd.DataFrame,
    rates_df: pd.DataFrame,
) -> pd.DataFrame:
    """Clean and enrich the raw orders DataFrame.

    Cleaning rules applied (in order):
        1. Filter invalid amounts — drop rows where ``total_amount`` is
           less than or equal to zero (system errors / refund artefacts).
        2. Currency conversion — compute ``usd_amount`` by multiplying
           ``total_amount`` by the matching ``rate_to_usd`` from the
           exchange-rates table, looked up on **both** ``currency`` and
           ``order_date``.  When no match is found (NULL currency, unknown
           currency, or no rate for that date) the order is assumed to be
           already in USD and a rate of 1.0 is applied.

    Args:
        orders_df: Raw orders DataFrame from :func:`extract_orders`.
        rates_df:  Exchange-rates DataFrame from :func:`extract_exchange_rates`.

    Returns:
        Cleaned pd.DataFrame with an added ``usd_amount`` column.
    """
    # --- Rule 1: Filter out invalid amounts --------------------------------
    original_rows = len(orders_df)
    orders_df = orders_df[orders_df["total_amount"] > 0].copy()
    removed = original_rows - len(orders_df)
    print(f"[transform_orders] Filtered {removed} invalid order(s) (amount ≤ 0). {len(orders_df)} orders remain.")

    # --- Rule 2: Convert amounts to USD ------------------------------------
    # Normalise date columns so the join key types match.
    orders_df["order_date"] = pd.to_datetime(orders_df["order_date"], errors="coerce")
    rates_df = rates_df.copy()
    rates_df["date"] = pd.to_datetime(rates_df["date"], errors="coerce")

    # Left-join on (currency, order_date) to get the matching rate.
    orders_df = orders_df.merge(
        rates_df.rename(columns={"date": "order_date"}),
        on=["currency", "order_date"],
        how="left",
    )

    # Fill unmatched rows (NULL currency, unknown currency, or no rate for
    # that specific date) with 1.0 so the amount is treated as-is (USD).
    no_rate = orders_df["rate_to_usd"].isna().sum()
    orders_df["rate_to_usd"] = orders_df["rate_to_usd"].fillna(1.0)
    print(f"[transform_orders] {no_rate} order(s) had no exchange rate — defaulted to 1.0 (USD).")

    orders_df["usd_amount"] = (orders_df["total_amount"] * orders_df["rate_to_usd"]).round(4)
    orders_df = orders_df.drop(columns=["rate_to_usd"])

    return orders_df



# ---------------------------------------------------------------------------
# FLOW
# ---------------------------------------------------------------------------


@flow(name="storemesh-etl-pipeline")
def run_pipeline(db_path: Path = DB_PATH) -> None:
    """Main Prefect flow. Executes Extract and Transform steps."""
    # Step 1 — Extract
    raw_customers = extract_customers(db_path)
    raw_orders = extract_orders(db_path)
    exchange_rates = extract_exchange_rates(db_path)

    # Step 2 — Transform customers
    clean_customers = transform_customers(raw_customers)

    # Step 3 — Transform orders
    clean_orders = transform_orders(raw_orders, exchange_rates)

    # Step 4 (Load) will be added in the next commit.


if __name__ == "__main__":
    run_pipeline()
