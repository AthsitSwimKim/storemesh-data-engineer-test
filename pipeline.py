"""
pipeline.py — Storemesh Data Engineer Test
ETL pipeline orchestrated with Prefect 3.x

Steps:
    1. Extract   — read raw data from SQLite views into DataFrames
    2. Transform — clean and enrich the raw data
    3. Load      — write cleaned data to analytics.db
"""

import re
import sqlite3
from pathlib import Path

import pandas as pd
from prefect import flow, get_run_logger, task

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_PATH          = Path(r"D:\storemesh-data-engineer-test\shopdata.db")
ANALYTICS_DB_PATH = Path(r"D:\storemesh-data-engineer-test\analytics.db")

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
    logger = get_run_logger()
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query("SELECT * FROM vw_raw_customers", conn)
    logger.info("[extract_customers]     %d rows loaded.", len(df))
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
    logger = get_run_logger()
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query("SELECT * FROM vw_raw_orders", conn)
    logger.info("[extract_orders]        %d rows loaded.", len(df))
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
    logger = get_run_logger()
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query("SELECT * FROM vw_exchange_rates", conn)
    logger.info("[extract_exchange_rates] %d rows loaded.", len(df))
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
    logger = get_run_logger()
    original_rows = len(df)

    # --- Rule 1: Deduplicate on customer_id, keep latest signup_date -------
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")
    df = (
        df.sort_values("signup_date", ascending=False)
        .drop_duplicates(subset="customer_id", keep="first")
        .reset_index(drop=True)
    )
    removed = original_rows - len(df)
    logger.info(
        "[transform_customers] Deduplication removed %d duplicate row(s). %d records remain.",
        removed, len(df),
    )


    # --- Rule 2: Standardise phone — keep digits only ----------------------
    df["phone"] = df["phone"].apply(
        lambda v: re.sub(r"\D", "", str(v)) if pd.notna(v) else v
    )

    # --- Rule 3: Fill missing email ----------------------------------------
    missing_email = df["email"].isna().sum()
    df["email"] = df["email"].fillna("unknown@domain.com")
    logger.info(
        "[transform_customers] Filled %d missing email(s) with 'unknown@domain.com'.",
        missing_email,
    )

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
    logger = get_run_logger()

    # --- Rule 1: Filter out invalid amounts --------------------------------
    original_rows = len(orders_df)
    orders_df = orders_df[orders_df["total_amount"] > 0].copy()
    removed = original_rows - len(orders_df)
    logger.info(
        "[transform_orders] Filtered %d invalid order(s) (amount ≤ 0). %d orders remain.",
        removed, len(orders_df),
    )

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
    logger.info(
        "[transform_orders] %d order(s) had no exchange rate — defaulted to 1.0 (USD).",
        no_rate,
    )

    orders_df["usd_amount"] = (orders_df["total_amount"] * orders_df["rate_to_usd"]).round(4)
    orders_df = orders_df.drop(columns=["rate_to_usd"])

    return orders_df



# ---------------------------------------------------------------------------
# STEP 4 — LOAD
# ---------------------------------------------------------------------------


@task(name="load", log_prints=True)
def load(
    customers_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    analytics_db_path: Path = ANALYTICS_DB_PATH,
) -> None:
    """Write cleaned DataFrames to the analytics SQLite database.

    Creates (or replaces) two tables:
        * ``dim_customers`` — cleaned customer dimension table.
        * ``fct_orders``    — cleaned orders fact table with USD amounts.

    Args:
        customers_df:      Cleaned customers DataFrame from :func:`transform_customers`.
        orders_df:         Cleaned orders DataFrame from :func:`transform_orders`.
        analytics_db_path: Path to the output SQLite database file.
                           The file is created automatically if it does not exist.
    """
    logger = get_run_logger()
    try:
        with sqlite3.connect(analytics_db_path) as conn:
            customers_df.to_sql(
                "dim_customers",
                conn,
                if_exists="replace",
                index=False,
            )
            logger.info(
                "[load] dim_customers written: %d rows -> %s",
                len(customers_df), analytics_db_path,
            )

            orders_df.to_sql(
                "fct_orders",
                conn,
                if_exists="replace",
                index=False,
            )
            logger.info(
                "[load] fct_orders written:    %d rows -> %s",
                len(orders_df), analytics_db_path,
            )
    except Exception as exc:
        logger.error("[load] Failed to write to %s: %s", analytics_db_path, exc)
        raise


# ---------------------------------------------------------------------------
# FLOW — full ETL orchestration
# ---------------------------------------------------------------------------


@flow(name="storemesh-etl-pipeline")
def run_pipeline(
    db_path: Path = DB_PATH,
    analytics_db_path: Path = ANALYTICS_DB_PATH,
) -> None:
    """Main Prefect flow: Extract -> Transform Customers -> Transform Orders -> Load.

    Args:
        db_path:           Path to the source SQLite database (shopdata.db).
        analytics_db_path: Path to the output SQLite database (analytics.db).
    """
    logger = get_run_logger()
    logger.info("Pipeline started. Source: %s | Target: %s", db_path, analytics_db_path)

    try:
        # Step 1 — Extract
        raw_customers  = extract_customers(db_path)
        raw_orders     = extract_orders(db_path)
        exchange_rates = extract_exchange_rates(db_path)

        # Step 2 — Transform customers
        clean_customers = transform_customers(raw_customers)

        # Step 3 — Transform orders
        clean_orders = transform_orders(raw_orders, exchange_rates)

        # Step 4 — Load
        load(clean_customers, clean_orders, analytics_db_path)

        logger.info("Pipeline finished successfully.")

    except Exception as exc:
        logger.error("Pipeline failed: %s", exc)
        raise


if __name__ == "__main__":
    run_pipeline()
