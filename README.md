# Storemesh Data Engineer Test

## Table of Contents
- [Setup](#setup)
- [Part 1: Data Exploration (SQL)](#part-1-data-exploration-sql)
- [Part 2: ETL Pipeline (Python + Prefect)](#part-2-etl-pipeline-python--prefect)
- [Part 3: Unit Testing (pytest)](#part-3-unit-testing-pytest)
- [Part 4: Analytical Query (SQL)](#part-4-analytical-query-sql)
- [Project Structure](#project-structure)

---

## Setup

**Prerequisites:** Python 3.12+, Git

```bash
# 1. Clone the repository
git clone <repository-url>
cd storemesh-data-engineer-test

# 2. Create and activate a virtual environment
python -m venv venv

# Windows (PowerShell)
.\venv\Scripts\Activate.ps1

# macOS / Linux
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Part 1: Data Exploration (SQL)

**File:** `exploration.sql`
**Source database:** `shopdata.db`

### How to Run

```bash
sqlite3 shopdata.db < exploration.sql
```

Or open `exploration.sql` in any SQLite client pointed at `shopdata.db`.

### Data Quality Issues Discovered

The following 5 anomalies were identified by querying the `vw_raw_customers` and `vw_raw_orders` views.

#### Issue 1 - Duplicate customer_id in vw_raw_customers

Customer IDs `1` (Alice Smith) and `2` (Bob Jones) each appear **twice** with conflicting details (different emails and phone numbers). This violates primary-key uniqueness and will produce duplicate rows in any downstream join, inflating aggregated metrics.

#### Issue 2 - Invalid total_amount in vw_raw_orders (Negative & Zero Values)

Three orders have a `total_amount <= 0`:
- Orders `103` and `113` have **negative amounts** (-50.0, -100.0) with status `SYSTEM_ERROR`.
- Order `114` has a **zero amount** (0.0) with status `COMPLETED`.

Including these records in revenue calculations will silently corrupt financial aggregations.

#### Issue 3 - Orphaned Orders (Referential Integrity Violation)

Orders `106` and `118` reference `customer_id = 99`, which **does not exist** in `vw_raw_customers`. Any join-based customer enrichment will silently drop or misrepresent these orders.

#### Issue 4 - Orders Placed Before Customer Signup Date (Temporal Anomaly)

**10 out of 20 orders** have an `order_date` earlier than the corresponding customer's `signup_date`. For example, order `101` (placed 2023-05-01) belongs to a customer who did not sign up until 2023-06-01. This suggests a clock/timezone mismatch or incorrect data backfilling.

#### Issue 5 - Missing Critical Fields (NULL values)

| View | Field | NULL Count |
|---|---|---|
| vw_raw_customers | email | 2 |
| vw_raw_customers | phone | 2 |
| vw_raw_orders | order_date | 1 |
| vw_raw_orders | currency | 2 |

NULL `currency` values make currency conversion impossible. NULL `order_date` prevents any time-series or cohort analysis.

---

## Part 2: ETL Pipeline (Python + Prefect)

**File:** `pipeline.py`
**Source database:** `shopdata.db` -> **Output database:** `analytics.db`

### How to Run

```bash
python pipeline.py
```

### Pipeline Steps

| Step | Task | Description |
|---|---|---|
| Extract | `extract_customers` | Read `vw_raw_customers` -> 12 rows |
| Extract | `extract_orders` | Read `vw_raw_orders` -> 20 rows |
| Extract | `extract_exchange_rates` | Read `vw_exchange_rates` -> 15 rows |
| Transform | `transform_customers` | Deduplicate, standardise phone, fill missing email |
| Transform | `transform_orders` | Filter invalid amounts, convert all amounts to USD |
| Load | `load` | Write `dim_customers` and `fct_orders` to `analytics.db` |

### Cleaning Rules Applied

**Customers:**
- Deduplicate on `customer_id` - keep the row with the most recent `signup_date`
- Standardise `phone` - strip all non-numeric characters (e.g. `+1 (555) 123-4567` becomes `15551234567`)
- Replace NULL `email` with `unknown@domain.com`

**Orders:**
- Filter out any order where `total_amount <= 0` (system errors)
- Convert `total_amount` to `usd_amount` using `vw_exchange_rates` matched on `(currency, order_date)`. If no rate is found, default to `1.0` (treated as USD)

### Expected Output (Prefect logs)

```
Pipeline started. Source: shopdata.db | Target: analytics.db
[extract_customers]      12 rows loaded.
[extract_orders]         20 rows loaded.
[extract_exchange_rates] 15 rows loaded.
[transform_customers]    Deduplication removed 2 duplicate row(s). 10 records remain.
[transform_customers]    Filled 1 missing email(s) with unknown@domain.com.
[transform_orders]       Filtered 3 invalid order(s) (amount <= 0). 17 orders remain.
[transform_orders]       14 order(s) had no exchange rate - defaulted to 1.0 (USD).
[load] dim_customers written: 10 rows -> analytics.db
[load] fct_orders written:    17 rows -> analytics.db
Pipeline finished successfully.
```

---

## Part 3: Unit Testing (pytest)

**File:** `tests/test_pipeline.py`
**Framework:** pytest

### How to Run

```bash
pytest tests/ -v
```

Expected output: **29 passed**

### Test Coverage

| Class | Function Tested | Tests |
|---|---|---|
| `TestDeduplication` | `transform_customers` | 5 |
| `TestPhoneStandardisation` | `transform_customers` | 5 |
| `TestMissingEmailFill` | `transform_customers` | 4 |
| `TestOrderFiltering` | `transform_orders` | 5 |
| `TestCurrencyConversion` | `transform_orders` | 10 |
| **Total** | | **29** |

All tests pass dummy pandas DataFrames directly into the transformation functions - no database connection required. Prefect's `get_run_logger` is mocked out so no Prefect server is needed.

---

## Part 4: Analytical Query (SQL)

**File:** `clv_report.sql`
**Source database:** `analytics.db`

### How to Run

> **Note:** Run `python pipeline.py` first to generate `analytics.db`.

```bash
sqlite3 analytics.db < clv_report.sql
```

### Query Output Columns

| Column | Description |
|---|---|
| `customer_id` | Unique customer identifier |
| `full_name` | Customer full name |
| `total_orders_placed` | Count of valid orders per customer |
| `lifetime_value_usd` | Sum of all valid USD order amounts |
| `customer_cohort` | Signup month formatted as `YYYY-MM` (e.g. `2023-06`) |

Results are ranked by `lifetime_value_usd` descending (highest-value customers first).

### Sample Output

| customer_id | full_name | total_orders_placed | lifetime_value_usd | customer_cohort |
|---|---|---|---|---|
| 3 | Charlie Brown | 1 | 25000.00 | 2023-03 |
| 1 | Alice Smith | 3 | 1686.00 | 2023-06 |
| 6 | Fiona Gallagher | 2 | 525.00 | 2023-05 |
| 4 | Diana Prince | 2 | 389.50 | 2023-04 |
| 2 | Bob Jones | 2 | 275.00 | 2023-09 |
| 5 | Evan Wright | 2 | 219.99 | 2023-04 |
| 8 | Hannah Abbott | 1 | 89.00 | 2023-07 |
| 7 | George Costanza | 2 | 55.99 | 2023-06 |
| 9 | Ian Malcolm | 0 | NULL | 2023-08 |
| 10 | Jane Doe | 0 | NULL | 2023-09 |

---

## Project Structure

```
storemesh-data-engineer-test/
|-- pipeline.py          # Part 2: Prefect ETL pipeline
|-- exploration.sql      # Part 1: Data quality exploration queries
|-- clv_report.sql       # Part 4: Customer Lifetime Value SQL report
|-- requirements.txt     # Python dependencies
|-- shopdata.db          # Source SQLite database (provided)
|-- analytics.db         # Output SQLite database (generated by pipeline.py)
|-- README.md            # This file
|-- tests/
    |-- __init__.py
    |-- test_pipeline.py # Part 3: pytest unit tests
```