-- =============================================================
-- clv_report.sql — Customer Lifetime Value (CLV) Report
-- =============================================================
-- Purpose:
--   Calculate CLV for every customer in dim_customers.
--   Data source: analytics.db (output of pipeline.py ETL).
--
-- Assumptions:
--   • dim_customers contains deduplicated, cleaned customer records.
--   • fct_orders contains only valid orders (total_amount > 0) with
--     all amounts already converted to USD in the usd_amount column.
--   • Orphaned orders (customer_id not in dim_customers) are
--     intentionally excluded by joining FROM dim_customers.
--
-- Output columns:
--   customer_id          – unique customer identifier
--   full_name            – customer's full name
--   total_orders_placed  – count of valid orders per customer
--   lifetime_value_usd   – sum of all valid USD order amounts (rounded to 2 dp)
--   customer_cohort      – signup month formatted as 'YYYY-MM'
--
-- Sort: lifetime_value_usd DESC (highest-value customers first)
-- =============================================================

SELECT
    c.customer_id,
    c.full_name,

    -- Count every matched order row; customers with no orders return 0.
    COUNT(o.order_id)                          AS total_orders_placed,

    -- Sum the pre-converted USD amounts from the fact table.
    -- ROUND to 2 decimal places for clean monetary display.
    ROUND(SUM(o.usd_amount), 2)                AS lifetime_value_usd,

    -- Extract 'YYYY-MM' from signup_date to define the acquisition cohort.
    -- signup_date is stored as ISO-8601 datetime ('2023-06-01 00:00:00'),
    -- so STRFTIME correctly parses the leading date portion.
    STRFTIME('%Y-%m', c.signup_date)           AS customer_cohort

FROM dim_customers AS c

-- LEFT JOIN ensures every customer appears in the report even if
-- they have placed no orders. Orphaned orders (customer_id = 99)
-- that exist in fct_orders but not in dim_customers are excluded.
LEFT JOIN fct_orders AS o
    ON c.customer_id = o.customer_id

-- Group by all non-aggregated SELECT columns.
GROUP BY
    c.customer_id,
    c.full_name,
    c.signup_date

-- Highest-value customers first; ties broken by customer_id ascending.
ORDER BY
    lifetime_value_usd DESC,
    c.customer_id      ASC;
