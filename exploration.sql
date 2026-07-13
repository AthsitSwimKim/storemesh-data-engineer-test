-- ============================================================
-- exploration.sql
-- Data Quality Exploration for vw_raw_customers & vw_raw_orders
-- ============================================================

-- ============================================================
-- SECTION 1: CUSTOMER DATA QUALITY
-- ============================================================

-- 1.1 Duplicate customer_id
-- A customer_id should be a unique identifier. Multiple rows sharing
-- the same customer_id indicate either duplicate inserts or a broken
-- upstream primary-key constraint.
SELECT
    customer_id,
    COUNT(*) AS occurrence_count
FROM vw_raw_customers
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY occurrence_count DESC;

-- 1.2 Show all rows involved in duplicate customer_id conflicts
-- Useful for understanding exactly which records are duplicated
-- and whether any fields differ between duplicates.
SELECT *
FROM vw_raw_customers
WHERE customer_id IN (
    SELECT customer_id
    FROM vw_raw_customers
    GROUP BY customer_id
    HAVING COUNT(*) > 1
)
ORDER BY customer_id;

-- 1.3 Missing (NULL) email addresses in customers
-- Email is a key contact field; NULL values prevent communication
-- and may indicate incomplete data ingestion.
SELECT
    customer_id,
    full_name,
    email,
    phone,
    signup_date
FROM vw_raw_customers
WHERE email IS NULL OR email = '';

-- 1.4 Missing (NULL) phone numbers in customers
-- Phone is a secondary contact field; NULL values reduce contact options.
SELECT
    customer_id,
    full_name,
    email,
    phone,
    signup_date
FROM vw_raw_customers
WHERE phone IS NULL OR phone = '';

-- 1.5 Summary of NULL counts across all customer fields
-- Provides a quick at-a-glance view of data completeness.
SELECT
    COUNT(*) AS total_customers,
    SUM(CASE WHEN email    IS NULL OR email    = '' THEN 1 ELSE 0 END) AS null_email_count,
    SUM(CASE WHEN phone    IS NULL OR phone    = '' THEN 1 ELSE 0 END) AS null_phone_count,
    SUM(CASE WHEN full_name IS NULL OR full_name = '' THEN 1 ELSE 0 END) AS null_name_count,
    SUM(CASE WHEN signup_date IS NULL OR signup_date = '' THEN 1 ELSE 0 END) AS null_signup_date_count
FROM vw_raw_customers;

-- ============================================================
-- SECTION 2: ORDER DATA QUALITY
-- ============================================================

-- 2.1 Negative or zero total_amount
-- A completed order with a negative amount is financially invalid and
-- likely signals a SYSTEM_ERROR that was not properly filtered.
-- A zero amount on a COMPLETED order is also suspicious.
SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    currency,
    status
FROM vw_raw_orders
WHERE total_amount <= 0
ORDER BY total_amount;

-- 2.2 Orders linked to non-existent customers (referential integrity violation)
-- If an order's customer_id does not exist in vw_raw_customers,
-- it is an orphaned record — the customer data was never loaded or was deleted.
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.total_amount,
    o.status
FROM vw_raw_orders o
LEFT JOIN vw_raw_customers c ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL;

-- 2.3 Orders placed before the customer's signup date (temporal anomaly)
-- If an order was placed before the customer signed up, the dates are
-- logically inconsistent — this may point to data entry errors or
-- a mismatch between the order system and the CRM system clocks.
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    c.signup_date,
    CAST(JULIANDAY(c.signup_date) - JULIANDAY(o.order_date) AS INTEGER) AS days_before_signup
FROM vw_raw_orders o
JOIN vw_raw_customers c ON o.customer_id = c.customer_id
WHERE o.order_date < c.signup_date
ORDER BY days_before_signup DESC;

-- 2.4 Orders with a NULL order_date
-- A missing order_date makes it impossible to perform time-series analysis,
-- calculate delivery times, or join with date-dimension tables.
SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    currency,
    status
FROM vw_raw_orders
WHERE order_date IS NULL OR order_date = '';

-- 2.5 Orders with a NULL currency
-- NULL currency prevents currency conversion to a standard unit (e.g., USD).
-- These orders cannot be reliably included in financial aggregations.
SELECT
    order_id,
    customer_id,
    order_date,
    total_amount,
    currency,
    status
FROM vw_raw_orders
WHERE currency IS NULL OR currency = '';

-- 2.6 Summary of NULL / invalid counts across all order fields
-- Provides a single-row dashboard of data quality for the orders view.
SELECT
    COUNT(*) AS total_orders,
    SUM(CASE WHEN order_date IS NULL OR order_date = '' THEN 1 ELSE 0 END) AS null_order_date_count,
    SUM(CASE WHEN currency  IS NULL OR currency  = '' THEN 1 ELSE 0 END) AS null_currency_count,
    SUM(CASE WHEN total_amount IS NULL                THEN 1 ELSE 0 END) AS null_amount_count,
    SUM(CASE WHEN total_amount <= 0                  THEN 1 ELSE 0 END) AS invalid_amount_count,
    SUM(CASE WHEN status = 'SYSTEM_ERROR'            THEN 1 ELSE 0 END) AS system_error_count
FROM vw_raw_orders;

-- 2.7 Distribution of order statuses
-- Helps identify unexpected status values beyond the standard
-- (COMPLETED, PENDING, CANCELLED). SYSTEM_ERROR rows should likely
-- be excluded from business-facing reports.
SELECT
    status,
    COUNT(*) AS order_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
FROM vw_raw_orders
GROUP BY status
ORDER BY order_count DESC;
