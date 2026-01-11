-- GOLD CUSTOMERS
CREATE OR REPLACE TABLE GOLD.dim_customer AS
SELECT
    ROW_NUMBER() OVER(ORDER BY customer_id) AS customer_key,
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    country,
    is_active,
    created_at
FROM SILVER.customers_silver;

-- GOLD PRODUCTS (WITH CURRENCY CONVERSION)
CREATE OR REPLACE TABLE GOLD.dim_product AS
SELECT
    ROW_NUMBER() OVER(ORDER BY p.product_id) AS product_key,
    p.product_id,
    p.product_name,
    p.category,
    p.price AS price_original,
    p.currency,
    ROUND(p.price * fx.rate_to_usd, 2) AS price_usd,
    p.is_active,
    p.created_at
FROM SILVER.products_silver p
LEFT JOIN GOLD.fx_rates fx
    ON p.currency = fx.currency;

-- GOLD DATES
CREATE OR REPLACE TABLE GOLD.dim_date AS
SELECT
    d::DATE AS date_key,
    YEAR(d) AS year,
    MONTH(d) AS month,
    DAY(d) AS day,
    TO_CHAR(d,'YYYY-MM') AS year_month,
    TO_CHAR(d,'DY') AS weekday
FROM (
    SELECT DATEADD(DAY, SEQ4(), '2020-01-01') AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 3650))
);