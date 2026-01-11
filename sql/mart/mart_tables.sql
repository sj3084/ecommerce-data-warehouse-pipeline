-- DAILY SALES
CREATE OR REPLACE VIEW MART.mart_daily_sales AS
SELECT
    d.date_key AS sales_date,
    d.year,
    d.month,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(COALESCE(fo.order_total_usd,0)) AS total_revenue_usd
FROM GOLD.fact_orders fo
JOIN GOLD.dim_date d
    ON fo.date_key = d.date_key
WHERE fo.order_status NOT IN ('CANCELLED','FAILED')
GROUP BY
    d.date_key, d.year, d.month;

-- MONTHLY SALES
CREATE OR REPLACE VIEW MART.mart_monthly_sales AS
SELECT
    d.year_month,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(COALESCE(fo.order_total_usd,0)) AS total_revenue_usd
FROM GOLD.fact_orders fo
JOIN GOLD.dim_date d
    ON fo.date_key = d.date_key
WHERE fo.order_status NOT IN ('CANCELLED','FAILED')
GROUP BY d.year_month;

SELECT * FROM mart.mart_monthly_sales ORDER BY year_month;

-- CUSTOMER VALUE
CREATE OR REPLACE VIEW MART.mart_customer_value AS
SELECT
    dc.customer_id,
    dc.first_name,
    dc.last_name,
    dc.email,
    dc.country,
    COUNT(DISTINCT fo.order_id) AS total_orders,
    SUM(COALESCE(fo.order_total_usd,0)) AS lifetime_value_usd
FROM GOLD.fact_orders fo
JOIN GOLD.dim_customer dc
    ON fo.customer_key = dc.customer_key
WHERE fo.order_status NOT IN ('CANCELLED','FAILED')
GROUP BY
    dc.customer_id,
    dc.first_name,
    dc.last_name,
    dc.email,
    dc.country;

-- PRODUCT CATEGORY PERFORMANCE 
CREATE OR REPLACE VIEW MART.mart_product_performance AS
SELECT
    dp.category,
    COUNT(DISTINCT dp.product_id) AS total_products,
    SUM(foi.quantity) AS total_units_sold,
    SUM(COALESCE(foi.item_total_usd, 0)) AS total_revenue_usd
FROM GOLD.fact_order_items foi
JOIN GOLD.dim_product dp
    ON foi.product_key = dp.product_key
GROUP BY
    dp.category;

-- PAYMENT SUCCESS
CREATE OR REPLACE VIEW MART.mart_payment_success AS
SELECT
    payment_status,
    COUNT(*) AS payment_count,
    SUM(COALESCE(amount_usd,0)) AS total_amount_usd
FROM GOLD.fact_payments
GROUP BY payment_status;

-- FULFILLMENT/DELIVERIES COMPLETED
CREATE OR REPLACE VIEW MART.mart_fulfillment_metrics AS
SELECT
    shipment_status,
    COUNT(*) AS shipment_count,
    AVG(
        CASE 
            WHEN shipment_status = 'DELIVERED' 
            THEN delivery_days 
        END
    ) AS avg_delivery_days
FROM GOLD.fact_shipments
GROUP BY shipment_status;