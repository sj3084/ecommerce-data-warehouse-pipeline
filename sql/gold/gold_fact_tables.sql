-- GOLD ORDERS (WITH CURRENCY CONVERSION)
CREATE OR REPLACE TABLE GOLD.fact_orders AS
SELECT
    o.order_id,
    dc.customer_key,
    d.date_key,
    o.order_status,
    o.order_total AS order_total_original,
    o.currency,
    ROUND(o.order_total * fx.rate_to_usd, 2) AS order_total_usd
FROM SILVER.orders_silver o
LEFT JOIN GOLD.dim_customer dc 
    ON o.customer_id = dc.customer_id
LEFT JOIN GOLD.dim_date d 
    ON o.order_date = d.date_key
LEFT JOIN GOLD.fx_rates fx 
    ON o.currency = fx.currency;

-- GOLD ORDER ITEMS (WITH CURRENCY CONVERSION)
CREATE OR REPLACE TABLE GOLD.fact_order_items AS
SELECT
    oi.order_item_id,
    oi.order_id,
    dp.product_key,
    oi.quantity,
    oi.unit_price AS unit_price_original,
    o.currency,
    ROUND(oi.unit_price * fx.rate_to_usd, 2) AS unit_price_usd,
    oi.item_total AS item_total_original,
    ROUND(oi.item_total * fx.rate_to_usd, 2) AS item_total_usd
FROM SILVER.order_items_silver oi
LEFT JOIN SILVER.orders_silver o
    ON oi.order_id = o.order_id
LEFT JOIN GOLD.dim_product dp
    ON oi.product_id = dp.product_id
LEFT JOIN GOLD.fx_rates fx
    ON o.currency = fx.currency;

-- GOLD PAYMENTS (WITH CURRENCY CONVERSION)
CREATE OR REPLACE TABLE GOLD.fact_payments AS
SELECT
    p.payment_id,
    p.order_id,
    d.date_key,
    p.payment_method,
    p.payment_status,
    p.amount AS amount_original,
    p.currency,
    ROUND(p.amount * fx.rate_to_usd, 2) AS amount_usd
FROM SILVER.payments_silver p
LEFT JOIN GOLD.dim_date d 
    ON DATE_TRUNC('DAY', p.payment_date) = d.date_key
LEFT JOIN GOLD.fx_rates fx 
    ON p.currency = fx.currency;

-- GOLD SHIPMENTS
CREATE OR REPLACE TABLE GOLD.fact_shipments AS
SELECT
    s.shipment_id,
    s.order_id,
    d.date_key,
    s.carrier,
    s.shipment_status,
    DATEDIFF('DAY', s.shipment_date, s.delivery_date) AS delivery_days
FROM SILVER.shipments_silver s
LEFT JOIN GOLD.dim_date d 
    ON DATE_TRUNC('DAY', s.shipment_date) = d.date_key;
