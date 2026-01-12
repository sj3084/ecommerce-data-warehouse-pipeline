-- CUSTOMERS
CREATE OR REPLACE TABLE SILVER.customers_silver AS
SELECT
    customer_id,
    INITCAP(first_name) AS first_name,
    INITCAP(last_name) AS last_name,
    LOWER(email) AS email,
    phone,
    country,
    created_at,
    COALESCE(is_active, TRUE) AS is_active
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY load_timestamp DESC) rn
    FROM BRONZE.customers_raw
)
WHERE rn = 1;

-- PRODUCTS
CREATE OR REPLACE TABLE SILVER.products_silver AS
SELECT
    product_id,
    product_name,
    COALESCE(NULLIF(category,''), 'UNKNOWN') AS category,
    price,
    UPPER(currency) AS currency,
    COALESCE(is_active, TRUE) AS is_active,
    created_at
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY load_timestamp DESC) rn
    FROM BRONZE.products_raw
)
WHERE rn = 1
  AND price > 0;

-- ORDERS
CREATE OR REPLACE TABLE SILVER.orders_silver AS
SELECT
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_status,
    o.order_total,
    UPPER(o.currency) AS currency
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY order_id ORDER BY load_timestamp DESC) rn
    FROM BRONZE.orders_raw
) o
LEFT JOIN SILVER.customers_silver c
  ON o.customer_id = c.customer_id
WHERE rn = 1
  AND order_total >= 0;

-- ORDER ITEMS
CREATE OR REPLACE TABLE SILVER.order_items_silver AS
SELECT
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price AS item_total
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY order_item_id ORDER BY load_timestamp DESC) rn
    FROM BRONZE.order_items_raw
) oi
LEFT JOIN SILVER.orders_silver o
  ON oi.order_id = o.order_id
LEFT JOIN SILVER.products_silver p
  ON oi.product_id = p.product_id
WHERE rn = 1
  AND quantity > 0
  AND unit_price > 0;

-- PAYMENTS
CREATE OR REPLACE TABLE SILVER.payments_silver AS
SELECT
    p.payment_id,
    p.order_id,
    p.payment_date,
    p.payment_method,
    p.amount,
    UPPER(p.currency) AS currency,
    p.payment_status
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY payment_id ORDER BY load_timestamp DESC) rn
    FROM BRONZE.payments_raw
) p
LEFT JOIN SILVER.orders_silver o
  ON p.order_id = o.order_id
WHERE rn = 1
  AND amount > 0;

-- SHIPMENTS
CREATE OR REPLACE TABLE SILVER.shipments_silver AS
SELECT
    s.shipment_id,
    s.order_id,
    s.shipment_date,
    s.delivery_date,
    s.carrier,
    s.shipment_status
FROM (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY shipment_id ORDER BY load_timestamp DESC) rn
    FROM BRONZE.shipments_raw
) s
LEFT JOIN SILVER.orders_silver o
  ON s.order_id = o.order_id
WHERE rn = 1
  AND s.shipment_date IS NOT NULL
  AND (
       (s.shipment_status = 'DELIVERED' AND s.delivery_date IS NOT NULL)
    OR (s.shipment_status IN ('IN_TRANSIT','CANCELLED') AND s.delivery_date IS NULL)
  );