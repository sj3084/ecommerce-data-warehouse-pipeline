CREATE TABLE IF NOT EXISTS BRONZE.products_raw (
    product_id STRING,
    product_name STRING,
    category STRING,
    price NUMBER(10,2),
    currency STRING,
    is_active BOOLEAN,
    created_at TIMESTAMP_NTZ,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS BRONZE.customers_raw (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    phone STRING,
    country STRING,
    created_at TIMESTAMP_NTZ,
    is_active BOOLEAN,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS BRONZE.shipments_raw (
    shipment_id STRING,
    order_id STRING,
    shipment_date TIMESTAMP_NTZ,
    delivery_date TIMESTAMP_NTZ,
    carrier STRING,
    shipment_status STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS BRONZE.orders_raw (
    order_id STRING,
    customer_id STRING,
    order_date TIMESTAMP_NTZ,
    order_status STRING,
    order_total NUMBER(12,2),
    currency STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS BRONZE.order_items_raw (
    order_item_id STRING,
    order_id STRING,
    product_id STRING,
    quantity INTEGER,
    unit_price NUMBER(10,2),
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS BRONZE.payments_raw (
    payment_id STRING,
    order_id STRING,
    payment_date TIMESTAMP_NTZ,
    payment_method STRING,
    amount NUMBER(12,2),
    currency STRING,
    payment_status STRING,
    load_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
