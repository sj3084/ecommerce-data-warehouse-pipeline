# Ecommerce Data Warehouse Pipeline
### Layered Analytics Architecture using Snowflake, SQL, Python, and GitHub

## 📖 Project Overview
This project implements a complete end-to-end data engineering pipeline for an ecommerce company using a layered data warehouse architecture. The pipeline transforms raw operational data into clean, analytics-ready datasets using professional data modeling and engineering best practices.

The solution follows a **Bronze → Silver → Gold → Mart** architecture and is fully orchestrated using Python with automated data quality validation. This project simulates real-world enterprise data engineering workflows.



---

## 🎯 Business Objectives
The pipeline enables business teams to answer critical questions, including:
* Daily and monthly revenue trends
* Top-performing products and categories
* Customer Lifetime Value (CLV)
* Payment success rates
* Order fulfillment performance

---

## 🏗 Architecture
The data flows through the following stages:

1.  **CSV Files:** Source data.
2.  **Bronze Layer:** Raw Ingestion (Exact copies of source).
3.  **Silver Layer:** Cleansed & Conformed (Deduplicated and standardized).
4.  **Gold Layer:** Analytics Model (Star Schema with Currency Normalization).
5.  **Mart Layer:** Business Reporting Views (KPI-focused aggregations).
6.  **Python Orchestration:** Execution and Data Quality Validation.

---

## 🗄 Data Layers

### 1. Bronze Layer (Raw Ingestion)
* **Purpose:** Store raw data exactly as received from the source.
* **Tables:** `customers_raw`, `products_raw`, `orders_raw`, `order_items_raw`, `payments_raw`, `shipments_raw`.

### 2. Silver Layer (Cleansed & Conformed)
* **Purpose:** Prepare data for analytics.
* **Key Processing:**
    * Deduplication using window functions.
    * Standardized formats (dates, strings).
    * Business rule validation.
    * Referential integrity enforcement.

### 3. Gold Layer (Analytics Model)
* **Purpose:** Create a Schema optimized for analytical performance.
* **Key Features:** USD currency normalization and clear grain definition.
* **Dimensions:** `dim_customer`, `dim_product`, `dim_date`.
* **Facts:** `fact_orders`, `fact_order_items`, `fact_payments`, `fact_shipments`.

### 4. Mart Layer (Reporting Views)
* **Purpose:** Aggregated datasets specifically for BI tools.
* **Views:**
    * `mart_daily_sales`
    * `mart_monthly_sales`
    * `mart_customer_value`
    * `mart_product_performance`

---

## 🐍 Python Orchestration & Quality Control
The pipeline is orchestrated via Python to provide:
* **Sequential Execution:** Runs SQL scripts in the correct dependency order.
* **Data Quality (DQ) Gateways:** Automated checks for `NULL` business metrics and negative monetary values.
* **Logging:** Detailed execution logs and quality reports.

**Sample DQ Report Output:**
```text
fact_orders.order_total_usd NULL -> PASS (0)
fact_orders.order_total_usd NEGATIVE -> PASS (0)
fact_order_items.item_total_usd NULL -> PASS (0)
fact_payments.amount_usd NEGATIVE -> PASS (0)
fact_orders.customer_key NULL -> PASS (0)
fact_orders.date_key NULL -> PASS (0)
fact_orders invalid customer FK -> PASS (0)
mart_monthly_sales has_rows -> FAIL (0)
```

## 🛠 Technologies Used
* **Snowflake:** Cloud Data Warehouse
* **SQL:** Data Transformation and Modeling
* **Python:** Pipeline Orchestration
* **GitHub:** Version Control and Documentation

## 🔮 Future Enhancements
- Implementation of incremental loading logic.
- Airflow orchestration for scheduling.
- SCD Type-2 tracking for customer and product dimensions.
- Integration with a BI dashboard (Tableau/PowerBI/Looker).
