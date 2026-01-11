Ecommerce Data Warehouse Pipeline
Layered Analytics Architecture using Snowflake, SQL, Python, and GitHub
Project Overview
This project implements a complete end-to-end data engineering pipeline for an ecommerce company using a layered data warehouse architecture. The pipeline transforms raw operational data into clean, analytics-ready datasets using professional data modeling and engineering best practices.

The solution follows a Bronze -> Silver -> Gold -> Mart architecture and is fully orchestrated using Python with automated data quality validation. This project simulates real-world enterprise data engineering workflows.

Business Objectives
The pipeline enables business teams to answer questions such as:

Daily and monthly revenue trends

Top performing products and categories

Customer lifetime value (CLV)

Payment success rates

Order fulfillment performance

Architecture
The data flows through the following stages:

CSV Files: Source data.

Bronze Layer: Raw Ingestion (Exact copies of source).

Silver Layer: Cleansed & Conformed (Deduplicated and standardized).

Gold Layer: Analytics Model (Star Schema with Currency Normalization).

Mart Layer: Business Reporting Views (KPI-focused aggregations).

Python Orchestration: Execution and Data Quality Validation.

Project Structure
Plaintext

ecommerce-data-warehouse-pipeline/
│
├── sql/
│   ├── bronze/          # DDL for raw tables
│   ├── silver/          # Cleaning and deduplication scripts
│   ├── gold/            # Star schema fact and dimension tables
│   ├── mart/            # Business reporting views
│
├── python/
│   ├── db_connector.py   # Snowflake connection management
│   ├── sql_runner.py     # Executes SQL files via Python
│   ├── pipeline_runner.py# Main orchestration script
│   ├── data_quality.py   # Validation logic
│
├── reports/
│   └── data_quality_report_sample.txt
│
├── .gitignore
├── requirements.txt
└── README.md
Data Layers
Bronze Layer (Raw Ingestion)
Purpose: Store raw data exactly as received.

Tables: customers_raw, products_raw, orders_raw, order_items_raw, payments_raw, shipments_raw.

Silver Layer (Cleansed & Conformed)
Purpose: Prepare data for analytics.

Key processing: Deduplication using window functions, standardized formats, business rule validation, and referential integrity enforcement.

Gold Layer (Analytics Model)
Purpose: Create a star schema optimized for performance.

Dimensions: dim_customer, dim_product, dim_date.

Facts: fact_orders, fact_order_items, fact_payments, fact_shipments.

Key Features: Surrogate keys, USD currency normalization, and clear grain definition.

Mart Layer (Reporting Views)
Purpose: Aggregated datasets for BI tools.

Views: mart_daily_sales, mart_monthly_sales, mart_customer_value, mart_product_performance.

Python Orchestration & Quality Control
The pipeline is orchestrated via Python to provide:

Sequential Execution: Runs SQL scripts in the correct dependency order.

Data Quality (DQ) Gateways: Automated checks for NULL business metrics and negative monetary values.

Logging: Detailed execution logs and quality reports.

Sample DQ Report:
Plaintext

GOLD.fact_orders.order_total_usd -> NULLS=0, NEGATIVES=0
GOLD.fact_payments.amount_usd -> NULLS=0, NEGATIVES=0
Technologies Used
Snowflake: Cloud Data Warehouse

SQL: Data Transformation and Modeling

Python: Pipeline Orchestration

GitHub: Version Control and Documentation

How to Run
Create a Snowflake account and database.

Configure credentials in a .config file (refer to .gitignore).

Bash

python python/pipeline_runner.py
Review the results in Snowflake or the generated reports in /reports.

Future Enhancements
Implementation of incremental loading logic.

Airflow orchestration for scheduling.

SCD Type-2 tracking for customer and product dimensions.

Integration with a BI dashboard (Tableau/PowerBI/Looker).
