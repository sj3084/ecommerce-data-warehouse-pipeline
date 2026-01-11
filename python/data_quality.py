from db_connector import get_connection

def scalar_check(cursor, query):
    cursor.execute(query)
    return cursor.fetchone()[0]

def run_data_quality_checks():
    conn = get_connection()
    cur = conn.cursor()

    report = []

    checks = {
        "fact_orders.order_total_usd NULL": 
            "SELECT COUNT(*) FROM GOLD.fact_orders WHERE order_total_usd IS NULL",

        "fact_orders.order_total_usd NEGATIVE":
            "SELECT COUNT(*) FROM GOLD.fact_orders WHERE order_total_usd < 0",

        "fact_order_items.item_total_usd NULL":
            "SELECT COUNT(*) FROM GOLD.fact_order_items WHERE item_total_usd IS NULL",

        "fact_payments.amount_usd NEGATIVE":
            "SELECT COUNT(*) FROM GOLD.fact_payments WHERE amount_usd < 0",

        "fact_orders.customer_key NULL":
            "SELECT COUNT(*) FROM GOLD.fact_orders WHERE customer_key IS NULL",

        "fact_orders.date_key NULL":
            "SELECT COUNT(*) FROM GOLD.fact_orders WHERE date_key IS NULL",

        "fact_orders invalid customer FK":
            """SELECT COUNT(*) 
               FROM GOLD.fact_orders fo
               LEFT JOIN GOLD.dim_customer dc
               ON fo.customer_key = dc.customer_key
               WHERE fo.customer_key IS NOT NULL
               AND dc.customer_key IS NULL""",

        "mart_monthly_sales has_rows":
            "SELECT COUNT(*) FROM MART.mart_monthly_sales"
    }

    for name, query in checks.items():
        result = scalar_check(cur, query)

        if "has_rows" in name:
            status = "FAIL" if result == 0 else "PASS"
        else:
            status = "PASS" if result == 0 else "FAIL"

        report.append(f"{name} -> {status} ({result})")

    cur.close()
    conn.close()

    with open("reports/data_quality_report.txt","w") as f:
        for line in report:
            f.write(line + "\n")

    print("Enterprise data quality checks completed.")
