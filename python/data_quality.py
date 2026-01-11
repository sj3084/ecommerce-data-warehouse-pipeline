from db_connector import get_connection

def check_nulls(cursor, table, column):
    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL")
    return cursor.fetchone()[0]

def check_negative(cursor, table, column):
    cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} < 0")
    return cursor.fetchone()[0]

def run_data_quality_checks():
    conn = get_connection()
    cur = conn.cursor()

    report_lines = []

    checks = [
        ("GOLD.fact_orders","order_total_usd"),
        ("GOLD.fact_order_items","item_total_usd"),
        ("GOLD.fact_payments","amount_usd")
    ]

    for table, column in checks:
        nulls = check_nulls(cur, table, column)
        negatives = check_negative(cur, table, column)

        line = f"{table}.{column} -> NULLS={nulls}, NEGATIVES={negatives}"
        report_lines.append(line)

    cur.close()
    conn.close()

    with open("reports/data_quality_report.txt","w") as f:
        for l in report_lines:
            f.write(l + "\n")

    print("Data quality checks completed successfully.")
