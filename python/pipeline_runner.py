from db_connector import get_connection
from sql_runner import run_sql_file
from data_quality import run_data_quality_checks
import os
import logging

logging.basicConfig(
    filename="pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def run_stage(stage_name, folder):
    conn = get_connection()
    cur = conn.cursor()

    try:
        for file in sorted(os.listdir(folder)):
            if file.endswith(".sql"):
                run_sql_file(cur, f"{folder}/{file}")
        logging.info(f"{stage_name} stage completed successfully")
    except Exception as e:
        logging.error(f"{stage_name} stage failed: {str(e)}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    run_stage("Bronze", "sql/bronze")
    run_stage("Silver", "sql/silver")
    run_stage("Gold", "sql/gold")
    run_stage("Mart", "sql/mart")

    run_data_quality_checks()
