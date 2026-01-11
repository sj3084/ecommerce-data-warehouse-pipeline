import pandas as pd
import configparser
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
import sys

# --------------------
# Snowflake connection
# --------------------
config = configparser.ConfigParser()
config.read(".config")

sf = config["snowflake"]

conn = snowflake.connector.connect(
    user=sf["user"],
    password=sf["password"],
    account=sf["account"],
    warehouse=sf["warehouse"],
    database=sf["database"],
    schema=sf["schema"]
)

cursor = conn.cursor()
cursor.execute(f'USE WAREHOUSE {sf["warehouse"]}')

# --------------------
# Helpers
# --------------------
def map_dtype_to_snowflake(dtype):
    dtype = str(dtype).lower()

    if "int" in dtype:
        return "NUMBER"
    elif "float" in dtype:
        return "FLOAT"
    elif "datetime" in dtype:
        return "TIMESTAMP_NTZ"
    elif "bool" in dtype:
        return "BOOLEAN"
    else:
        return "STRING"


def create_table_from_df(cursor, df, table_name):

    column_defs = []

    for col, dtype in zip(df.columns, df.dtypes):
        snow_type = map_dtype_to_snowflake(dtype)

        column_defs.append(f'"{col}" {snow_type}')

    create_sql = f"""
    CREATE OR REPLACE TABLE "{table_name}" (
        {", ".join(column_defs)}
    )
    """

    cursor.execute(create_sql)


def load_df_to_snowflake(conn, df, table_name):

    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name,
        quote_identifiers=True,
        chunk_size=50000
    )

    if not success:
        raise Exception(f"write_pandas failed for {table_name}")

    return nchunks, nrows


# --------------------
# Main Loop
# --------------------
csv_folder = "C:/Users/sdjos/Ecommerce_DWH/raw_data"

print("\n=== Snowflake CSV Loader Started ===\n")

for file in os.listdir(csv_folder):

    if file.endswith(".csv"):

        try:
            csv_path = os.path.join(csv_folder, file)
            table_name = file.replace(".csv", "").upper()

            print(f"Processing {file} → {table_name}")

            df = pd.read_csv(csv_path)

            print(f"Rows: {len(df)} | Columns: {len(df.columns)}")

            # Auto create table
            create_table_from_df(cursor, df, table_name)

            # Load data
            nchunks, nrows = load_df_to_snowflake(conn, df, table_name)

            print(f"Loaded {nrows} rows into {table_name} in {nchunks} chunks\n")

        except Exception as e:
            print(f"Failed for {file}: {str(e)}\n")

# --------------------
# Cleanup
# --------------------
cursor.close()
conn.close()

print("=== All CSV files processed ===")