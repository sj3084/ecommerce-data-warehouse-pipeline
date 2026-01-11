def run_sql_file(cursor, file_path):
    with open(file_path, "r") as f:
        sql = f.read()
        for stmt in sql.split(";"):
            if stmt.strip():
                cursor.execute(stmt)
