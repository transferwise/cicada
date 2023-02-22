import psycopg2
print(f'======>>>>>>')
# Create the test_db
pg_conn = psycopg2.connect(
    host="localhost",
    port="15432",
    user="pipelinewise",
    password="secret",
    database="postgres",
)
pg_conn.autocommit = True
pg_cur = pg_conn.cursor()
pg_cur.execute(f"CREATE DATABASE good")
