import pandas as pd
import psycopg2
import os
import time
import re

# รอให้ PostgreSQL พร้อม
time.sleep(5)

DB_HOST = os.environ.get("POSTGRES_HOST", "postgres")
DB_PORT = os.environ.get("POSTGRES_PORT", 5432)
DB_NAME = os.environ.get("POSTGRES_DB", "airflow")
DB_USER = os.environ.get("POSTGRES_USER", "airflow")
DB_PASS = os.environ.get("POSTGRES_PASSWORD", "airflow")

CSV_FILE = "/app/data/covid_19_data.csv"
TABLE_NAME = "covid_data"

df = pd.read_csv(CSV_FILE)

# sanitize column names → Province/State → Province_State
df.columns = [re.sub(r'\W+', '_', col) for col in df.columns]

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=DB_USER,
    password=DB_PASS
)
cur = conn.cursor()

# สร้าง table ถ้ายังไม่มี
columns = ", ".join([f"{col} TEXT" for col in df.columns])
cur.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} ({columns});")
conn.commit()

# insert ข้อมูล
for _, row in df.iterrows():
    placeholders = ", ".join(["%s"] * len(row))
    sql = f"INSERT INTO {TABLE_NAME} VALUES ({placeholders})"
    cur.execute(sql, tuple(row))
conn.commit()

cur.close()
conn.close()

print(f"Data from {CSV_FILE} inserted into {TABLE_NAME} successfully!")
