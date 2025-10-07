import psycopg2
import pandas as pd
import os

def load_data(output_path: str):
    # สร้าง connection ภายในฟังก์ชัน
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow")
    )
    conn.autocommit = True

    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM public.covid_data LIMIT 1000000;")
        df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
        df.to_csv(output_path, index=False)
    finally:
        # ปิด cursor และ connection ทุกครั้งหลังใช้งาน
        cur.close()
        conn.close()
