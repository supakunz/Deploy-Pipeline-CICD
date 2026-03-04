import psycopg2
import pandas as pd
import os
import boto3
from botocore.exceptions import ClientError

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

        minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        minio_access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
        minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
        minio_bucket = os.getenv("MINIO_BUCKET", "data-lake")
        minio_object_key = os.getenv("MINIO_RAW_OBJECT_KEY", "covid/raw/covid_19_raw.csv")
        use_ssl = os.getenv("MINIO_USE_SSL", "false").lower() == "true"

        s3_client = boto3.client(
            "s3",
            endpoint_url=minio_endpoint,
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key,
            use_ssl=use_ssl,
        )

        try:
            s3_client.head_bucket(Bucket=minio_bucket)
        except ClientError:
            s3_client.create_bucket(Bucket=minio_bucket)

        s3_client.upload_file(output_path, minio_bucket, minio_object_key)
        print(
            f"Raw data uploaded to MinIO bucket='{minio_bucket}' key='{minio_object_key}' "
            f"from {output_path}"
        )
    finally:
        # ปิด cursor และ connection ทุกครั้งหลังใช้งาน
        cur.close()
        conn.close()
