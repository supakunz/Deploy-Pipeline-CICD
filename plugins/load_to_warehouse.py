import os
import boto3
import pandas as pd
from botocore.exceptions import ClientError


def load_to_warehouse(input_path: str, output_path: str):
    # Keep local output behavior for compatibility with existing workflow.
    df = pd.read_csv(input_path)
    df.to_csv(output_path, index=False)

    minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    minio_bucket = os.getenv("MINIO_BUCKET", "data-lake")
    minio_object_key = os.getenv("MINIO_OBJECT_KEY", "covid/output/covid_19_clean.csv")
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
        f"Data uploaded to MinIO bucket='{minio_bucket}' key='{minio_object_key}' "
        f"from {output_path}"
    )
