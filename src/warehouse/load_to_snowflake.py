import os
import tempfile
import boto3
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_BUCKET = "upi-lakehouse"


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin123",
    )


def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "UPI_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "UPI_FRAUD_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    )


def sync_prefix_to_snowflake(s3, cur, prefix: str, stage: str, table: str):
    resp = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
    parquet_keys = [
        obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".parquet")
    ]
    if not parquet_keys:
        print(f"No Parquet files found under {prefix}")
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        for key in parquet_keys:
            local_path = os.path.join(tmpdir, os.path.basename(key))
            s3.download_file(MINIO_BUCKET, key, local_path)
            sf_path = local_path.replace("\\", "/")
            cur.execute(f"PUT 'file://{sf_path}' @{stage} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

    cur.execute(f"""
        COPY INTO {table}
        FROM @{stage}
        FILE_FORMAT = (TYPE = 'PARQUET')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'CONTINUE'
    """)
    print(f"Loaded {len(parquet_keys)} Parquet file(s) from '{prefix}' into {table}.")


def main():
    s3 = get_minio_client()
    conn = get_snowflake_conn()
    try:
        cur = conn.cursor()
        sync_prefix_to_snowflake(s3, cur, "bronze/upi_transactions/", "STG_UPI_TXNS", "RAW_UPI_TRANSACTIONS")
        sync_prefix_to_snowflake(s3, cur, "silver/fraud_alerts/", "STG_FRAUD_ALERTS", "RAW_FRAUD_ALERTS")
    finally:
        conn.close()


if __name__ == "__main__":
    main()