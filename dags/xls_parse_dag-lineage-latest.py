from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import re
from trino.dbapi import connect
from airflow.datasets import Dataset
import os
import io

# -------- Constants -------- #
S3_CONN_ID = "s3"        # <-- Airflow Connection ID for MinIO (type: S3)
RAW_XLS = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "dags", "data", "single_family.xlsx")
PARQUET_FILE = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "dags", "data", "single_family.parquet")
S3_BUCKET = "audit"
S3_KEY = "single_family.parquet"

# -------- Required Columns -------- #
REQUIRED_COLUMNS = [
    "Reference Pool ID", "Loan Identifier", "Monthly Reporting Period", "Channel",
    "Seller Name", "Servicer Name", "Master Servicer", "Original Interest Rate",
    "Current Interest Rate", "Original UPB", "UPB at Issuance", "Current Actual UPB",
    "Original Loan Term", "Origination Date", "First Payment Date", "Loan Age",
    "Remaining Months to Legal Maturity", "Remaining Months To Maturity", "Maturity Date",
    "Original Loan to Value Ratio (LTV)", "Original Combined Loan to Value Ratio (CLTV)",
    "Number of Borrowers", "Debt-To-Income (DTI)", "Borrower Credit Score at Origination",
    "Co-Borrower Credit Score at Origination", "First Time Home Buyer Indicator",
    "Loan Purpose", "Property Type", "Number of Units", "Occupancy Status",
    "Property State", "Metropolitan Statistical Area (MSA)", "Zip Code Short",
    "Mortgage Insurance Percentage", "Amortization Type", "Prepayment Penalty Indicator",
    "Interest Only Loan Indicator", "Interest Only First Principal And Interest Payment Date",
    "Months to Amortization", "Current Loan Delinquency Status", "Loan Payment History",
    "Modification Flag"
]

DATE_COLUMNS_MMYYYY = [
    "Monthly Reporting Period",
    "Origination Date",
    "First Payment Date",
    "Maturity Date",
    "Interest Only First Principal And Interest Payment Date"
]

# -------- Transformation maps -------- #
channel_map = {'R': 'Retail','B': 'Broker','C': 'Correspondent','T': 'TPO Not Specified','9': 'Not Available'}
first_time_buyer_map = {'Y': 'Yes','N': 'No'}
loan_purpose_map = {
    'P': 'Purchase','C': 'Refinance - Cash Out','N': 'Refinance - No Cash Out',
    'R': 'Refinance - Not Specified','9': 'Not Available'
}

# -------- Helpers -------- #
def convert_mm_yyyy_to_date(value):
    if pd.isna(value):
        return None
    try:
        return datetime.strptime(str(int(value)), "%m%Y").date().replace(day=1)
    except Exception:
        return None

def apply_transformations(df: pd.DataFrame) -> pd.DataFrame:
    df = df[REQUIRED_COLUMNS].copy()
    for col in DATE_COLUMNS_MMYYYY:
        df[col] = df[col].apply(convert_mm_yyyy_to_date)
        df[col] = pd.to_datetime(df[col], errors='coerce')
    df["Current Loan Delinquency Status"] = pd.to_numeric(df["Current Loan Delinquency Status"], errors='coerce')
    return df

def apply_transform_rules(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    df['Channel'] = df['Channel'].map(channel_map).fillna(df['Channel'])
    df['First Time Home Buyer Indicator'] = df['First Time Home Buyer Indicator'].map(first_time_buyer_map).fillna(df['First Time Home Buyer Indicator'])
    if 'Loan Purpose' in df.columns:
        df['Loan Purpose'] = df['Loan Purpose'].map(loan_purpose_map).fillna(df['Loan Purpose'])
    return df

def sanitize_column_name(col: str) -> str:
    return re.sub(r'\W+', '_', col).strip('_').lower()

def pandas_schema_to_iceberg(df):
    schema_lines = []
    sanitized_columns = []
    for col, dtype in df.dtypes.items():
        col_sanitized = sanitize_column_name(col)
        sanitized_columns.append(col_sanitized)
        if pd.api.types.is_integer_dtype(dtype):
            trino_type = "BIGINT"
        elif pd.api.types.is_float_dtype(dtype):
            trino_type = "DOUBLE"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            trino_type = "TIMESTAMP"
        else:
            trino_type = "VARCHAR"
        schema_lines.append(f"{col_sanitized} {trino_type}")
    df.columns = sanitized_columns
    return ",\n".join(schema_lines)

# -------- Core Tasks -------- #
def transform_and_upload():
    conn = BaseHook.get_connection(S3_CONN_ID)
    extra = conn.extra_dejson
    endpoint_url = extra.get("endpoint_url")

    df = pd.read_excel(RAW_XLS, engine="openpyxl")
    df = apply_transform_rules(df)
    df = apply_transformations(df)

    table = pa.Table.from_pandas(df)
    pq.write_table(table, PARQUET_FILE)
    # instead of Write to PARQUET_FILE write to in-memory buffer
    # buffer = io.BytesIO()
    # pq.write_table(table, buffer)
    # buffer.seek(0)  # reset pointer


    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if S3_BUCKET not in buckets:
        s3.create_bucket(Bucket=S3_BUCKET)
        
    s3.upload_file(PARQUET_FILE, S3_BUCKET, S3_KEY)
    # Upload buffer directly to s3 instead from file
    # s3.upload_fileobj(buffer, S3_BUCKET, S3_KEY)

def insert_into_iceberg():
    conn = connect(host="trino", port=8081, user="admin", catalog="iceberg")
    cursor = conn.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS iceberg.single_family")

    df = pd.read_parquet(PARQUET_FILE)
    schema_sql = pandas_schema_to_iceberg(df)

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS iceberg.single_family.loans (
            {schema_sql}
        )
    """)

    for _, row in df.iterrows():
        values = []
        for val in row:
            if pd.isna(val):
                values.append("NULL")
            elif isinstance(val, str):
                values.append(f"'{val.replace("'", "''")}'")
            elif isinstance(val, pd.Timestamp):
                values.append(f"TIMESTAMP '{val.strftime('%Y-%m-%d %H:%M:%S')}'")
            else:
                values.append(str(val))
        sql = f"INSERT INTO iceberg.single_family.loans VALUES ({','.join(values)})"
        cursor.execute(sql)

# -------- DAG Definition -------- #
with DAG(
    "single_family_pipeline_lineage",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    load_and_transform = PythonOperator(
        task_id="load_and_transform",
        python_callable=transform_and_upload,
        inlets=[Dataset("file://" + RAW_XLS)],
        outlets=[Dataset(f"s3://{S3_BUCKET}/{S3_KEY}")],
    )

    upload_to_minio = PythonOperator(
        task_id="insert_to_iceberg",
        python_callable=insert_into_iceberg,
        inlets=[Dataset("file://" + RAW_XLS)],
        outlets=[Dataset(f"s3://{S3_BUCKET}/{S3_KEY}")],
    )

    load_and_transform >> upload_to_minio
