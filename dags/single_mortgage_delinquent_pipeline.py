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

import requests
import urllib3
from trino.dbapi import connect
from trino.auth import JWTAuthentication
from trino.auth import OAuth2Authentication
import numpy as np


# Per your setup
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# -------- Constants -------- #
S3_CONN_ID = "s3"        # <-- Airflow Connection ID for MinIO (type: S3)
RAW_XLS = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "dags", "data", "single_family.xlsx")
PARQUET_FILE = os.path.join(os.environ.get("AIRFLOW_HOME", "/opt/airflow"), "dags", "data", "single_family.parquet")
S3_BUCKET = "mortgage"
S3_KEY = "single_family.parquet"
RAW_XLS_KEY = "raw-data/single_family/single_family.xlsx"
PARQUET_S3_KEY = "processed-data/single_family.parquet"



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


def get_trino_connection(use_keycloak=False):
    """
    Utility to switch between standard and Keycloak authentication.
    """
    # Standard connection parameters
    OAUTH_URL = os.environ.get("OAUTH_URL")
    OAUTH_CLIENT_ID = os.environ.get("OAUTH_CLIENT_ID", "trino")
    OAUTH_CLIENT_SECRET = os.environ.get("OAUTH_CLIENT_SECRET", "1z47wp2T746BvzAVF9U8mBGFDi1nTKr9")
    OAUTH_USER_NAME = os.environ.get("OAUTH_USER_NAME", "admin")
    OAUTH_PASSWORD = os.environ.get("OAUTH_PASSWORD", "Redhat2026$")
    TRINO_HOST = os.environ.get("TRINO_HOST")

    print("DEBUG OAUTH_URL-1:", OAUTH_URL)
    
    conn_params = {
        "host": TRINO_HOST,
        "catalog": "iceberg",
        "schema": "single_family",
    }

    if use_keycloak:
        # 1. Exchange Client Secret for a JWT Token
        # Update these URLs/Credentials to your environment        

        payload = {
            "grant_type": "password",
            "client_id": OAUTH_CLIENT_ID,
            "client_secret": OAUTH_CLIENT_SECRET,
            "username": OAUTH_USER_NAME,
            "password": OAUTH_PASSWORD
        }
     
        print("DEBUG OAUTH_URL-2:", OAUTH_URL)
        response = requests.post(OAUTH_URL, data=payload, verify=False)
        response.raise_for_status()
        token = response.json().get("access_token")

        # 2. Update params for Secure JWT connection
        conn_params.update({
            "port": 443,
            "http_scheme": "https",
            "auth": JWTAuthentication(token),
            # "auth": OAuth2Authentication(token),
            "verify": False
        })
    else:
        # Standard Insecure connection
        conn_params.update({
            "port": 8080,
            "user": "admin",
            "http_scheme": "http"
        })

    return connect(**conn_params)


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
def transform_and_upload_xls():
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
def transform_and_upload():
    # Get S3 connection
    conn = BaseHook.get_connection(S3_CONN_ID)
    extra = conn.extra_dejson
    endpoint_url = extra.get("endpoint_url")

    # Read XLS directly from MinIO S3
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )

    # Read XLS file from S3 into memory
    # response = s3.get_object(Bucket=S3_BUCKET, Key=RAW_XLS_KEY)
    # df = pd.read_excel(response["Body"], engine="openpyxl")
    
    # download to memory buffer
    obj = s3.get_object(Bucket=S3_BUCKET, Key=RAW_XLS_KEY)
    buffer = io.BytesIO(obj["Body"].read())

    # now Pandas can safely read it
    df = pd.read_excel(buffer, engine="openpyxl")


    # Apply your transformations
    df = apply_transform_rules(df)
    df = apply_transformations(df)

    # Convert to Parquet in-memory
    table = pa.Table.from_pandas(df)
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer)
    parquet_buffer.seek(0)  # Reset pointer to the start


    # Ensure bucket exists
    if S3_BUCKET not in [b["Name"] for b in s3.list_buckets()["Buckets"]]:
        s3.create_bucket(Bucket=S3_BUCKET)

    # Upload Parquet directly from buffer
    s3.upload_fileobj(parquet_buffer, S3_BUCKET, PARQUET_S3_KEY)
    print(f"Uploaded Parquet to s3://{S3_BUCKET}/{PARQUET_S3_KEY}")

def insert_into_iceberg():
    # conn = connect(host="trino", port=8080, user="admin", catalog="iceberg")
    conn = get_trino_connection(use_keycloak=True)

    cursor = conn.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS iceberg.single_family")

    df = pd.read_parquet(PARQUET_FILE)
    schema_sql = pandas_schema_to_iceberg(df)

    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS iceberg.single_family.loans (
            {schema_sql}
        )
    """)

    # for _, row in df.iterrows():
    #    values = []
    #    for val in row:
    #        if pd.isna(val):
    #            values.append("NULL")
    #        elif isinstance(val, str):
    #            values.append(f"'{val.replace("'", "''")}'")
    #        elif isinstance(val, pd.Timestamp):
    #            values.append(f"TIMESTAMP '{val.strftime('%Y-%m-%d %H:%M:%S')}'")
    #        else:
    #            values.append(str(val))
    #    sql = f"INSERT INTO iceberg.single_family.loans VALUES ({','.join(values)})"
    #    cursor.execute(sql)

    # all_rows = []

    # for _, row in df.iterrows():
    #    values = []
    #    for val in row:
    #        if pd.isna(val):
    #            values.append("NULL")
    #        elif isinstance(val, (str, pd.Timestamp)):
                # Escaping single quotes and formatting strings/dates
    #            clean_val = str(val).replace("'", "''")
    #            values.append(f"'{clean_val}'")
    #        else:
    #            values.append(str(val))
        
        # Wrap values in parentheses: (val1, val2, ...)
    #    all_rows.append(f"({','.join(values)})")

    # 1. Fetch metadata and force keys to lowercase
    # cursor.execute("DESCRIBE iceberg.single_family.loans")
    # Map name -> type (e.g., {'loan_id': 'bigint'})
    # columns_metadata = {row[0].lower(): row[1].lower() for row in cursor.fetchall()}
    cursor.execute("DESCRIBE iceberg.single_family.loans")
    meta_rows = cursor.fetchall()
    if not meta_rows:
        raise RuntimeError("OPA denied metadata access! Check 'FilterColumns' permission.")
    
    # Map lowercase col names to types: {'loan_id': 'bigint'}
    col_to_type = {row[0].lower(): row[1].lower() for row in meta_rows}
    all_rows = []
    for _, row in df.iterrows():
        sql_values = []
        for col_name in df.columns:
            val = row[col_name]
            dtype = col_to_type.get(col_name.lower(), "varchar")
            
            # Format value based on type
            if pd.isna(val):
                f_val = "NULL"
            elif "timestamp" in dtype:
                ts = pd.to_datetime(val).strftime('%Y-%m-%d %H:%M:%S.%f')
                f_val = f"TIMESTAMP '{ts}'"
            elif any(t in dtype for t in ["double", "bigint", "int", "decimal"]):
                f_val = str(val)
            else:
                clean_str = str(val).replace("'", "''")
                f_val = f"'{clean_str}'"
            
            # Wrap in CAST to satisfy Trino Type Matching
            sql_values.append(f"CAST({f_val} AS {dtype})")
        
        all_rows.append(f"({','.join(sql_values)})")

    # 5. Execute in Chunks of 500 to prevent Coordinator timeouts
    target_cols = ",".join([c.lower() for c in df.columns])
    for chunk in np.array_split(all_rows, max(1, len(all_rows) // 500)):
        if len(chunk) > 0:
            insert_sql = f"INSERT INTO iceberg.single_family.loans ({target_cols}) VALUES {','.join(chunk)}"
            cursor.execute(insert_sql)
            print(f"Inserted chunk of {len(chunk)} rows.")

    

# -------- DAG Definition -------- #
with DAG(
    "single_family_mortgage_delinquent_pipeline",
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
