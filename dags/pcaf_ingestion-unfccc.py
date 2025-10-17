import requests
from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import os
import urllib.request
import pandas as pd
import zipfile

with DAG(
    "pcaf_ingestion-unfccc",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=False
) as dag:

    @task(task_id="load_data_to_s3_bucket")
    def load_data_to_s3_bucket():
        url = "https://zenodo.org/records/8159736/files/parquet-only.zip"
        local_file = "parquet-only.zip"

        if os.path.isfile(local_file):
            os.remove(local_file)

        with urllib.request.urlopen(url) as file:
            with open(local_file, "wb") as new_file:
                new_file.write(file.read())

        with zipfile.ZipFile(local_file, "r") as zf:
            s3_hook = S3Hook(aws_conn_id='s3')
            for parquet_file_name in zf.namelist():
                with zf.open(parquet_file_name, "r") as file_descriptor:
                    df = pd.read_parquet(file_descriptor)
                    parquet_bytes = df.to_parquet(compression='gzip')
                    s3_hook.load_bytes(
                        parquet_bytes,
                        bucket_name="pcaf",
                        key=f'raw/unfccc/{parquet_file_name.lower()}',
                        replace=True
                    )

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id="trino_connection",
        sql="CREATE SCHEMA IF NOT EXISTS hive.pcaf WITH (location = 's3a://pcaf/data')"
    )

    create_annexi_table = SQLExecuteQueryOperator(
        task_id="create_annexi_table",
        conn_id="trino_connection",
        sql="""
            CREATE TABLE IF NOT EXISTS hive.pcaf.annexi (
                party varchar,
                category varchar,
                classification varchar,
                measure varchar,
                gas varchar,
                unit varchar,
                year varchar,
                numberValue double,
                stringValue varchar
            )
            WITH (
                external_location = 's3a://pcaf/raw/unfccc/data/annexi',
                format = 'PARQUET'
            )
        """
    )

    create_non_annexi_table = SQLExecuteQueryOperator(
        task_id="create_non_annexi_table",
        conn_id="trino_connection",
        sql="""
            CREATE TABLE IF NOT EXISTS hive.pcaf.non_annexi (
                party varchar,
                category varchar,
                classification varchar,
                measure varchar,
                gas varchar,
                unit varchar,
                year varchar,
                numberValue double,
                stringValue varchar
            )
            WITH (
                external_location = 's3a://pcaf/raw/unfccc/data/non-annexi',
                format = 'PARQUET'
            )
        """
    )

    # Task dependencies
    load_data_to_s3_bucket() >> create_schema >> [create_annexi_table, create_non_annexi_table]
