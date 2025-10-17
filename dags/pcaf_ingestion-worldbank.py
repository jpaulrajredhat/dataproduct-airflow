import os
import urllib.request
import zipfile
import pendulum
import pandas as pd

from airflow import DAG
from airflow.decorators import task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.trino.hooks.trino import TrinoHook

with DAG(
    "pcaf_ingestion-worldbank",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    @task(task_id="load_data_to_s3_bucket")
    def load_data_to_s3_bucket():
        # Define the URL to download the ZIP file from World Bank API.
        url = (
            "https://api.worldbank.org/v2/country/all/indicator/"
            "NY.GDP.MKTP.CD;NY.GDP.MKTP.PP.CD;SP.POP.TOTL?source=2&downloadformat=csv"
        )
        local_file = "worldbank.zip"

        # Remove existing file if it exists
        if os.path.isfile(local_file):
            os.remove(local_file)

        # Download the file
        with urllib.request.urlopen(url) as response, open(local_file, "wb") as out_file:
            out_file.write(response.read())

        # Open the ZIP file in binary read mode.
        with zipfile.ZipFile(open(local_file, "rb")) as zip_file:
            s3_hook = S3Hook(aws_conn_id="s3")
            for file_name in zip_file.namelist():
                print(f"Processing file: {file_name}")
                # Skip metadata files
                if "Metadata" not in file_name:
                    with zip_file.open(file_name, "r") as file_descriptor:
                        # Read CSV, skipping the first 4 rows
                        df = pd.read_csv(file_descriptor, skiprows=4, quotechar='"')
                        # Convert DataFrame to Parquet bytes with gzip compression
                        parquet_bytes = df.to_parquet(compression="gzip")
                        # Upload the bytes to S3 using the required 'bytes_data' parameter
                        s3_hook.load_bytes(
                            bytes_data=parquet_bytes,
                            bucket_name="pcaf",
                            key="raw/worldbank/worldbank.parquet",
                            replace=True,
                        )

    # Create the schema in Trino using the new SQLExecuteQueryOperator.
    trino_create_schema = SQLExecuteQueryOperator(
        task_id="trino_create_schema",
        conn_id="trino_connection",
        sql="CREATE SCHEMA IF NOT EXISTS hive.pcaf WITH (location = 's3a://pcaf/data')",
        handler=list,
    )

    trino_drop_table = SQLExecuteQueryOperator(
        task_id="trino_drop_table",
        conn_id="trino_connection",
        sql=f"drop table if exists hive.pcaf.worldbank",
        handler=list,
    )

    # Create the table in Trino referencing the S3 Parquet data.
    trino_create_worldbank_table = SQLExecuteQueryOperator(
        task_id="trino_create_worldbank_table",
        conn_id="trino_connection",
        sql="""CREATE TABLE IF NOT EXISTS hive.pcaf.worldbank (
            "Country Name" varchar,
            "Country Code" varchar,
            "Indicator Name" varchar,
            "Indicator Code" varchar,
            "1960" double,
            "1961" double,
            "1962" double,
            "1963" double,
            "1964" double,
            "1965" double,
            "1966" double,
            "1967" double,
            "1968" double,
            "1969" double,
            "1970" double,
            "1971" double,
            "1972" double,
            "1973" double,
            "1974" double,
            "1975" double,
            "1976" double,
            "1977" double,
            "1978" double,
            "1979" double,
            "1980" double,
            "1981" double,
            "1982" double,
            "1983" double,
            "1984" double,
            "1985" double,
            "1986" double,
            "1987" double,
            "1988" double,
            "1989" double,
            "1990" double,
            "1991" double,
            "1992" double,
            "1993" double,
            "1994" double,
            "1995" double,
            "1996" double,
            "1997" double,
            "1998" double,
            "1999" double,
            "2000" double,
            "2001" double,
            "2002" double,
            "2003" double,
            "2004" double,
            "2005" double,
            "2006" double,
            "2007" double,
            "2008" double,
            "2009" double,
            "2010" double,
            "2011" double,
            "2012" double,
            "2013" double,
            "2014" double,
            "2015" double,
            "2016" double,
            "2017" double,
            "2018" double,
            "2019" double,
            "2020" double,
            "2021" double,
            "2022" double,
            "2023" double,
            "2024" double
        )
        WITH (
            external_location = 's3a://pcaf/raw/worldbank/',
            format = 'PARQUET'
        )""",
        handler=list,
    )

    load_data_to_s3_bucket() >> trino_create_schema >> trino_drop_table >> trino_create_worldbank_table
