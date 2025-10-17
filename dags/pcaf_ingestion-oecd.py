import requests
from airflow import DAG
import pendulum
from airflow.decorators import task
import os.path
import urllib.request

import os
import urllib.request
import zipfile
import pandas as pd
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.trino.hooks.trino import TrinoHook



with DAG(
    "pcaf_ingestion-oecd", start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule_interval="@daily", catchup=False
) as dag:

    @task(
        task_id="load_data_to_s3_bucket"
    )
    def load_data_to_s3_bucket():
        import pandas as pd
        import zipfile
        import urllib.request
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        url = "https://sdmx.oecd.org/sti-public/rest/data/OECD.STI.PIE,DSD_ICIO_GHG_TRADE@DF_ICIO_GHG_TRADE_2023,1.0/A.TRADE_GHG...D+_T..T_CO2E?startPeriod=1995&format=csv"
        hdr = {"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0"}
        local_file = "Guetschow_et_al_2024a-PRIMAP-hist_v2.6_final_no_rounding_13-Sep-2024.csv"
        req = urllib.request.Request(url, headers=hdr)
        local_file = "oecd_api_data.csv"
        if os.path.isfile(local_file):
             os.remove(local_file)

        if not os.path.isfile(local_file):
            print(local_file)
            with urllib.request.urlopen(req) as file:
                with open(local_file, "wb") as new_file:
                    new_file.write(file.read())
                new_file.close()

        s3_hook = S3Hook(aws_conn_id='s3')

        with open(local_file,  "r") as file_descriptor:
            df = pd.read_csv(file_descriptor)
            cols=pd.Series(df.columns.str.lower())
            print(cols[cols.duplicated()].unique())
            for dup in cols[cols.duplicated()].unique(): 
                cols[cols[cols == dup].index.values.tolist()] = [dup + '_' + str(i) if i != 0 else dup for i in range(sum(cols == dup))]
            df.columns=cols
            parquet_bytes = df.to_parquet(compression='gzip')
            s3_hook.load_bytes(parquet_bytes, bucket_name= "pcaf", key="raw/oecd/oecd.parquet", replace=True)
        
        if os.path.isfile(local_file):
             os.remove(local_file)

    
    trino_create_schema = SQLExecuteQueryOperator(
        task_id="trino_create_schema",
        conn_id="trino_connection",
        sql=f"CREATE SCHEMA IF NOT EXISTS hive.pcaf WITH (location = 's3a://pcaf/data')",
        handler=list,
    )

    trino_drop_table = SQLExecuteQueryOperator(
        task_id="trino_drop_table",
        conn_id="trino_connection",
        sql=f"drop table if exists hive.pcaf.oecd",
        handler=list,
    )

    trino_create_oecd_table = SQLExecuteQueryOperator(
        task_id="trino_create_oecd_table",
        conn_id="trino_connection",
        sql=f"""create table if not exists hive.pcaf.oecd (
                        "DATAFLOW" varchar,
                        "FREQ" varchar,
                        "MEASURE" varchar,
                        "EXPORTER" varchar,
                        "IMPORTER" varchar,
                        "ACTIVITY" varchar,
                        "PRODUCT_CATEGORY" varchar,
                        "UNIT_MEASURE" varchar,
                        "TIME_PERIOD" bigint,
                        "OBS_VALUE" double,
                        "UNIT_MULT" bigint
                        
                        )
                        with (
                         external_location = 's3a://pcaf/raw/oecd/',
                         format = 'PARQUET'
                        )""",
        handler=list,
        outlets=['hive.pcaf.oecd']
    )

    load_data_to_s3_bucket()  >> trino_create_schema >> trino_drop_table >> trino_create_oecd_table 