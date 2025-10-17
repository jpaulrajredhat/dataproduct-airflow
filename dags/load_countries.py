from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import pycountry

def escape_sql(value: str) -> str:
    return value.replace("'", "''") if isinstance(value, str) else value

countries = [
    (escape_sql(c.alpha_3), escape_sql(c.alpha_2), escape_sql(c.name))
    for c in pycountry.countries
]

insert_values = ", ".join(
    [f"('{iso3}', '{iso2}', '{name}')" for iso3, iso2, name in countries]
)

insert_stmt = f"INSERT INTO hive.pcaf.countries VALUES {insert_values}"

with DAG(
    dag_id="upload_countries_to_trino_sql",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["trino", "pcaf", "metadata"]
) as dag:

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id="trino_connection",
        sql="CREATE SCHEMA IF NOT EXISTS hive.pcaf",
    )

    drop_table = SQLExecuteQueryOperator(
        task_id="drop_table",
        conn_id="trino_connection",
        sql="DROP TABLE IF EXISTS hive.pcaf.countries",
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="trino_connection",
        sql="""
        CREATE TABLE hive.pcaf.countries (
            country_iso3_code VARCHAR,
            country_iso2_code VARCHAR,
            country_name VARCHAR
        )
        """,
    )

    insert_data = SQLExecuteQueryOperator(
        task_id="insert_data",
        conn_id="trino_connection",
        sql=insert_stmt,
    )

    create_schema >> drop_table >> create_table >> insert_data
