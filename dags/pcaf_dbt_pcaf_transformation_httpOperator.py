from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import time
import requests
import json

DAG_ID = "dbt_transformation"
DBT_CONN_ID = "dbt_api_conn"  # Airflow HTTP connection ID

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

def poll_dbt_status(ti):
    # Parse response from trigger task
    response_text = ti.xcom_pull(task_ids="trigger_dbt_run")
    response = json.loads(response_text)

    # Extract job_id safely
    job_id = response.get("job_id")
    print(f"Job Id being processed is : {job_id}")
    if not job_id:
        raise Exception(f"DBT API response did not contain job_id: {response}")

    # Get connection info correctly
    conn = BaseHook.get_connection(DBT_CONN_ID)
    base_url = conn.host
    if conn.port:
        base_url = f"http://{conn.host}:{conn.port}"
    else:
        base_url = f"http://{conn.host}"

    url = f"{base_url}/status/{job_id}"

    # Poll job status
    while True:
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            status = r.json()

            if status["status"] == "finished":
                if status.get("returncode", 1) != 0:
                    raise Exception(f"DBT job failed:\n{status.get('stderr')}")
                print("DBT job completed successfully!")
                return status
            else:
                print(f"Job still running: {status['status']}")
                time.sleep(5)
        except requests.RequestException as e:
            raise Exception(f"Error polling DBT API: {e}")

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # Check DBT API health
    check_dbt_api = HttpSensor(
        task_id="check_dbt_api",
        http_conn_id=DBT_CONN_ID,
        endpoint="health",
        request_params={},
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=30,
    )

    # Trigger DBT run
    trigger_dbt_run = HttpOperator(
        task_id="trigger_dbt_run",
        http_conn_id=DBT_CONN_ID,
        endpoint="dbt/run",
        method="POST",
        data=json.dumps({"command": "run", "target": "dev_with_fal"}),
        headers={"Content-Type": "application/json"},
        log_response=True,
        do_xcom_push=True,
    )

    # Poll DBT job status
    poll_job_status = PythonOperator(
        task_id="poll_dbt_status",
        python_callable=poll_dbt_status,
    )

    check_dbt_api >> trigger_dbt_run >> poll_job_status
