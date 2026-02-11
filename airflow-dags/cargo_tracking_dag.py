from airflow import DAG
from airflow.operators.bash import BashOperator
from fivetran_provider.operators.fivetran import FivetranOperator
from datetime import datetime, timedelta

FIVETRAN_CONNECTOR_ID = "FIVETRAN-CONNECTOR-ID-GOES-HERE"

with DAG(
    dag_id="opensky_fivetran_dbt_dag",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    default_args = {
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
        'depends_on_past': False,
    },
):
    fivetran_sync = FivetranOperator(
        task_id="run_fivetran_sync",
        fivetran_conn_id="fivetran_connection_name_goes_here",
        connector_id=FIVETRAN_CONNECTOR_ID,
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run --project-dir /opt/airflow/dbt_transformation --profiles-dir /home/airflow/.dbt'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test --project-dir /opt/airflow/dbt_transformation --profiles-dir /home/airflow/.dbt'
    )
    
    fivetran_sync >> dbt_run >> dbt_test