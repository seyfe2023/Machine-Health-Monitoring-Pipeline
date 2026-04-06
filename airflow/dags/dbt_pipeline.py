from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "seyfe",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="machine_health_pipeline",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command='cd /opt/airflow/dbt && dbt run --profiles-dir /opt/airflow/dbt',
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command='cd /opt/airflow/dbt && dbt test --profiles-dir /opt/airflow/dbt',
    )

    dbt_run >> dbt_test
