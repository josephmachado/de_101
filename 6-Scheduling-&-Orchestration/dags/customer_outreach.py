from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'customer_outreach_etl',
    description='A simple DAG to generate customer outreach metrics',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /opt/airflow/dags/tpch_warehouse && dbt run',
    )
    
    test_dbt = BashOperator(
        task_id='test_dbt',
        bash_command='cd /opt/airflow/dags/tpch_warehouse && dbt run',
    )

    stop_pipeline = DummyOperator(task_id='stop_pipeline')

    run_dbt >> test_dbt >> stop_pipeline
