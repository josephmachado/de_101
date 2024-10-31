import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

import duckdb

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

    @task
    def write_out_preagg_to_sqlite(sqlite3_db='./data/sqlite3_tpch_preagg_tbls.db', duckdb_file='/opt/airflow/dags/tpch_warehouse/dbt.duckdb'):
        if os.path.isfile(sqlite3_db):
            os.remove(sqlite3_db)

        conn = duckdb.connect(duckdb_file)
        cursor = conn.cursor()

        # This code gets data from our preagg tables and writes it into a sqlite3 format for visualization with Metabase
        cursor.execute(f"ATTACH '{sqlite3_db}' AS sqlite_db (TYPE SQLITE)")
        cursor.execute(f"CREATE TABLE sqlite_db.customer_outreach_metrics AS SELECT * FROM customer_outreach_metrics")
        cursor.execute(f"CREATE TABLE sqlite_db.order_lineitem_metrics AS SELECT * FROM order_lineitem_metrics")

        conn.commit()
        conn.close()
        

    stop_pipeline = DummyOperator(task_id='stop_pipeline')

    run_dbt >> test_dbt >> write_out_preagg_to_sqlite() >> stop_pipeline
