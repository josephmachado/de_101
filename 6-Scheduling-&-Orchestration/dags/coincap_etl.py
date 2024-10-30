import csv
import os
from datetime import datetime, timedelta

import requests

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    'coincap_elt',
    description='A simple DAG to fetch data \
    from CoinCap Exchanges API and write to a file',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    url = "https://api.coincap.io/v2/exchanges"
    file_path = f'{os.getenv("AIRFLOW_HOME")}/data/coincap_exchanges.csv'

    @task
    def fetch_coincap_exchanges(url, file_path):
        response = requests.get(url)
        data = response.json()
        exchanges = data['data']
        if exchanges:
            keys = exchanges[0].keys()
            with open(file_path, 'w') as f:
                dict_writer = csv.DictWriter(f, fieldnames=keys)
                dict_writer.writeheader()
                dict_writer.writerows(exchanges)

    stop_pipeline = DummyOperator(task_id='stop_pipeline')

    fetch_coincap_exchanges(url, file_path) >> stop_pipeline