
import datetime as dt
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

fetch_events = BashOperator(
    task_id = "fetch_events",
    bash_command=(
        "mkdir -p /data &&"
        "curl -o /data/events.json"
        "http://localhost:5000/events?"
        "start_date=2019-01-01&"
        "end_date= 2019-01-02"
    ),

    dag = dag,
),