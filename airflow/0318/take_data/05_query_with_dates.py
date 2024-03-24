
import datetime as dt
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# 특정 시간 간격에 대한 이벤트 데이터 가져오기
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

# 특정 날짜 지점을 위해 템플릿 사용하기

fetch_events = BashOperator(
    task_id = "fetch_events",
    bash_command=(
        "mkdir -p /data && "
        "curl -o /data/events.json "
        "http://localhost:5000/events?"
        "start_date = {{execution_date.strftime('%Y-%m-%d')}}"
        "&end_date = {{next_execution_date.strftime('%Y-%m-%d')}}"
    ),

    dag = dag,


),