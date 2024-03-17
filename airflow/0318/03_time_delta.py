

#!!!!사용자 이벤트 처리하기!!!!

# 빈도 기반의 스케줄 간격 정의하기

# 매주 작업을 실행하거나 매주 토요일 23시 45분에 DAG를 정의하려면?


import datetime as dt
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id = "03_time_data",
    schedule_interval = dt.timedelta(days=3),
    start_date = dt.datetime(year=2019,month=1,day=1),
    end_date = dt.datetime(year=2019,month=1,day=5),
)


fetch_events = BashOperator(
    task_id = "fetch_events",
    bash_command = (
        "mkdir -p /data && "
        "curl -o /data/events.json "
        "https://localhost:5000/events" # api에서 이벤트를 가져온 후 저장
    ),
    dag = dag,
)

def _calculate_status(input_path,output_path):
    """이벤트 통계 계산하기"""
    events = pd.read_json(input_path)
    stats = events.groupby(["data","user"]).size().reset_index() # 이벤트 데이터를 로드하고 필요한 통계를 계산
    Path(output_path).parent.mkdir(exist_ok=True)  # 출력 디렉터리가 있는지 확인하고 결과를 csv 파일로 저장
    stats.to_csv(output_path,index=False)

   
calculate_status = PythonOperator(
    task_id = "calculate_stats",
    python_callable = _calculate_status,
    op_kwargs = {
        "input_path" : "/data/events.json",
        "output_path" : "/data/stats.csv",
    },
    dag = dag
)

fetch_events >> calculate_status 