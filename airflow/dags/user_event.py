

#!!!!사용자 이벤트 처리하기!!!!

# 웹 사이트에서 사용자 동작을 추적하고 사용자가 웹사이트에서 엑세스한 페이지를 분석할수있는 서비스가 있다고 가정
# 마케팅 목적으로 우리는 사용자들이 얼마나 많은 다양한 페이지에 접근하고 그들이 방문할 동안 얼마나 많은 시간을 소비하는지 알고싶음
# 시간이 지남에 따라 사용자 행동이 어떻게 변하는지 알기 위해, 우리는 이 통계량을 매일 계산하려고 함

# 외부 추적 서비스는 실용성을 이유로 30일 이상 데이터를 저장하지 않음
# 하지만 우리는 더 오랜 기간 동안 과거 데이터를 보존하고 싶기 때문에 , 직접 이 데이터를 모아 저장할것임
# 일반적으로 데이터가 매우 클 수 있기 때문에 아마존의 S3/ 구글 Cloud Storage 서비스와 같은 클라우드 스토리지 서비스에 데이터를 저장하는것이 합리적
# -> 나중에 s3 연결 해볼 예정


import datetime as dt
from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id = "01_unscheduled",
    start_date = dt.datetime(2019,1,1),
    schedule_interval = None,
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