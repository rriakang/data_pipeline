import json
import pathlib

import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id = "download_rocket_launches", #dag 이름
    start_date = airflow.utils.dates.days_ago(14), #dag 처음 실행 시작 날짜
    schedule_interval = None, #dag 실행 간격
)
 
download_launches = BashOperator( #데이터 가져오는 작업 
    task_id = "download_launches", #태스크 이름 
    bash_comman = "curl -o /tmp/launches.json -L'https://ll.thespacedevs.com/2.0.0/launch/upcoming'", dag=dag,
    
)

def _get_pictures(): #파이썬 함수는 결괏값을 파싱하고 모든 로켓 사진을 다운로드
    #경로가 존재하는지 확인
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
    #launches.json 파일에 있는 모든 그림 파일을 다운로드
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try :
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file,"wb") as f:
                    f.write(response.content)
                print(f"Downloaded{image_url} appears to be an invalied URL")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect  to {image_url}.")

get_pictures = PythonOperator(
    task_id = "get_pictures",
    python_callabe = _get_pictures,
    dag = dag,
)

notify = BashOperator(
    task_id= "notify",
    bash_command = 'echo "There are now $(ls /tmp/image/ | wc -l ) images."',
    dag = dag,
)

download_launches >> get_pictures >> notify #태스크 실행 순서 결정