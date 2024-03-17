from datetime import timedelta
from airflow import DAG
# Airflow 2.0 이상
from airflow.operators.bash import BashOperator # bash 명령

from airflow.utils.dates import days_ago

dag = DAG(
    'simple_dag',
    description = 'A simple DAG',
    schedule_interval = timedelta(days=1),
    start_date = days_ago(1),
)


t1 = BashOperator(
    task_id = 'print_date',
    bash_command ='date',
    dag = dag,
)

t2 = BashOperator(
    task_id ='sleep',
    depends_on_past = False,
    bash_command = 'sleep3',
    dag = dag,
)

t3 = BashOperator(
    task_id='print_end',
    bash_command='echo "Pipeline finished"',
    depends_on_past=False,  # 올바른 인수 이름을 사용합니다.
    dag=dag,
)


t1 >> t2 
t2 >> t3