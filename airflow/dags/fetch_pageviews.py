from urllib import request
import gzip
import shutil

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

dag = DAG(
    dag_id="listing_4_20",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    max_active_runs=1,
)

def _get_data(year, month, day, hour, output_path):
    url = (
        f"https://dumps.wikimedia.org/other/pageviews/{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)

def _extract_gz(gz_path, output_path):
    with gzip.open(gz_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

def _fetch_pageviews(pagenames, execution_date, **kwargs):
    hook = PostgresHook(postgres_conn_id="my_postgres")
    result = dict.fromkeys(pagenames, 0)
    # execution_date를 문자열로 변환
    execution_date_str = execution_date.strftime("%Y-%m-%d %H:%M:%S")
    with open("/tmp/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts
                # execution_date 대신 변환된 문자열 사용
                hook.run("INSERT INTO pageview_counts VALUES(%s, %s, %s)", 
                         parameters=(page_title, view_counts, execution_date_str))


get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={
        "year": "{{ execution_date.year }}",
        "month": "{{ execution_date.month }}",
        "day": "{{ execution_date.day }}",
        "hour": "{{ execution_date.hour }}",
        "output_path": "/tmp/wikipageviews.gz",
    },
    dag=dag,
)

extract_gz = PythonOperator(
    task_id="extract_gz",
    python_callable=_extract_gz,
    op_kwargs={
        "gz_path": "/tmp/wikipageviews.gz",
        "output_path": "/tmp/wikipageviews",
    },
    dag=dag,
)

fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
        "pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"},
    },
    dag=dag,
)

get_data >> extract_gz >> fetch_pageviews
