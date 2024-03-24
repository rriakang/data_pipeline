from urllib import request

import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag = DAG(
    dag_id="listing_4_20",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@hourly",
    template_searchpath="/tmp",
    max_active_runs=1,
)


def _get_data(year, month, day, hour, output_path):
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)


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
extract_gz = BashOperator(
    task_id = "extract_gz",
    bash_command = "gunzip --force /tmp/wikipageviews.gz",
    dag = dag,
)


def _fetch_pageviews(pagenames, execution_date, **_):
    result = dict.fromkeys(pagenames,0) # 0으로 모든 페이지 뷰에 대한 결과를 초기화한다.
    with open(f"/tmp/wikipageviews","r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts #페이지 뷰 저장


    with open(f"/tmp/postgress_query.sql","w") as f:
        for pagename, pageviewcount in result.items(): #각 결과에 대해 SQL 쿼리 작성
            f.write(
                "INSERT INTO pageview_counts VALUES("
                f"'{pagename}',{pageviewcount},'{execution_date}'"
                ");\n"
            )

fetch_pageviews = PythonOperator(
    task_id = "fetch_pageviews",
    python_callable = _fetch_pageviews,
    op_kwargs = {
        "pagenames" : {
            "Google",
            "Amazon",
            "Apple",
            "Microsoft",
            "Facebook",
        }
    },
    dag = dag,
)

write_to_postgres = PostgresOperator(
    task_id="write_to_postgres",
    postgres_conn_id="my_postgres", #연결에 사용할 인증 정보의 식별자
    sql="postgres_query.sql", # sql 쿼리 또는 sql 쿼리를 포함하는 파일의 경로
    dag=dag,
)
get_data >> extract_gz >> fetch_pageviews >> write_to_postgres