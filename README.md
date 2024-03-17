# ⏲️ data_pipeline
데이터 파이프라인 핵심 python code

## :fire: 데이터 수집 : 데이터 추출



#### 1. MySQL 데이터베이스에서 데이터 추출
 AWS - Redshift/S3/IAM

#### 2. 전체 증분 또는 증분 MySQL 테이블 추출

#### 3. MySQL 데이터의 이진 로그 복제
 -> 대용량 데이터 수집이 필요한 경우 변경 사항을 복제하기 위해 이진로그 사용이 효율적임
 
#### 4. PostgreSQL 데이터베이스에서 데이터 추출

#### 5. 전체 또는 증분 Postgres 테이블 추출

#### 6. Write-Ahead 로그를 사용한 데이터 복제

#### 7. MongoDB에서 데이터 추출
   <br/>
   `pip install pymongo1`
   <br/>
   `pip install dnspython`

#### 8. REST API에서 데이터 추출
   <br/>
   `pip install requests`

## :fire: airflow 

   <br/>
   #### airflow 설치
   `pip install apache-airflow`
   <br/>
   `cd airflow`
    <br/>
   #### airflow db init
   `airflow db init`
   <br/>
   `mkdir dags`
   <br/>
   #### 관리자 계정 설정
   `airflow users create -u admin -p admin -f Clueless -l Coder -r Admin -e admin@admin.com`
   <br/>
   #### airflow 실행
   `airflow webserver -p 8080`



