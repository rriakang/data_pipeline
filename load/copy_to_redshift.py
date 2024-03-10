## load

import boto3
import configparser
import psycopg2

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
dbname = parser.get("aws_creds","database")
user = parser.get("aws_creds","username")
password = parser.get("aws_creds","password")
host = parser.get("aws_creds","host")
port = parser.get("aws_creds","port")


#Redshift 클러스터에 연결

rs_conn = psycopg2.connect(

    "dbname=" + dbname
    + " user=" + user
    + " password=" + password
    + " host=" + host
    + " port=" + port
)
parser = configparser.ConfigParser()
parser.read("pipeline.conf")
account_id = parser.get("aws_boto_credentials","account_id")
iam_role = parser.get("aws_creds","iam_role")
bucket_name = parser.get("aws_boto_credentials","bucket_name")


#대상 테이블을 truncate
sql = "TRUNCATE public.Orders;"
cur = rs_conn.cursor()
cur.execute(sql)

cur.close()
rs_conn.commit()

#redshift에 파일을 로드하기 위해 COPY 명령을 실행

file_path = (
    "s3://" + bucket_name + "/order_extract.csv"
)
role_string = ("arn:aws:iam::" 
               + account_id
               + ":role/" + iam_role)

sql = "COPY public.Orders"
sql = sql + " from %s "
sql = sql + " iam_role %s;"

#cursor 객체를 생성하고 COPY를 실행

cur = rs_conn.cursor()
cur.execute(sql,(file_path,role_string))

#cursor를 종료하고 트랙잭션을 커밋

cur.close()
rs_conn.commit()

#연결을 종료

rs_conn.close()