import psycopg2
import pymysql
import csv
import boto3
import configparser

#데이터 웨어하우스의 주문 테이블에서 MAX(LastUpdated) 값을 쿼리

#1. pipline.conf파일 설정


#2. mysql_config파일 설정

    # [mysql_config]
    # hostname = my_host.com
    # port = 3306
    # username = my_user_name
    # password = my_password
    # database = db_name

#Redshift db connection 정보를 가져옴






parser = configparser.ConfigParser()
parser.read("pipline.conf")
dbname = parser.get("postgres_config","database")
user = parser.get("postgres_config","username")
password = parser.get("postgres_config","password")
host = parser.get("postgres_config","host")
port = parser.get("postgres_config","port")

#Redshift 클러스터에 연결

conn = psycopg2.connect(

    "dbname=" + dbname
    + " user=" + user
    + " password=" + password
    + " host=" + host,
    port = port
)


m_query = "SELECT * FROM Orders;"
local_filename = "order_extract.csv"

m_cursor = conn.cursor()
m_cursor.execute(m_query)
results = m_cursor.fetchall()

with open(local_filename, 'w') as fp:
    csv_w = csv.writer(fp, delimiter='|')
    csv_w.writerows(results)
fp.close()
m_cursor.close()
conn.close()

#aws_boto_credentials 값을 로드

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get('aws_boto_credentials',"access_key")
secret_key = parser.get('aws_boto_credentials',"secret_key")
bucket_name = parser.get('aws_boto_credentials',"bucket_name")

s3 = boto3.client('s3',
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key 
                  )

s3_file = local_filename

s3.upload_file(local_filename,bucket_name,s3_file)

