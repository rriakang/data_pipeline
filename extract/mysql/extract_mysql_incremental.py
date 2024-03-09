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
rs_sql = """SELECT COALESCE(MAX(LastUpdated),
        '1900-01-01'
        FROM Orders;"""
rs_cursor = rs_conn.cursor()
rs_cursor.execute(rs_sql)
result = rs_cursor.fetchone()

#오직 하나의 레코드만 반환됨
last_updated_warehouse = result[0]

rs_cursor.close()
rs_cursor.commit()



#MySQL 연결 정보를 가져온 뒤 연결
parser = configparser.ConfigParser()
parser.read("pipline.conf")
hostname = parser.get("mysql_config","hostname")
port = parser.get("mysql_config","port")
username = parser.get("mysql_config","username")
dbname= parser.get("mysql_config","database")
password = parser.get("mysql_config","password")

conn = pymysql.connect(host=hostname,
        user=username,
        password=password,
        db=dbname,
        port=int(port))

if conn is None :
    print("Error connecting to the M")

else :
    print("Mysql connection established!")

# 추출을 수행하기 위해 pymysql 라이브러리의 cursor 객체를 사용하여 SELECT 쿼리 실행

m_query = """SELECT *
    FROM Orders
    WHERE LastUpdated > %s;"""
local_filename = "order_extract.csv"

m_cursor = conn.cursor()
m_cursor.execute(m_query,(last_updated_warehouse,))
results = m_cursor.fetchall()

with open(local_filename, 'w') as fp:
    csv_w = csv.writer(fp, delimiter='|')
    csv_w.writerows(results)
fp.close()
m_cursor.close()
conn.close()


#csv파일을 s3버킷에 업로드하는 코드임

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

#스크립트 실행시 s3버킷 csv파일 넣어짐