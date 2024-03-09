import pymysql
import csv
import boto3
import configparser

#1. pipline.conf파일 설정


#2. mysql_config파일 설정

    # [mysql_config]
    # hostname = my_host.com
    # port = 3306
    # username = my_user_name
    # password = my_password
    # database = db_name

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