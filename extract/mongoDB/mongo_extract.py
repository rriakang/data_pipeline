from pymongo import MongoClient
import datetime
import configparser
import boto3
from datetime import timedelta
import csv


# PyMongo와 Boto3을 임포트하여 MongoDB 데이터베이스에서 데이터를 추출하고 그 결과를 s3 버킷에 저장할 수 있다.
# 또한 csv 라이브러리를 가져와 추출된 데이터를 수집 로드 단계에서 데이터 웨어하우스로 쉽게 가져올 수 있는 플랫 파일로 정형화하고 쓸 수 있다.

#mongo_config 값을 로드

parser = configparser.ConfigParser()

parser.read("pipeline.conf")
hostname = parser.get("mongo_config","hostname")
username = parser.get("mongo_config","username")
password = parser.get("mongo_config","password")
database_name = parser.get("mongo_config","database")
collection_name = parser.get("mongo_config","collection")

mongo_client = MongoClient(
    "mongodb+srv://" + username
    + ":" + password
    + "@" + hostname
    + "/" + database_name
    + "?retryWrites = true&"
    + "w=majority&ssl=true&"
    + "ssl_cert_reqs=CERT_NONE"
)

#컬렉션이 위치한 db에 연결
mongo_db = mongo_client[database_name]

#문서를 쿼리할 컬렉션을 선택

mongo_collection = mongo_db[collection_name]

#mongo_collection의 .find()기능을 호출하여 찾으려는 문서를 쿼리

start_date = datetime.datetime.today() + timedelta(days = -1)
end_date = start_date + timedelta(days=1)

mongo_query = {
    "$and" :[{"event_timestamp" : {"$gte":start_date}},{"event_timestamp":{"$lt":end_date}}]}

event_docs = mongo_collection.find(mongo_query,batch_size=3000)

##이벤트는 사용자가 로그인, 페이지 보기 또는 피드백 양식 제출과 같은 것을 나타낼 수 있음

#결과를 저장할 빈 리스트를 생성
all_events = []

#커서를 통해 반복 작업

for doc in event_docs:
    #기본 값
    event_id = str(doc.get("event_id",-1))
    event_timestamp = doc.get("event_timestamp",None)
    event_name = doc.get("event_name",None)

    #리스트에 모든 이벤트 속성을 추가

    current_event = []
    current_event.append(event_id)
    current_event.append(event_timestamp)
    current_event.append(event_name)

    #이벤트의 최종 리스트에 이벤트를 추가
    all_events.append(current_event)


export_file = "export_file.csv"
with open(export_file,'w') as fp :
    csvw = csv.writer(fp,delimiter='|')
    csvw.writerows(all_events)

fp.close()

parser = configparser.ConfigParser()
parser.read("pipeline.conf")
access_key = parser.get('aws_boto_credentials',"access_key")
secret_key = parser.get('aws_boto_credentials',"secret_key")
bucket_name = parser.get('aws_boto_credentials',"bucket_name")

s3 = boto3.client('s3',
    aws_access_key_id = access_key,
    aws_secret_access_key = secret_key 
                  )

s3_file = export_file
s3.upload_file(export_file,bucket_name,s3_file)
