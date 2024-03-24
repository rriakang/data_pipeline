

# #파티셔닝

# fetch_events = BashOperator(
#     task_id = "fetch_events",
#     bash_command = (
#         "mkdir -p /data/events && "
#         "curl -o /data/events/{{ds}}.json" #변환된 값이 템플릿 파일 이름에 기록된다
#         "http://localhost:5000/events?"
#         "start_date = {{ds}}&"
#         "end_date =  {{next_ds}}",
#     dag = dag,

#     )
# )