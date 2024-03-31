

#추가적인 수집 / 정제 테스크 추가하기
fetch_sales_old = PythonOperator (...)

clean_sales_old = PythonOperator (...)

fetch_sales_new = PythonOperator (...)
clean_sales_new= PythonOperator (...)

fetch_sales_old >> clean_sales_old
fetch_sales_new >> clean_sales_new


# BranchPythonOperator 를 사용한 브랜치 작업

def _pick_erp_system(**context):
    ...
_pick_erp_system=BranchPythonOperator (
    task_id = "pick_erp_syetem",
    python_callable = _pick_erp_system,

)