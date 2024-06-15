from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def http_request():
    import requests
    response = requests.get('http://www.google.com')
    print(response.status_code)
    print(response.text)

dag = DAG('requests-sample', 
    description='A simple DAG to show how to use requests library in PythonOperator',
    schedule_interval='0 12 * * *',
    start_date=datetime(2024, 1, 1), 
    catchup=False
)

start_task = DummyOperator(task_id='start', dag=dag)

request_task = PythonOperator(
    task_id='http_request',
    python_callable=http_request,
    dag=dag
)

end_task = DummyOperator(task_id='end', dag=dag)

start_task >> request_task >> end_task