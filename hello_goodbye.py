from datetime import datetime
from airflow import DAG

# For airflow 2.1.0
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# For airflow 1.10.10
#from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.python_operator import PythonOperator


def print_hello(**kwargs):
    return 'Hello world!'


def print_goodbye(**kwargs):
    return 'Goodbye world'


dag = DAG('hello_world', description='Simple tutorial DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2021, 3, 20), 
          catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

hello_operator = PythonOperator(task_id='hello_task', python_callable=print_hello, dag=dag)

goodbye_operator = PythonOperator(task_id='print_goodbye', python_callable=print_goodbye, dag=dag)

dummy_operator >> hello_operator >> goodbye_operator
