from airflow import DAG
from airflow.operators.bash import BashOperator 
from datetime import datetime

with DAG(dag_id="dag_simple", 
         start_date=datetime(2025, 1, 1), 
         schedule_interval="@daily", 
         catchup=False) as dag:

    task_1 = BashOperator(task_id="print_date", bash_command="date")

    task_2 = BashOperator(task_id="print_hello", bash_command="echo 'Hello from Airflow!'")

    task_3 = BashOperator( task_id="print_goodbye", bash_command="echo 'Goodbye from Airflow!'")


    task_1 >> task_2 >> task_3 # Task dependency: task_1 must finish before task_2
