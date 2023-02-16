from .migration_task import main as migrate_tasks
from datetime import timedelta, datetime
from airflow.operators.python_operator import PythonOperator


with DAG(
    "tutorial",
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args=def_arguements,
    # [END default_args]
    description="A simple tutorial DAG",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    hello_operator = PythonOperator(
        task_id='migration_task', 
        python_callable=migrate_tasks, 
        dag=dag
    )

hello_operator