import pendulum
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from cross_dag import FetchDagOperator

def read_config(**kwargs):
    task_instance = kwargs['ti']
    return task_instance.xcom_pull(task_ids='fetch_dag')
    

args = {
    'start_date': datetime(2021, 1, 23, 
        tzinfo=pendulum.timezone("Europe/Berlin")),
    'email': ['team@example.com'],
    'email_on_failure': False
}


dag = DAG(
    dag_id='fetch_dag',
    default_args=args,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False
)

with dag:
    fetch_dag = FetchDagOperator(task_id="fetch_dag")
    read_config = PythonOperator(
        task_id = 'read_config',
        python_callable=read_config,
        provide_context=True
    )

fetch_dag >> read_config