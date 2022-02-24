import pendulum
import shutil
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

def backup_by_python_callable(**kwargs):
    task_instance = kwargs['ti']
    paths = task_instance.xcom_pull(task_ids='list_dags')
    for path in paths.split(" "):
        source = f"/usr/local/airflow/dags/{path}"
        target = f"/usr/local/airflow/backup/{path}"
        print(f"{source} -> {target}")
        shutil.copy(source, target)



args = {
    'start_date': datetime(2021, 1, 23, 
        tzinfo=pendulum.timezone("Europe/Berlin")),
    'email': ['team@example.com'],
    'email_on_failure': False
}


dag = DAG(
    dag_id='xcoms',
    default_args={'start_date': datetime(2021,1,23)},
    schedule_interval=None,
    max_active_runs=1,
    catchup=False
)

list_dags = BashOperator(
    task_id = 'list_dags',
    bash_command='ls /usr/local/airflow/dags  | grep -v __py | xargs',
    dag=dag,
    xcom_push=True
)

create_backup_directory = BashOperator(
    task_id = 'create_backup_directory',
    bash_command='mkdir -p /usr/local/airflow/backup',
    dag=dag
)

backup_dags = BashOperator(
    task_id = 'backup_dags',
    bash_command = "echo '{{task_instance.xcom_pull(task_ids='list_dags')}}' ",
    dag=dag
)

backup_by_python = PythonOperator(
    task_id = 'backup_by_python',
    python_callable=backup_by_python_callable,
    provide_context=True,
    dag=dag
)

[create_backup_directory, list_dags] >> backup_dags >> backup_by_python