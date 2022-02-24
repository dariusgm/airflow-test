import pendulum
from datetime import datetime
from airflow.models import DAG
from cross_dag import TriggerDagOperator

args = {
    'start_date': datetime(2021, 1, 23, 
        tzinfo=pendulum.timezone("Europe/Berlin")),
    'email': ['team@example.com'],
    'email_on_failure': False
}


dag = DAG(
    dag_id='trigger_dag',
    default_args=args,
    schedule_interval=None,
    max_active_runs=1,
    catchup=False
)

with dag:
    trigger_dag = TriggerDagOperator(
        trigger_dag_id="fetch_dag",
        data={'some_data': True},
        task_id="trigger_dag",    
    )

