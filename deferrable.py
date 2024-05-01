from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.time_delta import TimeDeltaSensorAsync

default_args = {
    'owner': 'ryusa',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('deferred_example',
    default_args=default_args,
    description='A simple tutorial DAG with deferrable tasks',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    @task
    def prepare_data():
        print("Preparing data...")

    @task
    def process_data():
        print("Processing data...")

    start_task = prepare_data()

    # Deferrable task that waits for 5 minutes before proceeding
    wait_for_5_minutes = TimeDeltaSensorAsync(
        task_id='wait_for_5_minutes',
        delta=timedelta(minutes=5)
    )

    end_task = process_data()

    start_task >> wait_for_5_minutes >> end_task