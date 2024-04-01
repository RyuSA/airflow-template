from datetime import datetime, timedelta
import json
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import (
    PythonOperator,
)
from env import Env
import logging

logger = logging.getLogger(__name__)

# pull data from xcom
def python_callable(**context):
    ti = context['ti']
    xcom_value = ti.xcom_pull(key='now')
    logger.info(xcom_value)

# pull data from xcom
def xcom_push(**context):
    ti = context['ti']
    now = datetime.now().isoformat()
    ti.xcom_push(key="now", value=now)
    logger.info(now)

def task():
    import time
    logger.info("sleeping...")
    time.sleep(10)
    logger.info("hello!")

# `Generator` is a class to generate Airflow DAG
class Generator:

    def __init__(self, name) -> None:
        self.name = name

    def generate(self):
        from env import Env
        env = Env()

        with DAG(
            dag_id=self.name+"_generated",
            default_args={
                "depends_on_past": False,
                "email_on_failure": False,
                "email_on_retry": False,
                "retries": 1,
                "retry_delay": timedelta(minutes=5),
                "is_paused_upon_creation": False,
            },
            start_date=datetime(2021, 1, 1),
            schedule_interval='@hourly', 
            catchup=False,
            tags=["danger"],
        ) as dag:
            push = PythonOperator(
                task_id='xcompush',
                python_callable=xcom_push,
                dag=dag
            )

            t1 = BigQueryExecuteQueryOperator(
                task_id='query',
                sql='resources/query.sql',
                use_legacy_sql=False,
                dag=dag
            )

            t2 = BigQueryExecuteQueryOperator(
                task_id='query2',
                sql='resources/query.sql',
                use_legacy_sql=False,
                dag=dag
            )

            t3 = BigQueryExecuteQueryOperator(
                task_id='query3',
                sql='resources/query.sql',
                use_legacy_sql=False,
                dag=dag
            )

            t4 = BigQueryExecuteQueryOperator(
                task_id='query4',
                sql='resources/query.sql',
                use_legacy_sql=False,
                dag=dag
            )

            t5 = BigQueryExecuteQueryOperator(
                task_id='query5',
                sql='resources/query.sql',
                use_legacy_sql=False,
                dag=dag
            )

            t6 = BigQueryExecuteQueryOperator(
                task_id='query6',
                sql='resources/query.sql',
                use_legacy_sql=False,
                dag=dag
            )
            
            pull = PythonOperator(
                task_id='python',
                python_callable=python_callable,
                dag=dag
            )

            push >> t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> pull
