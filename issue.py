import datetime
import random
import time
from typing import Any, Dict, List

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup


client_qty = 50
CLIENTS_METADATA: List[Dict[str, Any]] = list()
for i in range(client_qty):
    if i // 2 == 0:
        client_group = "4"
        shared_resource_group = "us-01"
        client_size = "normal"
    else:
        client_group = "3"
        shared_resource_group = "us-02"
        client_size = "large"
    CLIENTS_METADATA.append(
        {
            "clientId": f"client{i}",
            "clientGroup": client_group,
            "sharedResourceGroup": shared_resource_group,
            "isDataIsolated": False,
            "resourceTags": {"size": client_size},
        }
    )

DE_STACK_NAME = "de-prod-us-01"


ARGS = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2000, 1, 1, 0, 0, 0),
}


def task(**kwargs):
    rand_int = random.randint(1, 10)
    print(f"Sleeping: {rand_int}s")
    time.sleep(rand_int)
    return rand_int


def build_abstract_dag(client: dict) -> DAG:
    """One copy of the graph per client"""
    client_id = client.get('clientId')
    client_group = client.get('clientGroup')
    resource_tags = client.get('resourceTags')
    resource_group = client.get('sharedResourceGroup')

    dag_id = 'client-specific-dag-{}'.format(client_id)

    dag = DAG(
        dag_id=dag_id,
        default_args=ARGS,
        schedule_interval=None,
        is_paused_upon_creation=False,
        catchup=False,
        # CHANGE THE QTY OF PARAMS HERE TO SEE THE IMPACT TO DAG PROCESSING RUNTIME
        params={
            'client_id': client_id,
            'client_group': client_group,
            'resource_tags': resource_tags,
            'resource_group': resource_group,
            'airflow_stack': DE_STACK_NAME,
        },
    )
    qty_task_groups = 20
    for i in range(qty_task_groups):
        with TaskGroup(f"task_group_{i}", dag=dag):
            t1 = PythonOperator(dag=dag, task_id="task_1", python_callable=task, retries=0)
            t2 = PythonOperator(dag=dag, task_id="task_2", python_callable=task, retries=0)
            t3 = PythonOperator(dag=dag, task_id="task_3", python_callable=task, retries=0)
            t4 = PythonOperator(dag=dag, task_id="task_4", python_callable=task, retries=0)

            [t3, t2] << t1
            t4 << t1

    return dag

def retry_all_main_graph_tasks(retry_meta: list, **context):
    print(f"Retrying: {retry_meta}")

dag_meta = DAG(
    dag_id='a-trigger-dag',
    default_args=ARGS,
    dagrun_timeout=datetime.timedelta(hours=8),
    is_paused_upon_creation=True,
    catchup=False,
    schedule_interval='00 05 * * *',  # daily, at 0500 UTC
)

trigger_dag_tasks = list()
retry_main_graph_tasks_meta = list()

for client_data in CLIENTS_METADATA:
    abstract_dag = build_abstract_dag(
        client=client_data,
    )
    trigger_dag_id = abstract_dag.dag_id
    globals()[trigger_dag_id] = abstract_dag

    trigger_operator = TriggerDagRunOperator(
        dag=dag_meta,
        task_id='trigger_{}'.format(trigger_dag_id),
        retries=1,
        trigger_dag_id=trigger_dag_id,
        wait_for_completion=True,
        allowed_states=[State.SUCCESS, State.FAILED],
        failed_states=['not-a-state'],
        execution_date='{{ data_interval_start }}',
        execution_timeout=datetime.timedelta(hours=2)
    )
    trigger_dag_tasks.append(trigger_operator)
    retry_main_graph_tasks_meta.append(
        dict(
            trigger_task_id=trigger_operator.task_id,
            dag=abstract_dag,
        )
    )

retry_main_graph_tasks = PythonOperator(
    dag=dag_meta,
    task_id='retry_all_main_graph_tasks',
    retries=1,
    trigger_rule='all_done',
    op_kwargs={
        'retry_meta': retry_main_graph_tasks_meta,
    },
    python_callable=retry_all_main_graph_tasks,
)
retry_main_graph_tasks << trigger_dag_tasks
