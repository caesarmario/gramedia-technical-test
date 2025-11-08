####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## DAG Orchestrator: trigger all L1 loads
####

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup


default_args = {"owner": "caesarmario87@gmail.com", "retries": 0}

with DAG(
    dag_id="02_dag_fakestore_load_orchestrator",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fakestore", "orchestrator", "load", "l1"],
) as dag:

    # Start anchor
    t00_start = EmptyOperator(task_id="t00_start")

    # Group all per-resource load triggers (runs in parallel)
    with TaskGroup(group_id="tg_load_resources") as t10_load:

        # Trigger one child load DAG for this resource.
        TriggerDagRunOperator(
            task_id="t10_trigger_load_products",
            trigger_dag_id="02_01_dag_fakestore_load_products",
            conf={"ds": "{{ ds }}"},
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=30,
            allowed_states=["success"],
            failed_states=["failed"],
        )

        # Trigger one child load DAG for this resource.
        TriggerDagRunOperator(
            task_id="t10_trigger_load_carts",
            trigger_dag_id="02_02_dag_fakestore_load_carts",
            conf={"ds": "{{ ds }}"},
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=30,
            allowed_states=["success"],
            failed_states=["failed"],
        )

        # Trigger one child load DAG for this resource.
        TriggerDagRunOperator(
            task_id="t10_trigger_load_users",
            trigger_dag_id="02_03_dag_fakestore_load_users",
            conf={"ds": "{{ ds }}"},
            wait_for_completion=True,
            reset_dag_run=True,
            poke_interval=30,
            allowed_states=["success"],
            failed_states=["failed"],
        )

    # End anchor
    t90_finish = EmptyOperator(task_id="t90_finish")

    # Flow: start → grouped triggers → finish
    t00_start >> t10_load >> t90_finish