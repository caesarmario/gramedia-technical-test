####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## DAG Orchestrator: trigger per-resource transform DAGs
####

from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {"owner": "data-eng", "retries": 0}

with DAG(
    dag_id="01_dag_fakestore_transform_orchestrator",
    schedule="0 * * * *",     # hourly (adjust to your cadence)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["fakestore","orchestrator","transform","parquet"],
) as dag:

    t00_start = EmptyOperator(task_id="t00_start")

    triggers = []
    t_trigger_products = TriggerDagRunOperator(
        task_id="t10_trigger_transform_products",
        trigger_dag_id="01_01_dag_fakestore_transform_products",
        conf={"ds": "{{ ds }}"},
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )
    triggers.append(t_trigger_products)
    t_trigger_carts = TriggerDagRunOperator(
        task_id="t10_trigger_transform_carts",
        trigger_dag_id="01_02_dag_fakestore_transform_carts",
        conf={"ds": "{{ ds }}"},
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )
    triggers.append(t_trigger_carts)
    t_trigger_users = TriggerDagRunOperator(
        task_id="t10_trigger_transform_users",
        trigger_dag_id="01_03_dag_fakestore_transform_users",
        conf={"ds": "{{ ds }}"},
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )
    triggers.append(t_trigger_users)

    t90_finish = EmptyOperator(task_id="t90_finish")

    t00_start >> triggers >> t90_finish