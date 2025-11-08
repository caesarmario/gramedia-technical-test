####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## DAG Orchestrator: Trigger per-resource extract DAGs
####

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

default_args = {"owner": "caesarmario87@gmail.com", "retries": 0}

with DAG(
    dag_id          = "00_dag_fakestore_orchestrator",
    schedule        = "0 1 * * *",
    start_date      = datetime(2025, 1, 1),
    catchup         = False,
    max_active_runs = 1,
    default_args    = default_args,
    tags            = ["fakestore", "orchestrator", "extract"],
) as dag:

    # Start anchor
    t00_start = EmptyOperator(task_id="t00_start")

    # Group all extract triggers so they run in parallel but are visually grouped.
    with TaskGroup(group_id="tg_extract_all", tooltip="Trigger per-resource extract DAGs") as t10_extract_all:


        TriggerDagRunOperator(
            task_id             = "t10_trigger_extract_products",
            trigger_dag_id      = "00_01_dag_fakestore_extract_products",
            conf                = {"ds": "{{ ds }}"},
            wait_for_completion = True,
            reset_dag_run       = True,
            poke_interval       = 30,
            allowed_states      = ["success"],
            failed_states       = ["failed"],
        )

        TriggerDagRunOperator(
            task_id             = "t10_trigger_extract_carts",
            trigger_dag_id      = "00_02_dag_fakestore_extract_carts",
            conf                = {"ds": "{{ ds }}"},
            wait_for_completion = True,
            reset_dag_run       = True,
            poke_interval       = 30,
            allowed_states      = ["success"],
            failed_states       = ["failed"],
        )

        TriggerDagRunOperator(
            task_id             = "t10_trigger_extract_users",
            trigger_dag_id      = "00_03_dag_fakestore_extract_users",
            conf                = {"ds": "{{ ds }}"},
            wait_for_completion = True,
            reset_dag_run       = True,
            poke_interval       = 30,
            allowed_states      = ["success"],
            failed_states       = ["failed"],
        )

    # Trigger transform orchestrator after ALL extracts complete
    t20_trigger_transform_orchestrator = TriggerDagRunOperator(
        task_id             = "t20_trigger_01_dag_fakestore_orchestrator",
        trigger_dag_id      = "01_dag_fakestore_transform_orchestrator",
        conf                = {"ds": "{{ ds }}"},
        wait_for_completion = False,
        reset_dag_run       = True,
        poke_interval       = 30,
        allowed_states      = ["success"],
        failed_states       = ["failed"],
    )

    # End anchor
    t90_finish = EmptyOperator(task_id="t90_finish")

    # Flow: start → [all extracts] → trigger transform orchestrator → finish
    t00_start >> t10_extract_all >> t20_trigger_transform_orchestrator >> t90_finish