####
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## DAG Orchestrator: trigger per-resource transform DAGs
####

from airflow import DAG
from datetime import datetime, timedelta

# Standard providers
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

default_args = {"owner": "caesarmario87@gmail.com", "retries": 0}

with DAG(
    dag_id          = "01_dag_fakestore_transform_orchestrator",
    start_date      = datetime(2025, 1, 1),
    catchup         = False,
    max_active_runs = 1,
    default_args    = default_args,
    tags            = ["fakestore","orchestrator","transform","parquet"],
) as dag:

    # Start anchor
    t00_start = EmptyOperator(task_id="t00_start")

    # Group all transform triggers ti run in parallel
    with TaskGroup(group_id="tg_transform_all", tooltip="Trigger per-resource transform DAGs") as t10_transform_all:


        # Fire the specific transform DAG for this resource.
        # Pass the current ds via conf so the child DAG can pick the same partition.
        TriggerDagRunOperator(
            task_id             = "t10_trigger_transform_products",
            trigger_dag_id      = "01_01_dag_fakestore_transform_products",
            conf                = {"ds": "{{ ds }}"},
            wait_for_completion = True,   # block until child DAG finishes
            reset_dag_run       = True,   # re-run if an existing run with same run_id exists
            poke_interval       = 30,     # poll interval while waiting
            allowed_states      = ["success"],
            failed_states       = ["failed"],
        )

        # Fire the specific transform DAG for this resource.
        # Pass the current ds via conf so the child DAG can pick the same partition.
        TriggerDagRunOperator(
            task_id             = "t10_trigger_transform_carts",
            trigger_dag_id      = "01_02_dag_fakestore_transform_carts",
            conf                = {"ds": "{{ ds }}"},
            wait_for_completion = True,   # block until child DAG finishes
            reset_dag_run       = True,   # re-run if an existing run with same run_id exists
            poke_interval       = 30,     # poll interval while waiting
            allowed_states      = ["success"],
            failed_states       = ["failed"],
        )

        # Fire the specific transform DAG for this resource.
        # Pass the current ds via conf so the child DAG can pick the same partition.
        TriggerDagRunOperator(
            task_id             = "t10_trigger_transform_users",
            trigger_dag_id      = "01_03_dag_fakestore_transform_users",
            conf                = {"ds": "{{ ds }}"},
            wait_for_completion = True,   # block until child DAG finishes
            reset_dag_run       = True,   # re-run if an existing run with same run_id exists
            poke_interval       = 30,     # poll interval while waiting
            allowed_states      = ["success"],
            failed_states       = ["failed"],
        )

    # Trigger load orchestrator after ALL transforms finished
    t20_trigger_load_orchestrator = TriggerDagRunOperator(
        task_id             = "t20_trigger_02_dag_fakestore_load_orchestrator",
        trigger_dag_id      = "02_dag_fakestore_load_orchestrator",
        conf                = {"ds": "{{ ds }}"},
        wait_for_completion = False,
        reset_dag_run       = True,
        poke_interval       = 30,
        allowed_states      = ["success"],
        failed_states       = ["failed"],
    )

    # Final anchor
    t90_finish = EmptyOperator(task_id="t90_finish")

    # Flow: start → (parallel transform triggers) → trigger load orchestrator → finish
    t00_start >> t10_transform_all >> t20_trigger_load_orchestrator >> t90_finish