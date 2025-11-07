#### 
## Gramedia Digital - Data Engineer Take Home Test
## by Mario Caesar // caesarmario87@gmail.com
## DAG Orchestrator to extract data from FakeStore API
####

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta

default_args = {"owner": "data-eng", "retries": 0}

with DAG(
    dag_id          = "00_dag_fakestore_orchestrator",
    schedule        = "0 * * * *",
    start_date      = datetime(2025, 1, 1),
    catchup         = False,
    max_active_runs = 1,
    default_args    = default_args,
    tags            = ["fakestore","orchestrator","extract"],
) as dag:

    t00_start = EmptyOperator(task_id="t00_start")

    triggers = []

    t_trigger_products = TriggerDagRunOperator(
        task_id             = "t10_trigger_extract_products",
        trigger_dag_id      = "00_01_dag_fakestore_extract_products",
        conf                = {"ds": "{{ ds }}"},
        wait_for_completion = True,
        reset_dag_run       = True,
        poke_interval       = 30,
        allowed_states      = ["success"],
        failed_states       = ["failed"],
    )
    triggers.append(t_trigger_products)

    t_trigger_carts = TriggerDagRunOperator(
        task_id             = "t10_trigger_extract_carts",
        trigger_dag_id      = "00_02_dag_fakestore_extract_carts",
        conf                = {"ds": "{{ ds }}"},
        wait_for_completion = True,
        reset_dag_run       = True,
        poke_interval       = 30,
        allowed_states      = ["success"],
        failed_states       = ["failed"],
    )
    triggers.append(t_trigger_carts)

    t_trigger_users = TriggerDagRunOperator(
        task_id             = "t10_trigger_extract_users",
        trigger_dag_id      = "00_03_dag_fakestore_extract_users",
        conf                = {"ds": "{{ ds }}"},
        wait_for_completion = True,
        reset_dag_run       = True,
        poke_interval       = 30,
        allowed_states      = ["success"],
        failed_states       = ["failed"],
    )
    triggers.append(t_trigger_users)

    t90_finish = EmptyOperator(task_id="t90_finish")

    # Parallel (default) — kalau mau sequential, chain satu-satu di render_scripts
    t00_start >> triggers >> t90_finish