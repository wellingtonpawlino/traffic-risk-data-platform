from datetime import datetime, timedelta

from airflow import DAG

# Fallback: algumas versões não têm EmptyOperator
try:
    from airflow.operators.empty import EmptyOperator
except Exception:
    from airflow.operators.dummy_operator import DummyOperator as EmptyOperator

from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="infosiga_pipeline_master",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,   # manual
    catchup=False,
    tags=["master", "infosiga", "orchestration"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Repassa dt_ref se você triggar a master com:
    # {"dt_ref": "2026-05-02"}
    conf_payload = {
        "dt_ref": "{{ dag_run.conf.get('dt_ref') if dag_run and dag_run.conf else None }}"
    }

    # 1) Bronze ingestion
    trigger_bronze = TriggerDagRunOperator(
        task_id="trigger_infosiga_bronze_ingestion",
        trigger_dag_id="infosiga_bronze_ingestion",
        conf=conf_payload,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # 2) Silver processing
    trigger_silver = TriggerDagRunOperator(
        task_id="trigger_infosiga_silver_processing",
        trigger_dag_id="infosiga_silver_processing",
        conf=conf_payload,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # 3) Data preparation
    trigger_prep = TriggerDagRunOperator(
        task_id="trigger_infosiga_data_preparation",
        trigger_dag_id="infosiga_data_preparation",
        conf=conf_payload,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    # 4) Loads para Postgres (PARALELO)
    trigger_load_sinistros = TriggerDagRunOperator(
        task_id="trigger_infosiga_load_sinistros",
        trigger_dag_id="infosiga_load_sinistros",
        conf=conf_payload,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_load_pessoas = TriggerDagRunOperator(
        task_id="trigger_infosiga_load_pessoas",
        trigger_dag_id="infosiga_load_pessoas",
        conf=conf_payload,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    trigger_load_veiculos = TriggerDagRunOperator(
        task_id="trigger_infosiga_load_veiculos",
        trigger_dag_id="infosiga_load_veiculos",
        conf=conf_payload,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    done = EmptyOperator(task_id="done")

    # Sequência + paralelização
    start >> trigger_bronze >> trigger_silver
    trigger_silver >> [trigger_load_sinistros, trigger_load_pessoas, trigger_load_veiculos] >> done