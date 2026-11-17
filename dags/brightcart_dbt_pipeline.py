"""
BrightCart end to end pipeline: Airflow + dbt.

Airflow runs the workflow (when, in what order, retries, failures).
dbt builds and tests the analytical models (staging, intermediate, marts).

    check_input_files >> clean_orders >> build_city_summary >> run_quality_check
        >> load_raw_to_warehouse >> dbt_build >> publish_completion_message

dbt is installed in .venv_airflow, so start Airflow with that venv activated.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG, Param
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from pipeline_tasks import (  # noqa: E402
    check_input_files,
    clean_orders,
    build_city_summary,
    run_quality_check,
)
from warehouse_tasks import load_raw_to_warehouse  # noqa: E402

DBT_DIR = PROJECT_ROOT / "dbt" / "brightcart_analytics"

RUN_DATE = "{{ params.run_date if params.run_date else ds_nodash }}"

DEFAULT_ARGS = {
    "owner": "brightcart",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="brightcart_dbt_pipeline",
    description="BrightCart daily files, validated in Python, modelled and tested with dbt",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "run_date": Param("", type="string",
                          description="Day to process as YYYYMMDD (e.g. 20261105). Empty = logical date."),
    },
    tags=["brightcart", "dbt"],
) as dag:

    check_files = PythonOperator(task_id="check_input_files", python_callable=check_input_files,
                                 op_kwargs={"run_date": RUN_DATE})

    clean = PythonOperator(task_id="clean_orders", python_callable=clean_orders,
                           op_kwargs={"run_date": RUN_DATE})

    build_summary = PythonOperator(task_id="build_city_summary", python_callable=build_city_summary,
                                   op_kwargs={"run_date": RUN_DATE})

    quality_check = PythonOperator(task_id="run_quality_check", python_callable=run_quality_check,
                                   op_kwargs={"run_date": RUN_DATE}, retries=0)

    # Source readiness for dbt: only rows that passed the quality gate reach the warehouse
    load_warehouse = PythonOperator(task_id="load_raw_to_warehouse", python_callable=load_raw_to_warehouse,
                                    op_kwargs={"run_date": RUN_DATE})

    # dbt build: build the models and run the tests, in dependency order.
    # retries=0: a SQL error or a failed test fails again the same way.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd \"{DBT_DIR}\" && dbt build",
        retries=0,
    )

    publish_message = BashOperator(
        task_id="publish_completion_message",
        bash_command=f'echo "BrightCart marts published for {RUN_DATE}: all checks and dbt tests passed"',
    )

    check_files >> clean >> build_summary >> quality_check >> load_warehouse >> dbt_build >> publish_message
