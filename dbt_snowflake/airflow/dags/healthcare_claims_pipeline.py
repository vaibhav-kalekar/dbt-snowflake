"""Healthcare claims: land -> freshness gate -> dbt Core build (models + tests) -> test report.

Orchestration notes
- dbt Core is invoked from the DAG; there is no dbt Cloud dependency.
- Ingestion of files into the external stage is upstream; this DAG loads staged files into RAW.
- `dbt build` runs models, snapshots, seeds and tests in DAG order and skips anything
  downstream of a failure, so bad data never reaches the marts.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG

try:  # Airflow 3.x
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:  # Airflow 2.x
    from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import (
    CopyFromExternalStageToSnowflakeOperator,
)

# Env vars, not Variable.get(): avoids a metadata-DB hit every time the DAG file is parsed.
DBT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt/dbt_snowflake")
DBT_TARGET = os.environ.get("DBT_TARGET", "prod")
DBT = f"cd {DBT_DIR} && dbt"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="healthcare_claims_pipeline",
    description="Load raw claims, gate on freshness, dbt build with tests, publish test summary",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["dbt", "snowflake", "healthcare"],
) as dag:

    load_raw_claims = CopyFromExternalStageToSnowflakeOperator(
        task_id="load_raw_claims",
        snowflake_conn_id="snowflake_default",
        table="RAW_CLAIMS",
        schema="RAW",
        stage="RAW_CLAIMS_STAGE",
        prefix="claims/{{ ds_nodash }}/",
        file_format="(TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"')",
        copy_options="ON_ERROR = 'ABORT_STATEMENT'",
    )

    dbt_deps = BashOperator(task_id="dbt_deps", bash_command=f"{DBT} deps")

    # Fails the run if a source is stale beyond error_after (defined in the sources yml).
    source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT} source freshness --target {DBT_TARGET}",
    )

    # Models + snapshots + tests together. Unit tests are run first by dbt.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build --target {DBT_TARGET} --exclude resource_type:seed",
    )

    # Runs even when dbt_build fails, so failures always produce a report.
    test_summary = BashOperator(
        task_id="publish_test_summary",
        bash_command=f"cd {DBT_DIR} && python scripts/summarise_run_results.py target/run_results.json",
        trigger_rule="all_done",
    )

    load_raw_claims >> dbt_deps >> source_freshness >> dbt_build >> test_summary
