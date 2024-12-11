from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
#from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from datetime import datetime, timedelta
import pandas as pd

def extract_claims_data(**kwargs):
    # Example extraction logic
    df = pd.read_csv('/opt/airflow/data/outpatient.csv')
    df.to_parquet('/opt/airflow/data/staged_claims.parquet', index=False)
    return {'total_claims': len(df)}

def validate_claims_data(**kwargs):
    df = pd.read_parquet('/opt/airflow/data/staged_claims.parquet')
    if df.isnull().sum().sum() > 0:
        raise ValueError("Missing values detected")

with DAG(
    'healthcare_claims_etl',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily'
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_claims_data',
        python_callable=extract_claims_data
    )
    
    validate_task = PythonOperator(
        task_id='validate_claims_data',
        python_callable=validate_claims_data
    )
    
    # dbt Run Models Task
    dbt_run_models = BashOperator(
        task_id='dbt_run_models',
        bash_command='cd /opt/dbt && dbt run --models staging mart'
    )
    
    # Optional: Snowflake Load Task
    snowflake_load = CopyFromExternalStageToSnowflakeOperator(
        task_id='load_to_snowflake',
        s3_bucket='your-s3-bucket',
        s3_key='claims_data/',
        table='HEALTHCARE_CLAIMS',
        stage='your_stage_name',
        schema='YOUR_SCHEMA'
    )
    
    # Define task dependencies
    extract_task >> validate_task >> dbt_run_models >> snowflake_load