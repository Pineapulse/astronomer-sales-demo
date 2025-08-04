from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime


with DAG(
    'test_snowflake_conn',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test"],
) as dag:

    test_sf = SnowflakeOperator(
        task_id="test_conn",
        snowflake_conn_id="snowflake_default",
        sql="SELECT CURRENT_USER(), CURRENT_DATE();",
    )
