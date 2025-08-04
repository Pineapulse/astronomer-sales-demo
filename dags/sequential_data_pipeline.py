from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import logging

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'astro_sequential_data_pipeline',
    default_args=default_args,
    description='A sequential ETL demo with real API using Astro',
    schedule_interval='@daily',
    catchup=False,
    tags=['demo', 'sequential', 'etl']
)

# 1. Check if API is available
check_api_availability = HttpSensor(
    task_id='check_api_availability',
    http_conn_id='api_default',
    endpoint='posts/1',  # checks a real endpoint
    poke_interval=30,
    timeout=300,
    dag=dag
)

# 2. Extract data from API
@task(dag=dag)
def extract_data():
    import requests
    import pandas as pd
    log = logging.getLogger(__name__)
    log.info("Extracting data from API...")

    url = 'https://jsonplaceholder.typicode.com/posts'
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    df = pd.DataFrame(data)
    df = df[['userId', 'id', 'title']]
    df['transaction_amount'] = df['userId'] * 100 + 50
    df['transaction_date'] = pd.date_range(start='2024-01-01', periods=len(df), freq='D')
    df['transaction_date'] = df['transaction_date'].dt.strftime('%Y-%m-%d')
    log.info(f"Extracted {len(df)} rows from API")
    return df.head(5).to_dict(orient='list')

# 3. Transform
@task(dag=dag)
def transform_data(raw_data):
    import pandas as pd
    log = logging.getLogger(__name__)
    log.info("Transforming data...")

    df = pd.DataFrame(raw_data)
    df['transaction_date'] = pd.to_datetime(df['transaction_date'])
    df['day_of_week'] = df['transaction_date'].dt.day_name()
    df['is_high_value'] = df['transaction_amount'] > 200

    summary = {
        'total_transactions': len(df),
        'total_amount': float(df['transaction_amount'].sum()),
        'avg_transaction': float(df['transaction_amount'].mean()),
        'high_value_count': int(df['is_high_value'].sum())
    }
    log.info(f"Summary: {summary}")
    return summary

# 4. Validate
@task(dag=dag)
def validate_data(transformed_data):
    log = logging.getLogger(__name__)
    log.info("Validating data quality...")
    if transformed_data['total_transactions'] == 0:
        raise ValueError("No transactions found in data")
    if transformed_data['avg_transaction'] < 0:
        raise ValueError("Negative transaction average!")
    log.info("Data validated successfully.")
    return True

# 5. Save summary locally
@task(dag=dag)
def save_summary_locally(transformed_data):
    import pandas as pd
    log = logging.getLogger(__name__)
    df = pd.DataFrame([transformed_data])
    local_path = '/tmp/transformed_summary.csv'
    df.to_csv(local_path, index=False)
    log.info(f"Saved summary CSV to {local_path}")
    log.info(df)
    # Return both data and path for downstream tasks
    return {"csv_path": local_path, "data": transformed_data}

# 6. Load to Snowflake
load_to_snowflake = SnowflakeOperator(
    task_id='load_to_snowflake',
    snowflake_conn_id='snowflake_default',
    sql="""
        INSERT INTO PUBLIC.TRANS_SUMMARY
            (total_transactions, total_amount, avg_transaction, high_value_count)
        VALUES (
            {{ ti.xcom_pull(task_ids='save_summary_locally')['data']['total_transactions'] }},
            {{ ti.xcom_pull(task_ids='save_summary_locally')['data']['total_amount'] }},
            {{ ti.xcom_pull(task_ids='save_summary_locally')['data']['avg_transaction'] }},
            {{ ti.xcom_pull(task_ids='save_summary_locally')['data']['high_value_count'] }}
        )
    """,
    autocommit=True,
    dag=dag
)

# 7. Notify
@task(dag=dag)
def send_notification(load_status):
    log = logging.getLogger(__name__)
    log.info(f"Pipeline Status: {load_status}")
    notification_msg = f"""
    Daily Data Pipeline Completed Successfully
    Status: {load_status}
    Timestamp: {datetime.now()}
    """
    log.info(notification_msg)
    return "Notification sent"

# Task dependencies (serial execution)
extracted_data = extract_data()
transformed_data = transform_data(extracted_data)
validation_status = validate_data(transformed_data)
saved_summary = save_summary_locally(transformed_data)
check_api_availability >> extracted_data >> transformed_data >> validation_status >> saved_summary
saved_summary >> load_to_snowflake
notify = send_notification(load_to_snowflake)
load_to_snowflake >> notify