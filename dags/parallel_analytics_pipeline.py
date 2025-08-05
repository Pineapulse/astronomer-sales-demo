from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
import pandas as pd

# --- CONFIG ---
CSV_PATH = '/usr/local/airflow/include/sales_data.csv'
SUMMARY_PATH = '/usr/local/airflow/include/sales_summary.csv'
SNOWFLAKE_TABLE = 'SALES_SUMMARY'
SNOWFLAKE_CONN_ID = 'snowflake_default'

default_args = {
    'owner': 'analytics-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['team@company.com'],
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    'parallel_sales_analytics_to_snowflake',
    default_args=default_args,
    description='Parallel analytics on sales CSV, then load summary to Snowflake',
    schedule='@daily',
    catchup=False,
    tags=['demo', 'parallel', 'analytics']
)

start = EmptyOperator(task_id='start', dag=dag)

#Parallel steps

@task(dag=dag)
def read_sales_data():
    df = pd.read_csv(CSV_PATH)
    return df.to_dict(orient='records')

@task(dag=dag)
def product_sales_summary(sales_data):
    df = pd.DataFrame(sales_data)
    summary = df.groupby('product_id').agg(
        total_sales=('quantity', 'sum'),
        total_revenue=('price', lambda x: (df.loc[x.index, 'quantity'] * x).sum())
    ).reset_index()
    summary['summary_type'] = 'product'
    return summary.to_dict(orient='records')

@task(dag=dag)
def revenue_by_day(sales_data):
    df = pd.DataFrame(sales_data)
    summary = df.groupby('transaction_date').agg(
        daily_revenue=('price', lambda x: (df.loc[x.index, 'quantity'] * x).sum()),
        transactions=('transaction_id', 'count')
    ).reset_index()
    summary['summary_type'] = 'daily'
    return summary.to_dict(orient='records')

@task(dag=dag)
def channel_sales_summary(sales_data):
    df = pd.DataFrame(sales_data)
    summary = df.groupby('channel').agg(
        total_sales=('quantity', 'sum'),
        total_revenue=('price', lambda x: (df.loc[x.index, 'quantity'] * x).sum())
    ).reset_index()
    summary['summary_type'] = 'channel'
    return summary.to_dict(orient='records')

@task(dag=dag)
def inventory_snapshot(sales_data):
    df = pd.DataFrame(sales_data)
    summary = df.groupby('product_id').agg(
        last_inventory=('inventory', 'last')
    ).reset_index()
    summary['summary_type'] = 'inventory'
    return summary.to_dict(orient='records')

@task(dag=dag)
def consolidate_and_save_csv(product_summary, daily_summary, channel_summary, inventory_summary):
    all_rows = []
    for section in [product_summary, daily_summary, channel_summary, inventory_summary]:
        all_rows.extend(section)
    df = pd.DataFrame(all_rows)
    df.to_csv(SUMMARY_PATH, index=False)
    return SUMMARY_PATH

@task(dag=dag)
def load_to_snowflake(summary_csv_path):
    import pandas as pd
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    df = pd.read_csv(summary_csv_path)

    columns = df.columns.tolist()
    table = SNOWFLAKE_TABLE

    rows = df.to_dict(orient='records')
    if not rows:
        return 'No data to load.'

    batch_size = 50
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i+batch_size]
        values_sql = []
        for row in batch:
            vals = []
            for c in columns:
                val = row[c]
                if pd.isnull(val):
                    vals.append("NULL")
                else:
                    safe_val = str(val).replace("'", "''")
                    vals.append(f"'{safe_val}'")
            values_sql.append(f"({', '.join(vals)})")
        values_sql_str = ',\n'.join(values_sql)
        insert_sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES {values_sql_str};"
        hook.run(insert_sql)
    return f"Loaded {len(rows)} rows into {table}"

end = EmptyOperator(task_id='end', dag=dag)

sales_data = read_sales_data()
prod_summary = product_sales_summary(sales_data)
day_summary = revenue_by_day(sales_data)
chan_summary = channel_sales_summary(sales_data)
inv_summary = inventory_snapshot(sales_data)

summary_path = consolidate_and_save_csv(prod_summary, day_summary, chan_summary, inv_summary)
load = load_to_snowflake(summary_path)

start >> sales_data
sales_data >> [prod_summary, day_summary, chan_summary, inv_summary] >> summary_path >> load >> end
