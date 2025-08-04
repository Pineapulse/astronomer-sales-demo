from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable

# Default arguments
default_args = {
    'owner': 'ml-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['ml-team@company.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2)
}

# Create the DAG
dag = DAG(
    'ecommerce_recommendation_pipeline',
    default_args=default_args,
    description='Real-time product recommendation pipeline for e-commerce platform',
    schedule_interval='0 */4 * * *',  # Run every 4 hours
    catchup=False,
    max_active_runs=1,
    tags=['production', 'ml', 'recommendations', 'ecommerce']
)

# Task 1: Extract user behavior data from Snowflake
extract_user_behavior = SnowflakeOperator(
    task_id='extract_user_behavior_data',
    sql="""
        SELECT 
            user_id,
            product_id,
            action_type,
            timestamp,
            session_id,
            device_type,
            location
        FROM raw_data.user_events
        WHERE timestamp >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
        AND action_type IN ('view', 'add_to_cart', 'purchase', 'wishlist')
    """,
    snowflake_conn_id='snowflake_default',  # Shared connection
    warehouse='COMPUTE_WH',
    database='ANALYTICS',
    role='DATA_SCIENTIST',
    dag=dag
)

# Task 2: Extract product catalog data
extract_product_catalog = SnowflakeOperator(
    task_id='extract_product_catalog',
    sql="""
        SELECT 
            product_id,
            category_id,
            subcategory_id,
            brand,
            price,
            avg_rating,
            total_reviews,
            in_stock,
            attributes
        FROM raw_data.product_catalog
        WHERE is_active = TRUE
        AND in_stock > 0
    """,
    snowflake_conn_id='snowflake_default',  # Shared connection
    warehouse='COMPUTE_WH',
    database='ANALYTICS',
    role='DATA_SCIENTIST',
    dag=dag
)

# Task 3: Feature engineering in Databricks
feature_engineering = DatabricksRunNowOperator(
    task_id='feature_engineering',
    databricks_conn_id='databricks_default',  # Shared connection
    job_id='{{ var.value.feature_engineering_job_id }}',
    notebook_params={
        "environment": "production",
        "run_date": "{{ ds }}",
        "lookback_hours": "24"
    },
    dag=dag
)

# Task 4: Train/update recommendation model
@task(dag=dag)
def train_recommendation_model(**context):
    """Train or update the recommendation model using latest features"""
    import json
    
    print("Training recommendation model...")
    
    # In real scenario, this would trigger ML training pipeline
    model_params = {
        'algorithm': 'collaborative_filtering',
        'embedding_dim': 128,
        'learning_rate': 0.001,
        'batch_size': 256,
        'epochs': 10,
        'regularization': 0.01
    }
    
    # Simulate model training metrics
    metrics = {
        'auc': 0.872,
        'precision_at_5': 0.412,
        'precision_at_10': 0.387,
        'recall_at_5': 0.523,
        'recall_at_10': 0.651,
        'coverage': 0.782,
        'diversity': 0.893
    }
    
    print(f"Model training completed with metrics: {json.dumps(metrics, indent=2)}")
    
    # Save model version
    model_version = f"v{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    context['task_instance'].xcom_push(key='model_version', value=model_version)
    context['task_instance'].xcom_push(key='model_metrics', value=metrics)
    
    return model_version

# Task 5: Validate model performance
@task(dag=dag)
def validate_model(model_version, **context):
    """Validate model meets performance thresholds"""
    metrics = context['task_instance'].xcom_pull(key='model_metrics')
    
    print(f"Validating model {model_version}...")
    
    # Define performance thresholds
    thresholds = {
        'auc': 0.85,
        'precision_at_10': 0.35,
        'recall_at_10': 0.60,
        'coverage': 0.75
    }
    
    # Check if model meets thresholds
    validation_passed = True
    for metric, threshold in thresholds.items():
        if metrics[metric] < threshold:
            print(f"❌ {metric}: {metrics[metric]} < {threshold}")
            validation_passed = False
        else:
            print(f"✅ {metric}: {metrics[metric]} >= {threshold}")
    
    if not validation_passed:
        raise ValueError("Model validation failed - performance below thresholds")
    
    print(f"Model {model_version} passed validation!")
    return validation_passed

# Task 6: Generate personalized recommendations
@task(dag=dag)
def generate_recommendations(model_version):
    """Generate personalized recommendations for all active users"""
    import pandas as pd
    import numpy as np
    
    print(f"Generating recommendations using model {model_version}...")
    
    # Simulate recommendation generation
    # In reality, this would call your ML inference service
    num_users = 10000
    recommendations_per_user = 20
    
    recommendations_data = {
        'user_id': [],
        'recommendations': [],
        'scores': [],
        'generated_at': []
    }
    
    for i in range(100):  # Sample of users for demo
        user_id = f"user_{i:06d}"
        # Generate mock product recommendations
        product_ids = [f"prod_{j:05d}" for j in np.random.choice(5000, recommendations_per_user, replace=False)]
        scores = np.random.uniform(0.5, 1.0, recommendations_per_user).round(3).tolist()
        
        recommendations_data['user_id'].append(user_id)
        recommendations_data['recommendations'].append(product_ids)
        recommendations_data['scores'].append(scores)
        recommendations_data['generated_at'].append(datetime.now().isoformat())
    
    print(f"Generated recommendations for {len(recommendations_data['user_id'])} users")
    return recommendations_data

# Task 7: Store recommendations in cache
@task(dag=dag)
def store_recommendations_cache(recommendations_data):
    """Store recommendations in Redis cache for real-time serving"""
    print("Storing recommendations in cache...")
    
    # In real scenario, this would connect to Redis/Memcached
    # and store recommendations for fast retrieval
    
    stored_count = len(recommendations_data['user_id'])
    cache_ttl = 14400  # 4 hours TTL
    
    print(f"Stored {stored_count} user recommendations in cache with {cache_ttl}s TTL")
    return {'stored_count': stored_count, 'cache_ttl': cache_ttl}

# Task 8: Update recommendation database
update_recommendation_db = SnowflakeOperator(
    task_id='update_recommendation_database',
    sql="""
        -- Create temporary table with new recommendations
        CREATE OR REPLACE TEMPORARY TABLE temp_recommendations AS
        SELECT * FROM {{ ti.xcom_pull(task_ids='generate_recommendations') }};
        
        -- Update main recommendations table
        MERGE INTO production.recommendations AS target
        USING temp_recommendations AS source
        ON target.user_id = source.user_id
        WHEN MATCHED THEN UPDATE SET
            recommendations = source.recommendations,
            scores = source.scores,
            model_version = '{{ ti.xcom_pull(task_ids='train_recommendation_model') }}',
            updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (
            user_id, recommendations, scores, model_version, created_at, updated_at
        ) VALUES (
            source.user_id, source.recommendations, source.scores,
            '{{ ti.xcom_pull(task_ids='train_recommendation_model') }}',
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
        );
        
        -- Log execution stats
        INSERT INTO production.pipeline_logs (
            pipeline_name, execution_date, records_processed, model_version, status
        ) VALUES (
            'ecommerce_recommendation_pipeline',
            '{{ ds }}',
            (SELECT COUNT(*) FROM temp_recommendations),
            '{{ ti.xcom_pull(task_ids='train_recommendation_model') }}',
            'SUCCESS'
        );
    """,
    snowflake_conn_id='snowflake_default',
    warehouse='COMPUTE_WH',
    database='ANALYTICS',
    role='DATA_SCIENTIST',
    dag=dag
)

# Task 9: Trigger A/B test update
update_ab_test = SimpleHttpOperator(
    task_id='update_ab_test_config',
    method='POST',
    http_conn_id='api_default',  # Shared connection
    endpoint='api/v1/experiments/recommendations/update',
    data="""
    {
        "experiment_id": "rec_engine_v2",
        "model_version": "{{ ti.xcom_pull(task_ids='train_recommendation_model') }}",
        "traffic_percentage": 20,
        "segments": ["power_users", "frequent_buyers"]
    }
    """,
    headers={"Content-Type": "application/json"},
    dag=dag
)

# Task 10: Send success notification
@task(dag=dag)
def send_success_notification(**context):
    """Send notification about successful pipeline execution"""
    model_version = context['task_instance'].xcom_pull(task_ids='train_recommendation_model')
    metrics = context['task_instance'].xcom_pull(key='model_metrics')
    cache_info = context['task_instance'].xcom_pull(task_ids='store_recommendations_cache')
    
    notification = f"""
    ✅ Recommendation Pipeline Completed Successfully
    
    Execution Date: {context['ds']}
    Model Version: {model_version}
    
    Model Performance:
    - AUC: {metrics['auc']}
    - Precision@10: {metrics['precision_at_10']}
    - Recall@10: {metrics['recall_at_10']}
    - Coverage: {metrics['coverage']}
    
    Cache Status:
    - Users Updated: {cache_info['stored_count']}
    - TTL: {cache_info['cache_ttl']}s
    
    A/B Test: Updated with 20% traffic allocation
    """
    
    print(notification)
    
    # In real scenario, send via Slack/Email
    return "Notification sent successfully"

# Task 11: Data quality monitoring
@task(dag=dag)
def monitor_data_quality():
    """Monitor recommendation quality metrics"""
    print("Running data quality checks...")
    
    quality_metrics = {
        'null_recommendation_rate': 0.002,
        'duplicate_recommendation_rate': 0.001,
        'avg_diversity_score': 0.893,
        'catalog_coverage': 0.782,
        'recommendation_freshness': 0.956
    }
    
    # Check for anomalies
    if quality_metrics['null_recommendation_rate'] > 0.01:
        raise ValueError("High null recommendation rate detected!")
    
    print(f"Data quality metrics: {quality_metrics}")
    return quality_metrics

# End node
pipeline_complete = DummyOperator(
    task_id='pipeline_complete',
    dag=dag,
    trigger_rule='all_success'
)

# Define task dependencies
# Parallel extraction
[extract_user_behavior, extract_product_catalog] >> feature_engineering

# Model training and validation
model_version = train_recommendation_model()
validation = validate_model(model_version)

# Feature engineering feeds into model training
feature_engineering >> model_version >> validation

# Generate and store recommendations
recommendations = generate_recommendations(model_version)
cache_status = store_recommendations_cache(recommendations)

# Recommendation generation depends on validation
validation >> recommendations >> [cache_status, update_recommendation_db]

# Quality monitoring runs in parallel with storage
validation >> monitor_data_quality()

# Update A/B test after database update
update_recommendation_db >> update_ab_test

# Send notification after all critical tasks
[cache_status, update_ab_test, monitor_data_quality()] >> send_success_notification() >> pipeline_complete