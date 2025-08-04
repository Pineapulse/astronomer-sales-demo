from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain, cross_downstream
import random

# Default arguments
default_args = {
    'owner': 'chaos-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

# Create the DAG
dag = DAG(
    'crazy_complex_workflow',
    default_args=default_args,
    description='The most visually complex DAG with maximum dependencies and chaos!',
    schedule_interval=None,
    catchup=False,
    tags=['demo', 'complex', 'chaos', 'visual']
)

# Start nodes
start = DummyOperator(task_id='start', dag=dag)
init_phase_1 = DummyOperator(task_id='initialize_phase_1', dag=dag)
init_phase_2 = DummyOperator(task_id='initialize_phase_2', dag=dag)

# Branch decision function
def choose_path(**context):
    """Randomly choose execution path"""
    choice = random.choice(['path_alpha', 'path_beta', 'path_gamma'])
    print(f"Choosing path: {choice}")
    return choice

# Create branching operator
branching_decision = BranchPythonOperator(
    task_id='branching_decision',
    python_callable=choose_path,
    dag=dag
)

# Path Alpha Tasks
path_alpha = DummyOperator(task_id='path_alpha', dag=dag)
alpha_process_1 = BashOperator(task_id='alpha_process_1', bash_command='echo "Alpha 1"', dag=dag)
alpha_process_2 = BashOperator(task_id='alpha_process_2', bash_command='echo "Alpha 2"', dag=dag)
alpha_process_3 = BashOperator(task_id='alpha_process_3', bash_command='echo "Alpha 3"', dag=dag)

# Path Beta Tasks
path_beta = DummyOperator(task_id='path_beta', dag=dag)
beta_process_1 = BashOperator(task_id='beta_process_1', bash_command='echo "Beta 1"', dag=dag)
beta_process_2 = BashOperator(task_id='beta_process_2', bash_command='echo "Beta 2"', dag=dag)

# Path Gamma Tasks
path_gamma = DummyOperator(task_id='path_gamma', dag=dag)
gamma_process_1 = BashOperator(task_id='gamma_process_1', bash_command='echo "Gamma 1"', dag=dag)
gamma_process_2 = BashOperator(task_id='gamma_process_2', bash_command='echo "Gamma 2"', dag=dag)
gamma_process_3 = BashOperator(task_id='gamma_process_3', bash_command='echo "Gamma 3"', dag=dag)
gamma_process_4 = BashOperator(task_id='gamma_process_4', bash_command='echo "Gamma 4"', dag=dag)

# Convergence point (trigger rule to handle branching)
convergence_point = DummyOperator(
    task_id='convergence_point', 
    dag=dag,
    trigger_rule='none_failed_or_skipped'
)

# Task Group 1: Data Processing Cluster
with TaskGroup("data_processing_cluster", dag=dag) as processing_group:
    cluster_start = DummyOperator(task_id='cluster_start', dag=dag)
    
    # Create interconnected processing nodes
    processors = []
    for i in range(5):
        processor = BashOperator(
            task_id=f'processor_{i}',
            bash_command=f'echo "Processing node {i}"',
            dag=dag
        )
        processors.append(processor)
    
    cluster_merge = DummyOperator(task_id='cluster_merge', dag=dag)
    
    # Complex dependencies within cluster
    cluster_start >> processors[0]
    cluster_start >> processors[1]
    processors[0] >> processors[2]
    processors[1] >> processors[2]
    processors[1] >> processors[3]
    processors[2] >> processors[4]
    processors[3] >> processors[4]
    processors[4] >> cluster_merge

# Task Group 2: Parallel Analytics Web
with TaskGroup("analytics_web", dag=dag) as analytics_group:
    web_start = DummyOperator(task_id='web_start', dag=dag)
    
    # Create web of analytics tasks
    analytics = []
    for i in range(4):
        analytic = BashOperator(
            task_id=f'analytics_{i}',
            bash_command=f'echo "Analytics {i}"',
            dag=dag
        )
        analytics.append(analytic)
    
    web_end = DummyOperator(task_id='web_end', dag=dag)
    
    # Create complex web pattern
    web_start >> analytics[0]
    web_start >> analytics[1]
    analytics[0] >> analytics[2]
    analytics[1] >> analytics[2]
    analytics[0] >> analytics[3]
    analytics[2] >> web_end
    analytics[3] >> web_end

# Additional chaos nodes
chaos_node_1 = BashOperator(task_id='chaos_node_1', bash_command='echo "Chaos 1"', dag=dag)
chaos_node_2 = BashOperator(task_id='chaos_node_2', bash_command='echo "Chaos 2"', dag=dag)
chaos_node_3 = BashOperator(task_id='chaos_node_3', bash_command='echo "Chaos 3"', dag=dag)
chaos_node_4 = BashOperator(task_id='chaos_node_4', bash_command='echo "Chaos 4"', dag=dag)
chaos_node_5 = BashOperator(task_id='chaos_node_5', bash_command='echo "Chaos 5"', dag=dag)

# Fan-out section
fan_out_start = DummyOperator(task_id='fan_out_start', dag=dag)
fan_tasks = []
for i in range(6):
    fan_task = BashOperator(
        task_id=f'fan_task_{i}',
        bash_command=f'echo "Fan task {i}"',
        dag=dag
    )
    fan_tasks.append(fan_task)

# Diamond pattern nodes
diamond_top = DummyOperator(task_id='diamond_top', dag=dag)
diamond_left = BashOperator(task_id='diamond_left', bash_command='echo "Diamond left"', dag=dag)
diamond_right = BashOperator(task_id='diamond_right', bash_command='echo "Diamond right"', dag=dag)
diamond_bottom = DummyOperator(task_id='diamond_bottom', dag=dag)

# Cross-dependency section
cross_1 = BashOperator(task_id='cross_1', bash_command='echo "Cross 1"', dag=dag)
cross_2 = BashOperator(task_id='cross_2', bash_command='echo "Cross 2"', dag=dag)
cross_3 = BashOperator(task_id='cross_3', bash_command='echo "Cross 3"', dag=dag)
cross_4 = BashOperator(task_id='cross_4', bash_command='echo "Cross 4"', dag=dag)

# Final convergence
pre_final_1 = DummyOperator(task_id='pre_final_1', dag=dag, trigger_rule='none_failed_or_skipped')
pre_final_2 = DummyOperator(task_id='pre_final_2', dag=dag, trigger_rule='none_failed_or_skipped')
final_merge = DummyOperator(task_id='final_merge', dag=dag, trigger_rule='none_failed_or_skipped')
end = DummyOperator(task_id='end', dag=dag, trigger_rule='none_failed_or_skipped')

# Define the chaos of dependencies!

# Initial flow
start >> [init_phase_1, init_phase_2]
init_phase_1 >> branching_decision
init_phase_2 >> chaos_node_1

# Branching paths
branching_decision >> [path_alpha, path_beta, path_gamma]

# Alpha path
path_alpha >> alpha_process_1 >> alpha_process_2
alpha_process_1 >> alpha_process_3
[alpha_process_2, alpha_process_3] >> convergence_point

# Beta path
path_beta >> [beta_process_1, beta_process_2] >> convergence_point

# Gamma path
path_gamma >> gamma_process_1 >> [gamma_process_2, gamma_process_3]
gamma_process_2 >> gamma_process_4
gamma_process_3 >> gamma_process_4
gamma_process_4 >> convergence_point

# Chaos connections
chaos_node_1 >> [chaos_node_2, chaos_node_3]
chaos_node_2 >> chaos_node_4
chaos_node_3 >> [chaos_node_4, chaos_node_5]

# Connect to task groups
convergence_point >> processing_group
chaos_node_4 >> analytics_group
chaos_node_5 >> analytics_group

# Fan out pattern
processing_group >> fan_out_start
fan_out_start >> fan_tasks

# Diamond pattern
analytics_group >> diamond_top
diamond_top >> [diamond_left, diamond_right]
[diamond_left, diamond_right] >> diamond_bottom

# Cross dependencies
fan_tasks[0] >> cross_1
fan_tasks[1] >> cross_2
fan_tasks[2] >> [cross_1, cross_3]
fan_tasks[3] >> [cross_2, cross_4]
fan_tasks[4] >> cross_3
fan_tasks[5] >> cross_4

cross_1 >> pre_final_1
cross_2 >> pre_final_1
cross_3 >> pre_final_2
cross_4 >> pre_final_2

# Final convergence
diamond_bottom >> final_merge
[pre_final_1, pre_final_2] >> final_merge
final_merge >> end

# Add some extra cross-connections for maximum visual complexity
chaos_node_2 >> fan_tasks[2]
alpha_process_2 >> chaos_node_3
beta_process_1 >> diamond_left
gamma_process_2 >> cross_2