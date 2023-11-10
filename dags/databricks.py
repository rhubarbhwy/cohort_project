from datetime import datetime
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

# Define default arguments and DAG configuration
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'trigger_databricks_notebook',
    default_args=default_args,
    description='DAG to trigger a Databricks notebook',
    schedule_interval='@daily',  # Set your desired schedule interval
    catchup=False,
    dagrun_timeout=None,
    tags=['databricks'],
)

# Define the Databricks task
databricks_task = DatabricksSubmitRunOperator(
    task_id='run_databricks_notebook',
    databricks_conn_id='databricks_test',  # Connection ID configured in Airflow
    json={
        'run_name': 'My Databricks Run',  # Specify your run name
        'notebook_task': {
            'notebook_path': '/Users/anamicatiscooking@gmail.com/test',  # Specify the Databricks notebook path
        },
        'new_cluster': {
            'spark_version': '13.3.x-scala2.12',  # Specify your Spark version
            'node_type_id': 'i3.xlarge',  # Specify your VM type
            'num_workers': 2,  # Specify the number of worker nodes
        },
    },
    dag=dag,
)

# Set task dependencies
databricks_task  # This task is executed daily based on the specified schedule_interval
