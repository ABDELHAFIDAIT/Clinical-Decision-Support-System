from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'abdelhafid',
    'start_date': datetime(2025, 12, 25),
}

with DAG(
    '02_test_spark_connection',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['test', 'spark']
) as dag:

    test_spark_job = SparkSubmitOperator(
        task_id='test_spark_pi',
        conn_id='spark_default',
        application='/home/airflow/.local/lib/python3.8/site-packages/pyspark/examples/src/main/python/pi.py',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='512m',
        name='airflow_test_pi',
        verbose=True
    )

    test_spark_job