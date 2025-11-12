"""Simple test DAG to verify UI visibility"""
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    "test_simple_dag",
    start_date=datetime(2025, 11, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:
    task1 = BashOperator(
        task_id="print_date",
        bash_command="date",
    )

