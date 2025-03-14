from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner":"airflow",
    "retries":5,
    "retry_delay":timedelta(minutes=5),
}

with DAG(
    dag_id="task_dag_v4",
    default_args=default_args,
    description="DAG with dependent tasks",
    start_date=datetime(2025, 3, 13),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    task1 = BashOperator(
        task_id="start_task",
        bash_command="echo 'Step 1: Start the process'"
    )

    task2 = BashOperator(
        task_id="middle_task",
        bash_command="echo 'Step 2: Processing data'"
    )

    task3 = BashOperator(
        task_id="end_task",
        bash_command="echo 'Step 3: DAG completed'"
    )

    # ✅ Define dependencies
    task1 >> [task2, task3]

    # ✅ Setting downstream dependencies
    # task1.set_downstream(task2)