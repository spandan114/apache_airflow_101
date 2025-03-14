from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner":"airflow",
    "retries":5,
    "retry_delay":timedelta(minutes=5),
}

def greet(name, age):
    print(f"Hello, {name}! You are {age} years old. 🎉")

# Task 1: This function's return value will be auto-stored in XCom
def push_data():
    return "Hello from Task 1! 🎉"  # ✅ Auto-pushed to XCom

# Task 2: This function pulls the value from XCom
# ti stands for Task Instance, and it is an object that represents the execution of a specific task in a DAG run.
def pull_data(**kwargs):
    ti = kwargs["ti"]
    received_message = ti.xcom_pull(task_ids="push_data_task")  # Auto-pulled from XCom
    print(f"Received Message: {received_message}")  # Prints XCom data

with DAG(
    dag_id="dag_with_operator_v2",
    default_args=default_args,
    description="DAG with python operator",
    start_date=datetime(2025, 3, 14),
    schedule_interval="@daily",
    # catchup=False,
) as dag:
    # Define the PythonOperator
    # task1 = PythonOperator(
    #     task_id="run_python_function",
    #     python_callable=greet, 
    #     op_kwargs={"name": "Spandan", "age": 25}, # Pass function arguments
    # )

    task1 = PythonOperator(
        task_id="push_data_task",
        python_callable=push_data,  # Auto-pushes return value to XCom
    )

    task2 = PythonOperator(
        task_id="pull_data_task",
        python_callable=pull_data,
        provide_context=True,  # Allows kwargs access
    )

    task1 >> task2  # Define task dependencies