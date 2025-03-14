from datetime import datetime, timedelta
from airflow.decorators import dag, task

@dag(
    dag_id="dag_with_taskflow_api",
    default_args={"owner": "airflow", "retries": 3, "retry_delay": timedelta(minutes=5)},
    start_date=datetime(2025, 3, 14),
    schedule_interval="@daily",
    catchup=False
)
def my_dag():
    # ✅ Task 1: Returns data (Auto-pushed to XCom)
    @task()
    def push_data():
        return "Hello from Task 1! 🎉"  # ✅ Auto-stored in XCom

    # ✅ Task 2: Automatically receives the output of push_data()
    @task()
    def pull_data(message):
        print(f"Received Message: {message}")  # ✅ No need to manually pull from XCom

    # Define Task Dependencies
    msg = push_data()  # Task 1
    pull_data(msg)  # Task 2 (Receives msg from Task 1)

# ✅ Instantiate the DAG
dag_instance = my_dag()