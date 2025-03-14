from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Default arguments
default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
with DAG(
    dag_id="dag_with_postgres_operator",
    default_args=default_args,
    description="DAG with PostgresOperator using Cron",
    start_date=datetime(2025, 3, 14),
    schedule_interval="0 0 * * *",  # ✅ Runs every day at midnight
    catchup=False,
) as dag:

    # ✅ Task 1: Create a table
    create_table_task = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgress_localhost",
        sql="""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                age INT
            );
        """,
    )

    # ✅ Task 2: Insert data
    insert_data_task = PostgresOperator(
        task_id="insert_data",
        postgres_conn_id="postgress_localhost",
        sql="INSERT INTO users (name, age) VALUES ('John Doe', 30);",
    )

    # ✅ Task 3: Fetch data
    fetch_data_task = PostgresOperator(
        task_id="fetch_data",
        postgres_conn_id="postgress_localhost",
        sql="SELECT * FROM users;",
    )

    # Define Task Dependencies
    create_table_task >> insert_data_task >> fetch_data_task
