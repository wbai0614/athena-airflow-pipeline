from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def hello(**context):
    """
    Task 1:
    - Prints a message
    - Returns a dict that will be pushed to XCom automatically
    """
    msg = "Hello from MWAA + S3!"
    run_date = context["ds"]  # rendered execution date (YYYY-MM-DD)
    print(msg)
    return {"message": msg, "run_date": run_date}


def consume_xcom(**context):
    """
    Task 2:
    - Pulls the XCom returned by hello_task
    - Prints it (proves inter-task data passing)
    """
    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="hello_task")
    print(f"Pulled XCom from hello_task: {payload}")


with DAG(
    dag_id="mwaa_hello",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["portfolio", "mwaa"],
) as dag:

    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello,
    )

    consume_task = PythonOperator(
        task_id="consume_xcom_task",
        python_callable=consume_xcom,
    )

    hello_task >> consume_task
