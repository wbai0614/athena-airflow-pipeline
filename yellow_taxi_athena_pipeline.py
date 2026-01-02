from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.athena import AthenaOperator


# --------------------
# CONFIG
# --------------------
DATABASE = "portfolio_db"
RAW_TABLE = "yellow_yellow"
CURATED_TABLE = "yellow_taxi_curated"

DATA_BUCKET = "nyc-tlc-athena-airflow"
CURATED_PREFIX = "curated/yellow/"         # partition folders live under here
ATHENA_RESULTS = f"s3://{DATA_BUCKET}/athena-results/"
WORKGROUP = "primary"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


# --------------------
# HELPERS
# --------------------
def get_year_month(context):
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    year = conf.get("year", "2010")
    month = conf.get("month", "02")  # default month if not provided
    month = str(month).zfill(2)
    return str(year), month


def cleanup_partition_s3(**context):
    """
    Idempotency: delete existing curated S3 objects for the month partition.
    """
    year, month = get_year_month(context)
    prefix = f"{CURATED_PREFIX}year={year}/month={month}/"

    s3 = S3Hook(aws_conn_id="aws_default")
    keys = s3.list_keys(bucket_name=DATA_BUCKET, prefix=prefix)

    if not keys:
        print(f"✅ No existing objects to delete for s3://{DATA_BUCKET}/{prefix}")
        return

    print(f"🧹 Deleting {len(keys)} objects under s3://{DATA_BUCKET}/{prefix}")
    s3.delete_objects(bucket=DATA_BUCKET, keys=keys)
    print("✅ S3 partition cleanup done")


def dq_check(**context):
    """
    Run Athena query to validate curated data for the month partition.
    Compatible with older AthenaHook versions (no get_state()).
    """
    year, month = get_year_month(context)
    hook = AthenaHook(aws_conn_id="aws_default")

    query = f"""
    SELECT
        COUNT(*) AS row_count,
        SUM(CASE WHEN pickup_ts IS NULL THEN 1 ELSE 0 END) AS null_pickups
    FROM {DATABASE}.{CURATED_TABLE}
    WHERE year='{year}' AND month='{month}';
    """

    qid = hook.run_query(
        query=query,
        query_context={"Database": DATABASE},
        result_configuration={"OutputLocation": ATHENA_RESULTS},
    )

    # This will raise AirflowException if the query ends in FAILED/CANCELLED
    hook.poll_query_status(qid, max_polling_attempts=180)

    # If we got here, the query succeeded; fetch results
    res = hook.get_query_results(qid)
    if not res or "ResultSet" not in res:
        raise AirflowException(f"Athena returned no results for qid={qid}")

    rows = res["ResultSet"]["Rows"]
    if len(rows) < 2:
        raise AirflowException(f"Unexpected Athena result shape for qid={qid}: {rows}")

    row_count = int(rows[1]["Data"][0]["VarCharValue"])
    null_pickups = int(rows[1]["Data"][1]["VarCharValue"])

    print(f"✅ Year-Month = {year}-{month}")
    print(f"✅ Row count = {row_count}")
    print(f"✅ Null pickup_ts rows = {null_pickups}")

    if row_count == 0:
        raise AirflowException("DQ FAILED: curated partition has 0 rows")
    if null_pickups > 0:
        raise AirflowException("DQ FAILED: pickup_ts contains NULL values")


# --------------------
# DAG
# --------------------
with DAG(
    dag_id="yellow_taxi_athena_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # manual trigger (portfolio-friendly)
    catchup=False,
    default_args=default_args,
    tags=["portfolio", "real-data", "athena", "yellow-taxi", "idempotent"],
) as dag:

    # 1) Discover new raw partitions (e.g., month=02)
    repair_raw_partitions = AthenaOperator(
        task_id="repair_raw_partitions",
        query=f"MSCK REPAIR TABLE {DATABASE}.{RAW_TABLE};",
        database=DATABASE,
        output_location=ATHENA_RESULTS,
        workgroup=WORKGROUP,
    )

    # 2) Create curated EXTERNAL table once (schema-only, no data)
    create_curated_table = AthenaOperator(
        task_id="create_curated_table_if_missing",
        query=f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{CURATED_TABLE} (
          pickup_ts timestamp,
          dropoff_ts timestamp,
          passenger_count integer,
          trip_distance double,
          pickup_longitude double,
          pickup_latitude double,
          dropoff_longitude double,
          dropoff_latitude double,
          payment_type string,
          fare_amount double,
          tip_amount double,
          total_amount double
        )
        PARTITIONED BY (year string, month string)
        STORED AS PARQUET
        LOCATION 's3://{DATA_BUCKET}/{CURATED_PREFIX}';
        """,
        database=DATABASE,
        output_location=ATHENA_RESULTS,
        workgroup=WORKGROUP,
    )

    # 3) Drop curated partition metadata (safe if missing)
    drop_curated_partition = AthenaOperator(
        task_id="drop_curated_partition",
        query="""
        ALTER TABLE portfolio_db.yellow_taxi_curated
        DROP IF EXISTS PARTITION (
          year='{{ dag_run.conf.get("year", "2010") }}',
          month='{{ "%02d"|format(dag_run.conf.get("month", 2)|int) if dag_run.conf.get("month") is not none else "02" }}'
        );
        """,
        database=DATABASE,
        output_location=ATHENA_RESULTS,
        workgroup=WORKGROUP,
    )

    # 4) Delete S3 objects for that partition (true overwrite behavior)
    cleanup_s3_partition = PythonOperator(
        task_id="cleanup_s3_partition",
        python_callable=cleanup_partition_s3,
    )

    # 5) Insert that month (recreates partition data)
    insert_month = AthenaOperator(
        task_id="insert_month_partition",
        query=f"""
        INSERT INTO {DATABASE}.{CURATED_TABLE}
        SELECT
          try(date_parse(pickup_datetime, '%Y-%m-%d %H:%i:%s'))  AS pickup_ts,
          try(date_parse(dropoff_datetime, '%Y-%m-%d %H:%i:%s')) AS dropoff_ts,
          CAST(passenger_count AS integer) AS passenger_count,
          trip_distance,
          pickup_longitude,
          pickup_latitude,
          dropoff_longitude,
          dropoff_latitude,
          payment_type,
          fare_amount,
          tip_amount,
          total_amount,
          '{{{{ dag_run.conf.get("year", "2010") }}}}' AS year,
          '{{{{ "%02d"|format(dag_run.conf.get("month", 2)|int) if dag_run.conf.get("month") is not none else "02" }}}}' AS month
        FROM {DATABASE}.{RAW_TABLE}
        WHERE year='{{{{ dag_run.conf.get("year", "2010") }}}}'
          AND month='{{{{ "%02d"|format(dag_run.conf.get("month", 2)|int) if dag_run.conf.get("month") is not none else "02" }}}}'
          AND trip_distance > 0
          AND total_amount >= 0
          AND pickup_datetime IS NOT NULL
          AND dropoff_datetime IS NOT NULL;
        """,
        database=DATABASE,
        output_location=ATHENA_RESULTS,
        workgroup=WORKGROUP,
    )

    # 6) Refresh curated partitions (so partition filters work reliably)
    repair_curated_partitions = AthenaOperator(
        task_id="repair_curated_partitions",
        query=f"MSCK REPAIR TABLE {DATABASE}.{CURATED_TABLE};",
        database=DATABASE,
        output_location=ATHENA_RESULTS,
        workgroup=WORKGROUP,
    )

    # 7) DQ check for that month
    dq = PythonOperator(
        task_id="data_quality_check",
        python_callable=dq_check,
    )

    (
        repair_raw_partitions
        >> create_curated_table
        >> drop_curated_partition
        >> cleanup_s3_partition
        >> insert_month
        >> repair_curated_partitions
        >> dq
    )
