from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.operators.python import PythonOperator


# --------------------
# CONFIG
# --------------------
DATABASE = "portfolio_db"
RAW_TABLE = "yellow_yellow"
CURATED_TABLE = "yellow_taxi_curated"

DATA_BUCKET = "nyc-tlc-athena-airflow"
ATHENA_RESULTS = f"s3://{DATA_BUCKET}/athena-results/"
WORKGROUP = "primary"

YEAR = "2010"
MONTH = "01"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


# --------------------
# DATA QUALITY CHECK
# --------------------
def dq_check(**context):
    # MWAA provider version doesn't support workgroup= in constructor
    hook = AthenaHook(aws_conn_id="aws_default")

    query = f"""
    SELECT
        COUNT(*) AS row_count,
        SUM(CASE WHEN pickup_ts IS NULL THEN 1 ELSE 0 END) AS null_pickups
    FROM {DATABASE}.{CURATED_TABLE}
    WHERE year='{YEAR}' AND month='{MONTH}';
    """

    qid = hook.run_query(
        query=query,
        query_context={"Database": DATABASE},
        result_configuration={"OutputLocation": ATHENA_RESULTS},
        workgroup=WORKGROUP,  # pass workgroup here (safe)
    )

    hook.poll_query_status(qid, max_polling_attempts=180)
    res = hook.get_query_results(qid)

    rows = res["ResultSet"]["Rows"]
    row_count = int(rows[1]["Data"][0]["VarCharValue"])
    null_pickups = int(rows[1]["Data"][1]["VarCharValue"])

    print(f"✅ Row count = {row_count}")
    print(f"✅ Null pickup_ts rows = {null_pickups}")

    if row_count == 0:
        raise ValueError("DQ FAILED: curated table has 0 rows")

    if null_pickups > 0:
        raise ValueError("DQ FAILED: pickup_ts contains NULL values")



# --------------------
# DAG
# --------------------
with DAG(
    dag_id="yellow_taxi_athena_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule=None,          # manual trigger (perfect for demos)
    catchup=False,
    default_args=default_args,
    tags=["portfolio", "real-data", "athena", "yellow-taxi"],
) as dag:

    # 1) Register partitions
    repair_partitions = AthenaOperator(
        task_id="repair_partitions",
        query=f"MSCK REPAIR TABLE {DATABASE}.{RAW_TABLE};",
        database=DATABASE,
        output_location=ATHENA_RESULTS,
        workgroup=WORKGROUP,
    )

    # 2) Drop curated table (idempotent reruns)
    drop_curated = AthenaOperator(
        task_id="drop_curated_table",
        query=f"DROP TABLE IF EXISTS {DATABASE}.{CURATED_TABLE};",
        database=DATABASE,
        output_location=ATHENA_RESULTS,
        workgroup=WORKGROUP,
    )

    # 3) CTAS: create curated Parquet
    curate = AthenaOperator(
        task_id="curate_yellow_taxi",
        query=f"""
        CREATE TABLE {DATABASE}.{CURATED_TABLE}
        WITH (
          format='PARQUET',
          external_location='s3://{DATA_BUCKET}/curated/yellow/',
          partitioned_by = ARRAY['year','month']
        ) AS
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
          year,
          month
        FROM {DATABASE}.{RAW_TABLE}
        WHERE year='{YEAR}' AND month='{MONTH}'
          AND trip_distance > 0
          AND total_amount >= 0
          AND pickup_datetime IS NOT NULL
          AND dropoff_datetime IS NOT NULL;
        """,
        database=DATABASE,
        output_location=ATHENA_RESULTS,
        workgroup=WORKGROUP,
    )

    # 4) Data quality check
    dq = PythonOperator(
        task_id="data_quality_check",
        python_callable=dq_check,
    )

    repair_partitions >> drop_curated >> curate >> dq
