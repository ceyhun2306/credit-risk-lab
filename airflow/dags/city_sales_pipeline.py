import os
import tempfile
from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, task

SHOP_PG_CONN_ID = "shop_pg_conn"   
MINIO_CONN_ID = "minio_conn"       
SPARK_CONN_ID = "spark_default"
BUCKET = "shop-bucket"
REPORT_TABLE = "city_daily_sales"

CREATE_REPORT_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {REPORT_TABLE} (
    report_date  DATE          NOT NULL,
    city         VARCHAR(30)   NOT NULL,
    order_count  INTEGER       NOT NULL,
    total_amount NUMERIC(12,2) NOT NULL,
    PRIMARY KEY (report_date, city)
);
"""


def _extract_to_minio(select_sql: str, key: str) -> int:
    """Postgres sorğusunun nəticəsini CSV kimi MinIO-ya yazır, sətir sayını qaytarır."""
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        path = tmp.name
    try:
        PostgresHook(postgres_conn_id=SHOP_PG_CONN_ID).copy_expert(
            f"COPY ({select_sql}) TO STDOUT WITH CSV HEADER", path
        )
        with open(path, encoding="utf-8") as f:
            row_count = max(sum(1 for _ in f) - 1, 0)   # -1: başlıq sətri
        if row_count > 0:
            S3Hook(aws_conn_id=MINIO_CONN_ID).load_file(
                filename=path, key=key, bucket_name=BUCKET, replace=True
            )
    finally:
        os.remove(path)
    return row_count


with DAG(
    dag_id="city_sales_pipeline",
    description="Şəhərlər üzrə gündəlik satış hesabatı: Postgres -> MinIO -> Spark -> MinIO -> Postgres",
    start_date=datetime(2026, 10, 20),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,   
    tags=["sales", "spark", "minio", "etl"],
) as dag:

    
    check_connection = SQLExecuteQueryOperator(
        task_id="check_postgres_connection",
        conn_id=SHOP_PG_CONN_ID,
        sql="SELECT 1",
    )

    create_report_table = SQLExecuteQueryOperator(
        task_id="create_report_table",
        conn_id=SHOP_PG_CONN_ID,
        sql=CREATE_REPORT_TABLE_SQL,
    )

  
    @task
    def extract_customers():
        count = _extract_to_minio(
            "SELECT customer_id, name, city FROM customers ORDER BY customer_id",
            "raw/customers/customers.csv",   # hər dəfə üzərinə yazılır
        )
        print(f"customers (full load): {count} sətir")
        return count

    @task
    def extract_orders(ds=None):   
        count = _extract_to_minio(
            f"SELECT order_id, customer_id, amount, order_date FROM orders "
            f"WHERE order_date = DATE '{ds}' ORDER BY order_id",
            f"raw/orders/order_date={ds}/orders.csv",   # tarixə görə ayrılmış qovluq
        )
        print(f"orders (incremental, {ds}): {count} sətir")
        return count   

    
    @task.short_circuit
    def has_orders(orders_count: int):
        print(f"Sifariş sayı: {orders_count}")
        return orders_count > 0

    
    run_spark = SparkSubmitOperator(
        task_id="run_spark",
        conn_id=SPARK_CONN_ID,
        application="/opt/airflow/spark_jobs/city_sales_job.py",
        name="city_sales_job",
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        application_args=["--run-date", "{{ ds }}", "--bucket", BUCKET],
        env_vars={
            "MINIO_ENDPOINT": "{{ conn.minio_conn.extra_dejson.endpoint_url }}",
            "MINIO_ACCESS_KEY": "{{ conn.minio_conn.login }}",
            "MINIO_SECRET_KEY": "{{ conn.minio_conn.password }}",
        },
        verbose=True,
    )

  
    @task
    def load_report(ds=None):
        import pyarrow.parquet as pq

        s3 = S3Hook(aws_conn_id=MINIO_CONN_ID)
        prefix = f"processed/city_daily_sales/report_date={ds}/"
        keys = [k for k in (s3.list_keys(bucket_name=BUCKET, prefix=prefix) or []) if k.endswith(".parquet")]
        if not keys:
            raise ValueError(f"MinIO-da parquet tapılmadı: {prefix}")

        rows = []
        for key in keys:
            local_path = s3.download_file(key=key, bucket_name=BUCKET, local_path="/tmp")
            try:
                for rec in pq.read_table(local_path).to_pylist():
                    rows.append((rec["report_date"], rec["city"], rec["order_count"], rec["total_amount"]))
            finally:
                os.remove(local_path)

        conn = PostgresHook(postgres_conn_id=SHOP_PG_CONN_ID).get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {REPORT_TABLE} WHERE report_date = %s", (ds,))
                cur.executemany(
                    f"INSERT INTO {REPORT_TABLE} (report_date, city, order_count, total_amount) "
                    f"VALUES (%s, %s, %s, %s)",
                    rows,
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
        print(f"{ds}: {len(rows)} sətir {REPORT_TABLE} cədvəlinə yazıldı")

    
    validate_report = SQLCheckOperator(
        task_id="validate_report",
        conn_id=SHOP_PG_CONN_ID,
        sql=f"SELECT COUNT(*) FROM {REPORT_TABLE} WHERE report_date = '{{{{ ds }}}}'",
    )

    
    customers_done = extract_customers()
    orders_count = extract_orders()
    proceed = has_orders(orders_count)
    loaded = load_report()

    check_connection >> create_report_table >> [customers_done, orders_count]
    [customers_done, proceed] >> run_spark >> loaded >> validate_report
