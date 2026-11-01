import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Mühit dəyişəni tapılmadı: {name}")
    return value


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", required=True, help="YYYY-MM-DD (Airflow ds)")
    parser.add_argument("--bucket", required=True)
    return parser.parse_args()


def main():
    args = parse_args()

    spark = (
        SparkSession.builder.appName("city_sales_job")
        .config("spark.hadoop.fs.s3a.endpoint", require_env("MINIO_ENDPOINT"))
        .config("spark.hadoop.fs.s3a.access.key", require_env("MINIO_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", require_env("MINIO_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    base = f"s3a://{args.bucket}"
    customers = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{base}/raw/customers/customers.csv")
    )
    orders = (
        spark.read.option("header", True).option("inferSchema", True)
        .csv(f"{base}/raw/orders/order_date={args.run_date}/orders.csv")
    )

    report = (
        orders.join(customers, on="customer_id", how="inner")
        .groupBy("city")
        .agg(
            F.count("order_id").cast("int").alias("order_count"),
            F.round(F.sum("amount"), 2).cast("decimal(12,2)").alias("total_amount"),
        )
        .withColumn("report_date", F.lit(args.run_date).cast("date"))
        .select("report_date", "city", "order_count", "total_amount")
    )

    print(f"=== {args.run_date} üçün hesabat ===")
    report.orderBy("city").show(truncate=False)

    (
        report.coalesce(1)
        .write.mode("overwrite")
        .parquet(f"{base}/processed/city_daily_sales/report_date={args.run_date}")
    )
    print(f"Parquet yazıldı: {base}/processed/city_daily_sales/report_date={args.run_date}")
    spark.stop()


if __name__ == "__main__":
    main()
