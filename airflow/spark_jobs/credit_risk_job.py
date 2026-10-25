"""Kredit risk Spark job.
 
Postgres (JDBC) + blacklist.csv -> risk_status -> Delta (MinIO, application_date üzrə partition).
Login/parol və bağlantı məlumatı mühit dəyişənlərindən gəlir (kodda yazılmır):
  PG_USER, PG_PASSWORD, PG_HOST, PG_PORT, PG_DB          (DAG, pg_conn_id Connection-dan ötürür)
  MINIO_SERVER_URL, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD (konteynerin .env-dən)
"""
import argparse
import os
 
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
 
BLACKLIST_PATH = "file:///opt/airflow/data/blacklist.csv"
 
 
def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Mühit dəyişəni tapılmadı: {name}")
    return value
 
 
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, help="məs. public.credit_applications")
    parser.add_argument("--s3-path", required=True, help="məs. s3a://credit-bucket/delta/credit_applications")
    return parser.parse_args()
 
 
def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("credit_risk_job")
        # Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # MinIO (S3A)
        .config("spark.hadoop.fs.s3a.endpoint", require_env("MINIO_SERVER_URL"))
        .config("spark.hadoop.fs.s3a.access.key", require_env("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", require_env("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
 
 
def main():
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
 
    # 1) Postgres-dən JDBC ilə oxu
    jdbc_url = f"jdbc:postgresql://{require_env('PG_HOST')}:{require_env('PG_PORT')}/{require_env('PG_DB')}"
    applications = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", args.table)
        .option("user", require_env("PG_USER"))
        .option("password", require_env("PG_PASSWORD"))
        .option("driver", "org.postgresql.Driver")
        .load()
    )
 
    # 2) blacklist.csv-ni oxu
    blacklist = (
        spark.read.option("header", True)
        .csv(BLACKLIST_PATH)
        .select("customer_id")
        .distinct()
        .withColumn("is_blacklisted", F.lit(True))
    )
 
    # 3) customer_id üzrə birləşdir (left join: uyğun gəlməyən müştərilərdə is_blacklisted = NULL)
    joined = applications.join(blacklist, on="customer_id", how="left")
 
    # 4) risk_status: F.when zənciri yuxarıdan aşağı yoxlanır, ilk uyğun qalır
    risk_status = (
        F.when(F.col("is_blacklisted").isNotNull(), "rejected")
        .when(
            (F.col("requested_amount") > F.col("monthly_income") * 12)
            | (F.col("existing_debt") > F.col("monthly_income") * 0.7)
            | (F.col("employment_years") < 1)
            | (F.col("age") < 21),
            "high_risk",
        )
        .otherwise("approved")
    )
    result = joined.withColumn("risk_status", risk_status).drop("is_blacklisted")
 
    # 5) Hər statusun sayı
    counts = {row["risk_status"]: row["count"] for row in result.groupBy("risk_status").count().collect()}
    print("=== risk_status say ===")
    for status in ("approved", "high_risk", "rejected"):
        print(f"{status}: {counts.get(status, 0)}")
 
    # 6) Delta formatında, application_date üzrə partition, MinIO-ya yaz
    result.write.format("delta").mode("overwrite").partitionBy("application_date").save(args.s3_path)
    print(f"Delta yazıldı: {args.s3_path}")
 
    spark.stop()
 
 
if __name__ == "__main__":
    main()
 