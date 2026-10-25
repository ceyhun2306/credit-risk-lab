from datetime import datetime, timedelta
 
from airflow.exceptions import AirflowFailException
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk import DAG, TaskGroup, Variable
 
PG_CONN_ID = "pg_conn_id"
SPARK_CONN_ID = "spark_default"
FS_CONN_ID = "fs_default"
CONFIG_VAR = "credit_pipeline_config"   # {"postgres_table": "...", "s3_path": "..."}
BLACKLIST_PATH = "/opt/airflow/data/blacklist.csv"
SPARK_APP = "/opt/airflow/spark_jobs/credit_risk_job.py"
SPARK_PACKAGES = ",".join(
    [
        "org.postgresql:postgresql:42.7.4",
        "io.delta:delta-spark_2.12:3.2.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]
)
 
 
def get_yesterday(logical_date) -> str:
    """'Dünən' = logical_date - 1 gün. Hesab YALNIZ burada aparılır (sensor SQL-i və branch eyni funksiyanı işlədir)."""
    if logical_date is None:
        raise ValueError("logical_date boşdur: DAG-ı Trigger edəndə 'Logical Date' göstər.")
    return (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")
 
 
def notify_failure(context):
    """Hər task uğursuz olanda: hansı DAG, hansı task."""
    ti = context["ti"]
    print(f"XƏTA: '{ti.dag_id}' DAG-ında '{ti.task_id}' task-ı uğursuz oldu (run_id: {ti.run_id}).")
 
 
def choose_branch(**context):
    yesterday = get_yesterday(context["logical_date"])
    table = Variable.get(CONFIG_VAR, deserialize_json=True)["postgres_table"]
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    row_count = hook.get_first(
        f"SELECT COUNT(*) FROM {table} WHERE application_date = %s", parameters=(yesterday,)
    )[0]
    print(f"{yesterday} üçün sətir sayı: {row_count}")
    return "run_spark" if row_count > 2 else "fail_task"
 
 
def always_fail():
    raise AirflowFailException("Dünən üçün sətir sayı 2-dən çox deyil, pipeline dayandırıldı.")
 
 
with DAG(
    dag_id="credit_risk_pipeline",
    description="Kredit müraciətlərinin risk qiymətləndirməsi (Postgres + blacklist -> Spark -> Delta/MinIO)",
    start_date=datetime(2026, 3, 1),
    schedule=None,   # yalnız əl ilə Trigger
    catchup=False,
    default_args={"on_failure_callback": notify_failure},
    user_defined_macros={"yesterday": get_yesterday},   # Jinja-da {{ yesterday(logical_date) }}
    tags=["credit", "spark", "delta"],
) as dag:
 
    start = EmptyOperator(task_id="start")
 
    # Bonus: iki sensor TaskGroup-da (prefix_group_id=False -> task id-ləri dəyişmir)
    with TaskGroup(group_id="sensors", prefix_group_id=False) as sensors:
        check_yesterday_data = SqlSensor(
            task_id="check_yesterday_data",
            conn_id=PG_CONN_ID,
            sql=(
                "SELECT COUNT(*) FROM {{ var.json.credit_pipeline_config.postgres_table }} "
                "WHERE application_date = '{{ yesterday(logical_date) }}'"
            ),
            mode="reschedule",
            poke_interval=10,
            timeout=60,
        )
 
        check_blacklist_file = FileSensor(
            task_id="check_blacklist_file",
            filepath=BLACKLIST_PATH,
            fs_conn_id=FS_CONN_ID,
            mode="reschedule",
            poke_interval=10,
            timeout=60,
        )
 
    branch_on_row_count = BranchPythonOperator(
        task_id="branch_on_row_count",
        python_callable=choose_branch,
    )
 
    fail_task = PythonOperator(
        task_id="fail_task",
        python_callable=always_fail,
    )
 
    run_spark = SparkSubmitOperator(
        task_id="run_spark",
        conn_id=SPARK_CONN_ID,
        application=SPARK_APP,
        name="credit_risk_job",
        packages=SPARK_PACKAGES,
        application_args=[
            "--table", "{{ var.json.credit_pipeline_config.postgres_table }}",
            "--s3-path", "{{ var.json.credit_pipeline_config.s3_path }}",
        ],
        # Login/parol pg_conn_id Connection-dan, mühit dəyişəni kimi (kodda yazılmır)
        env_vars={
            "PG_USER": "{{ conn.pg_conn_id.login }}",
            "PG_PASSWORD": "{{ conn.pg_conn_id.password }}",
            "PG_HOST": "{{ conn.pg_conn_id.host }}",
            "PG_PORT": "{{ conn.pg_conn_id.port }}",
            "PG_DB": "{{ conn.pg_conn_id.schema }}",
        },
        verbose=True,
    )
 
    end_task = EmptyOperator(
        task_id="end_task",
        trigger_rule="all_done",   # qabaqkılar success / failed / skipped olsa da işləyir
    )
 
    start >> sensors >> branch_on_row_count >> [run_spark, fail_task] >> end_task