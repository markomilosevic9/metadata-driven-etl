from datetime import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

from pipeline.core.config_loader import load_config
from pipeline.stages.dq_metrics import compute_and_persist_dq_metrics


PROJECT_ROOT = "/opt/motor-policy"


default_args = {
    "owner": "marko",
    "depends_on_past": False,
    "start_date": datetime(2024, 10, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


@dag(
    dag_id="motor_policy_pipeline",
    default_args=default_args,
    description="DAG for pipeline",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
)
def motor_policy_pipeline():
    config = load_config()
    jars_csv = ",".join(config.get("spark", {}).get("jars", []))
    storage = config.get("storage", {})

    run_env = {"RUN_ID": "{{ run_id }}"}

    pre_pipeline_tests = BashOperator(
        task_id="pre_pipeline_tests",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"python -m pytest {PROJECT_ROOT}/tests -m \"not post_pipeline\" -v --tb=short"
        ),
        env=run_env,
        append_env=True,
    )

    run_pipeline = SparkSubmitOperator(
        task_id="run_pipeline",
        application=f"{PROJECT_ROOT}/pipeline/jobs/run_pipeline.py",
        conn_id="spark_default",
        jars=jars_csv,
        conf={
            "spark.hadoop.fs.s3a.access.key": storage["access_key"],
            "spark.hadoop.fs.s3a.secret.key": storage["secret_key"],
            "spark.hadoop.fs.s3a.endpoint": storage["endpoint"],
            "spark.hadoop.fs.s3a.path.style.access": str(
                storage.get("path_style_access", True)
            ).lower(),
            "spark.hadoop.fs.s3a.connection.ssl.enabled": str(
                storage.get("secure", False)
            ).lower(),
            "spark.pyspark.python": "python3",
            "spark.pyspark.driver.python": "python3",
            "spark.executorEnv.PYTHONPATH": PROJECT_ROOT,
        },
        # POSTGRES_ANALYTICS_* and MINIO_* env vars are supplied to the Spark driver/executors by docker-compose's per-container env
        env_vars={
            "PYTHONPATH": PROJECT_ROOT,
            "RUN_ID": "{{ run_id }}",
        },
        verbose=True,
    )

    dq_metrics_calculation = PythonOperator(
        task_id="dq_metrics_calculation",
        python_callable=compute_and_persist_dq_metrics,
        op_kwargs={"run_id": "{{ run_id }}"},
    )

    post_pipeline_tests = BashOperator(
        task_id="post_pipeline_tests",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f"python -m pytest {PROJECT_ROOT}/tests -m \"post_pipeline\" -v --tb=short"
        ),
        env=run_env,
        append_env=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        pre_pipeline_tests
        >> run_pipeline
        >> dq_metrics_calculation
        >> post_pipeline_tests
    )


motor_policy_pipeline_dag = motor_policy_pipeline()
