from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(
    dag_id="agri_pipeline_workflow",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["agri"]
) as dag:

    trigger_pipeline = DatabricksRunNowOperator(
        task_id="run_crop_data_pipeline",
        databricks_conn_id="databricks_default",
        job_id=585526303294808
    )

    trigger_pipeline
