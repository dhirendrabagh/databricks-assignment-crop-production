from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(
    dag_id="test_databricks_connection",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    test_connection = DatabricksRunNowOperator(
        task_id="run_test_job",
        databricks_conn_id="databricks_default",
        job_id=1030911163894154
    )
