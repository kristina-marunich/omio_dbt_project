from airflow import DAG
from airflow.operators.bash import BashOperator
from astronomer.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime, timedelta

# Define default arguments for the DAG
default_args = {
    "owner": "analytics_engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # Email notification is the core failure handler
    "email_on_failure": True,
    "email": ["data-team@omio.com"],
}

# Define the DAG
with DAG(
    dag_id="bookings_pipeline_dbtcloud",
    description="Orchestrates ingestion and two-stage dbt Cloud transformation (Build/Test -> Deploy)",
    start_date=datetime(2024, 1, 1),
    # Schedule daily at 6 AM UTC (or local time depending on Airflow config)
    schedule_interval="0 6 * * *",
    catchup=False,
    default_args=default_args,
    tags=["dbt", "data-quality", "bookings"],
) as dag:

    # Step 1: Ingest Raw Export (MOCK)
    # This task represents the loading of the raw semi-structured data into the data warehouse.
    ingest_raw_export = BashOperator(
        task_id="ingest_raw_export",
        bash_command="echo 'Downloading latest booking export and loading to raw schema...' && sleep 2",
    )

    # Step 2: RUN MODELS & TESTS ON STAGING
    # This dbt Cloud Job must be configured to run: `dbt build --target staging`
    # Airflow fails the pipeline if any model or test fails in this job.
    run_staging_build_and_test = DbtCloudRunJobOperator(
        task_id="run_staging_build_and_test",
        dbt_cloud_conn_id="dbt_cloud_default",
        job_id=12345,
        check_interval=60,
        timeout=3600,
        trigger_rule="all_success",
    )

    # Step 3: PROMOTE TO PRODUCTION
    # This dbt Cloud Job only runs if Step 2 succeeded (i.e., all tests passed).
    # It must be configured to run: `dbt run --target production`
    run_prod_deploy = DbtCloudRunJobOperator(
        task_id="run_prod_deploy",
        dbt_cloud_conn_id="dbt_cloud_default",
        job_id=67890,
        check_interval=60,
        timeout=3600,
        trigger_rule="all_success",
    )

    # Step 4: Notify Success
    notify_success = BashOperator(
        task_id="notify_success",
        bash_command="echo 'Production pipeline completed",
    )

    # Define the Task Dependencies (The data pipeline flow)
    (
        ingest_raw_export
        >> run_staging_build_and_test  # If this fails, the DAG stops and an email is sent.
        >> run_prod_deploy             # Only runs if staging build/tests pass.
        >> notify_success
    )
