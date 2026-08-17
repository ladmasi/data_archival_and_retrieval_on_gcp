"""
weekly_housekeeping.py

Two fixes applied:
1. dq_summary_report's params used to pass the literal string "{{ ds }}"
   through params.run_date, which never got a second Jinja render pass --
   the SQL file now references {{ ds }} directly instead, which resolves
   correctly since Airflow macros work when written directly into a
   templated field.
2. log_dag_run called BigQueryHook.insert_rows_json() directly -- that
   method only exists on google.cloud.bigquery.Client, reached via
   .get_client(). Same bug already fixed in structured_dags.py and
   pdf_archival.py; this file just hadn't been touched yet.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import storage

PROJECT = Variable.get("gcp_project")
BUCKET = Variable.get("archive_bucket")
LOCATION = Variable.get("bq_location", default_var="US")
RETENTION_DAYS = Variable.get("audit_log_retention_days", default_var="90")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}


def check_storage_lifecycle_rule(**context):
    client = storage.Client(project=PROJECT)
    bucket = client.bucket(BUCKET)
    bucket.reload()

    rules = list(bucket.lifecycle_rules)
    has_rule = len(rules) > 0
    if not has_rule:
        print(f"WARNING: bucket {BUCKET} has no lifecycle rule set.")
    else:
        print(f"OK: bucket {BUCKET} has {len(rules)} lifecycle rule(s).")


def log_dag_run(**context):
    dag_run = context["dag_run"]
    bq_client = BigQueryHook(gcp_conn_id="google_cloud_default", location=LOCATION).get_client(
        project_id=PROJECT
    )
    row = {
        "run_id": dag_run.run_id,
        "dag_id": dag_run.dag_id,
        "status": "SUCCESS",
        "execution_date": context["ds"],
        "logged_at": datetime.utcnow().isoformat(),
    }
    errors = bq_client.insert_rows_json(table=f"{PROJECT}.ops.audit_log", json_rows=[row])
    if errors:
        raise AirflowFailException(f"Failed to write audit log: {errors}")


with DAG(
    dag_id="weekly_housekeeping",
    default_args=default_args,
    schedule_interval="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    template_searchpath=["/home/airflow/gcs/dags/sql"],
    tags=["archival", "housekeeping"],
) as dag:

    reconcile_orphan_pdfs = BigQueryInsertJobOperator(
        task_id="reconcile_orphan_pdfs",
        location=LOCATION,
        configuration={"query": {"query": "reconcile_orphans.sql", "useLegacySql": False}},
        params={"project": PROJECT, "curated_dataset": "curated", "ops_dataset": "ops"},
    )

    dq_summary_report = BigQueryInsertJobOperator(
        task_id="dq_summary_report",
        location=LOCATION,
        configuration={"query": {"query": "dq_summary.sql", "useLegacySql": False}},
        params={"project": PROJECT, "ops_dataset": "ops"},  
    )

    storage_lifecycle_check = PythonOperator(
        task_id="storage_lifecycle_check",
        python_callable=check_storage_lifecycle_rule,
    )

    purge_old_audit_logs = BigQueryInsertJobOperator(
        task_id="purge_old_audit_logs",
        location=LOCATION,
        configuration={"query": {"query": "purge_audit_logs.sql", "useLegacySql": False}},
        params={"project": PROJECT, "ops_dataset": "ops", "retention_days": RETENTION_DAYS},
    )

    audit_log = PythonOperator(
        task_id="audit_log",
        python_callable=log_dag_run,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    reconcile_orphan_pdfs >> dq_summary_report >> storage_lifecycle_check >> purge_old_audit_logs >> audit_log