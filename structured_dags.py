from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

PROJECT = Variable.get("gcp_project")
BUCKET = Variable.get("archive_bucket")
LOCATION = Variable.get("bq_location", default_var="US")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def log_dag_run(**context):
    dag_run = context["dag_run"]
    hook = BigQueryHook(gcp_conn_id="google_cloud_default", location=LOCATION)

    row = {
        "run_id": dag_run.run_id,
        "dag_id": dag_run.dag_id,
        "task_id": context["task"].task_id,
        "status": "SUCCESS",
        "started_at": dag_run.start_date.isoformat() if dag_run.start_date else None,
        "ended_at": datetime.utcnow().isoformat(),
        "execution_date": context["ds"],
    }

    hook.get_client(project_id=PROJECT).insert_rows_json(
        table=f"{PROJECT}.ops.audit_log",
        json_rows=[row],
    )


with DAG(
    dag_id="structured_dags",
    default_args=default_args,
    schedule_interval="@weekly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    template_searchpath=["/home/airflow/gcs/dags/sql"],
    tags=["archival", "structured"],
) as dag:

    load_customers = GCSToBigQueryOperator(
        task_id="load_customers",
        bucket=BUCKET,
        source_objects=["landing/csv/customers/customers.csv"],
        destination_project_dataset_table=f"{PROJECT}.raw.customers",
        source_format="CSV",
        skip_leading_rows=1,
        schema_fields=[
            {"name": "customer_id", "type": "STRING"},
            {"name": "customer_name", "type": "STRING"},
            {"name": "email", "type": "STRING"},
            {"name": "phone", "type": "STRING"},
            {"name": "created_date", "type": "STRING"},
            {"name": "country", "type": "STRING"},
        ],
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning={"type": "DAY"},
    )

    merge_customers = BigQueryInsertJobOperator(
        task_id="merge_customers",
        location=LOCATION,
        configuration={"query": {"query": "merge_customers.sql", "useLegacySql": False}},
        params={"project": PROJECT, "curated_dataset": "curated", "raw_dataset": "raw"},
    )

    load_customers >> merge_customers

    load_orders = GCSToBigQueryOperator(
        task_id="load_orders",
        bucket=BUCKET,
        source_objects=["landing/csv/orders/orders.csv"],
        destination_project_dataset_table=f"{PROJECT}.raw.orders",
        source_format="CSV",
        skip_leading_rows=1,
        schema_fields=[
            {"name": "order_id", "type": "STRING"},
            {"name": "customer_id", "type": "STRING"},
            {"name": "order_date", "type": "STRING"},
            {"name": "order_status", "type": "STRING"},
            {"name": "order_total", "type": "STRING"},
            {"name": "source_updated_at", "type": "STRING"},
        ],
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning={"type": "DAY"},
    )

    merge_orders = BigQueryInsertJobOperator(
        task_id="merge_orders",
        location=LOCATION,
        configuration={"query": {"query": "merge_orders.sql", "useLegacySql": False}},
        params={"project": PROJECT, "curated_dataset": "curated", "raw_dataset": "raw"},
    )

    load_orders >> merge_orders

    load_order_items = GCSToBigQueryOperator(
        task_id="load_order_items",
        bucket=BUCKET,
        source_objects=["landing/csv/order_items/order_items.csv"],
        destination_project_dataset_table=f"{PROJECT}.raw.order_items",
        source_format="CSV",
        skip_leading_rows=1,
        schema_fields=[
            {"name": "order_item_id", "type": "STRING"},
            {"name": "order_id", "type": "STRING"},
            {"name": "product_id", "type": "STRING"},
            {"name": "product_name", "type": "STRING"},
            {"name": "quantity", "type": "STRING"},
            {"name": "unit_price", "type": "STRING"},
        ],
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning={"type": "DAY"},
    )

    merge_order_items = BigQueryInsertJobOperator(
        task_id="merge_order_items",
        location=LOCATION,
        configuration={"query": {"query": "merge_order_items.sql", "useLegacySql": False}},
        params={"project": PROJECT, "curated_dataset": "curated", "raw_dataset": "raw"},
    )

    load_order_items >> merge_order_items

    load_payments = GCSToBigQueryOperator(
        task_id="load_payments",
        bucket=BUCKET,
        source_objects=["landing/csv/payments/payments.csv"],
        destination_project_dataset_table=f"{PROJECT}.raw.payments",
        source_format="CSV",
        skip_leading_rows=1,
        schema_fields=[
            {"name": "payment_id", "type": "STRING"},
            {"name": "order_id", "type": "STRING"},
            {"name": "payment_date", "type": "STRING"},
            {"name": "payment_method", "type": "STRING"},
            {"name": "amount", "type": "STRING"},
            {"name": "payment_status", "type": "STRING"},
        ],
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning={"type": "DAY"},
    )

    merge_payments = BigQueryInsertJobOperator(
        task_id="merge_payments",
        location=LOCATION,
        configuration={"query": {"query": "merge_payments.sql", "useLegacySql": False}},
        params={"project": PROJECT, "curated_dataset": "curated", "raw_dataset": "raw"},
    )

    load_payments >> merge_payments

    load_returns = GCSToBigQueryOperator(
        task_id="load_returns",
        bucket=BUCKET,
        source_objects=["landing/csv/returns/returns.csv"],
        destination_project_dataset_table=f"{PROJECT}.raw.returns",
        source_format="CSV",
        skip_leading_rows=1,
        schema_fields=[
            {"name": "return_id", "type": "STRING"},
            {"name": "order_id", "type": "STRING"},
            {"name": "return_date", "type": "STRING"},
            {"name": "reason", "type": "STRING"},
            {"name": "refund_amount", "type": "STRING"},
        ],
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning={"type": "DAY"},
    )

    merge_returns = BigQueryInsertJobOperator(
        task_id="merge_returns",
        location=LOCATION,
        configuration={"query": {"query": "merge_returns.sql", "useLegacySql": False}},
        params={"project": PROJECT, "curated_dataset": "curated", "raw_dataset": "raw"},
    )

    load_returns >> merge_returns

    load_pdf_manifest = GCSToBigQueryOperator(
        task_id="load_pdf_manifest",
        bucket=BUCKET,
        source_objects=["landing/csv/pdf_manifest/pdf_manifest.csv"],
        destination_project_dataset_table=f"{PROJECT}.raw.pdf_manifest",
        source_format="CSV",
        skip_leading_rows=1,
        schema_fields=[
            {"name": "file_name", "type": "STRING"},
            {"name": "order_id", "type": "STRING"},
            {"name": "document_type", "type": "STRING"},
            {"name": "created_date", "type": "STRING"},
        ],
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        time_partitioning={"type": "DAY"},
    )

    merge_pdf_manifest = BigQueryInsertJobOperator(
        task_id="merge_pdf_manifest",
        location=LOCATION,
        configuration={"query": {"query": "merge_pdf_manifest.sql", "useLegacySql": False}},
        params={"project": PROJECT, "curated_dataset": "curated", "raw_dataset": "raw"},
    )

    load_pdf_manifest >> merge_pdf_manifest

    run_dq_checks = BigQueryInsertJobOperator(
        task_id="run_dq_checks",
        location=LOCATION,
        configuration={"query": {"query": "dq_checks.sql", "useLegacySql": False}},
        params={
            "project": PROJECT,
            "curated_dataset": "curated",
            "ops_dataset": "ops",
            "run_date": "{{ ds }}",
            "run_id": "{{ run_id }}",
        },
    )

    audit_log = PythonOperator(
        task_id="audit_log",
        python_callable=log_dag_run,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    trigger_pdf_dag = TriggerDagRunOperator(
        task_id="trigger_pdf_archival",
        trigger_dag_id="pdf_archival",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        wait_for_completion=False,
    )

    merge_customers >> merge_orders
    merge_orders >> merge_order_items
    merge_orders >> merge_payments
    merge_orders >> merge_returns

    merge_order_items >> run_dq_checks
    merge_payments >> run_dq_checks
    merge_returns >> run_dq_checks
    merge_pdf_manifest >> run_dq_checks

    run_dq_checks >> audit_log
    run_dq_checks >> trigger_pdf_dag
