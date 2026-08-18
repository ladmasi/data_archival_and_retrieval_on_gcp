import re
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

PROJECT = Variable.get("gcp_project", default_var="")
BUCKET = Variable.get("archive_bucket", default_var="")
LOCATION = Variable.get("bq_location", default_var="US")

if not PROJECT:
    raise ValueError("Airflow Variable 'gcp_project' is missing or empty.")
if not BUCKET:
    raise ValueError("Airflow Variable 'archive_bucket' is missing or empty.")

FILENAME_PATTERN = re.compile(r"invoice_(?P<order_id>O\d+)_(?P<date>\d{8})\.pdf$")

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}


def preflight_check(**context):
    bq_client = BigQueryHook(gcp_conn_id="google_cloud_default", location=LOCATION).get_client(
        project_id=PROJECT
    )

    orders_count = list(
        bq_client.query(f"SELECT COUNT(*) AS n FROM `{PROJECT}.curated.orders`").result()
    )[0]["n"]
    manifest_count = list(
        bq_client.query(f"SELECT COUNT(*) AS n FROM `{PROJECT}.curated.pdf_manifest`").result()
    )[0]["n"]

    if orders_count == 0:
        raise AirflowFailException(
            "curated.orders is empty. structured_dags must run and populate it "
            "before pdf_archival can validate anything -- every PDF would "
            "incorrectly show as ORPHAN. Check structured_dags succeeded first."
        )
    if manifest_count == 0:
        raise AirflowFailException(
            "curated.pdf_manifest is empty. Confirm pdf_manifest.csv was "
            "actually uploaded to landing/csv/pdf_manifest/ and that "
            "structured_dags' merge_pdf_manifest task succeeded."
        )

    gcs = GCSHook(gcp_conn_id="google_cloud_default")
    pdf_objects = gcs.list(bucket_name=BUCKET, prefix="landing/pdfs/")
    if not pdf_objects:
        raise AirflowFailException(
            f"No objects found under gs://{BUCKET}/landing/pdfs/. "
            "Confirm the invoice PDFs were actually uploaded there."
        )

    print(
        f"Preflight OK: {orders_count} curated orders, "
        f"{manifest_count} manifest rows, {len(pdf_objects)} PDF objects found."
    )


def list_new_pdfs(**context):
    gcs = GCSHook(gcp_conn_id="google_cloud_default")
    bq_client = BigQueryHook(gcp_conn_id="google_cloud_default", location=LOCATION).get_client(
        project_id=PROJECT
    )

    all_pdf_paths = gcs.list(bucket_name=BUCKET, prefix="landing/pdfs/")
    all_filenames = [p.split("/")[-1] for p in all_pdf_paths if p.endswith(".pdf")]

 
    already_valid = {
        row["file_name"]
        for row in bq_client.query(
            f"SELECT DISTINCT file_name FROM `{PROJECT}.ops.pdf_index` "
            f"WHERE status = 'VALID'"
        ).result()
    }

    new_filenames = [f for f in all_filenames if f not in already_valid]
    context["ti"].xcom_push(key="new_filenames", value=new_filenames)
    print(
        f"{len(all_filenames)} total PDFs in landing/, "
        f"{len(already_valid)} already VALID, "
        f"{len(new_filenames)} to (re)process this run."
    )


def has_new_pdfs(**context):
    new_filenames = context["ti"].xcom_pull(task_ids="list_new_pdfs", key="new_filenames") or []
    return len(new_filenames) > 0


def parse_and_validate(**context):
    from pdfplumber import open as open_pdf

    new_filenames = context["ti"].xcom_pull(task_ids="list_new_pdfs", key="new_filenames") or []
    gcs = GCSHook(gcp_conn_id="google_cloud_default")
    bq_client = BigQueryHook(gcp_conn_id="google_cloud_default", location=LOCATION).get_client(
        project_id=PROJECT
    )

    manifest_lookup = {
        row["file_name"]: row["order_id"]
        for row in bq_client.query(
            f"SELECT file_name, order_id FROM `{PROJECT}.curated.pdf_manifest`"
        ).result()
    }
    valid_order_ids = {
        row["order_id"]
        for row in bq_client.query(f"SELECT order_id FROM `{PROJECT}.curated.orders`").result()
    }

    results = []
    for file_name in new_filenames:
        try:
            match = FILENAME_PATTERN.match(file_name)
            filename_order_id = match.group("order_id") if match else None
            filename_date = match.group("date") if match else None

            local_path = f"/tmp/{file_name}"
            gcs.download(bucket_name=BUCKET, object_name=f"landing/pdfs/{file_name}", filename=local_path)

            content_order_id = None
            try:
                with open_pdf(local_path) as pdf:
                    page_text = pdf.pages[0].extract_text() or ""
                found = re.search(r"Order ID:\s*(\S+)", page_text)
                content_order_id = found.group(1) if found else None
            except Exception as parse_error:
                print(f"Could not read PDF content for {file_name}: {parse_error}")

            manifest_order_id = manifest_lookup.get(file_name)

            if manifest_order_id is None:
                status = "INVALID"
            elif manifest_order_id not in valid_order_ids:
                status = "ORPHAN"
            elif filename_order_id != manifest_order_id or (
                content_order_id and content_order_id != manifest_order_id
            ):
                status = "INVALID"
            elif not filename_date:
                status = "INVALID"
            else:
                status = "VALID"

            results.append({
                "file_name": file_name,
                "order_id": manifest_order_id or filename_order_id,
                "filename_order_date": filename_date,
                "status": status,
            })

        except Exception as file_error:
            print(f"Failed to process {file_name}: {file_error}")
            results.append({
                "file_name": file_name,
                "order_id": None,
                "filename_order_date": None,
                "status": "INVALID",
            })

    context["ti"].xcom_push(key="results", value=results)
    valid_count = sum(1 for r in results if r["status"] == "VALID")
    print(f"Processed {len(results)} files: {valid_count} valid.")


def move_valid_to_archive(**context):
    results = context["ti"].xcom_pull(task_ids="parse_and_validate", key="results") or []
    gcs = GCSHook(gcp_conn_id="google_cloud_default")
    updated = []

    for r in results:
        if r["status"] != "VALID":
            continue
        try:
            year, month = r["filename_order_date"][:4], r["filename_order_date"][4:6]
            destination = f"archive/pdfs/{year}/{month}/{r['order_id']}/{r['file_name']}"
            gcs.copy(BUCKET, f"landing/pdfs/{r['file_name']}", BUCKET, destination)
            gcs.delete(bucket_name=BUCKET, object_name=f"landing/pdfs/{r['file_name']}")
            r["gcs_archive_path"] = f"gs://{BUCKET}/{destination}"
        except Exception as move_error:
            print(f"Failed to archive {r['file_name']}: {move_error}")
            r["status"] = "FAILED"
            r["gcs_archive_path"] = None
        updated.append(r)

    context["ti"].xcom_push(key="results_after_move", value=updated)


def move_invalid_to_quarantine(**context):
    results = context["ti"].xcom_pull(task_ids="parse_and_validate", key="results") or []
    gcs = GCSHook(gcp_conn_id="google_cloud_default")
    updated = []

    for r in results:
        if r["status"] == "VALID":
            continue
        try:
            destination = f"quarantine/pdfs/{r['file_name']}"
            gcs.copy(BUCKET, f"landing/pdfs/{r['file_name']}", BUCKET, destination)
            gcs.delete(bucket_name=BUCKET, object_name=f"landing/pdfs/{r['file_name']}")
            r["gcs_archive_path"] = f"gs://{BUCKET}/{destination}"
        except Exception as move_error:
            print(f"Failed to quarantine {r['file_name']}: {move_error}")
            r["status"] = "FAILED"
            r["gcs_archive_path"] = None
        updated.append(r)

    context["ti"].xcom_push(key="results_after_move", value=updated)


def index_to_bigquery(**context):
    valid_moved = context["ti"].xcom_pull(
        task_ids="move_valid_to_archive", key="results_after_move"
    ) or []
    invalid_moved = context["ti"].xcom_pull(
        task_ids="move_invalid_to_quarantine", key="results_after_move"
    ) or []

    results = valid_moved + invalid_moved
    if not results:
        print("No results to index.")
        return

    bq_client = BigQueryHook(gcp_conn_id="google_cloud_default", location=LOCATION).get_client(
        project_id=PROJECT
    )

    rows = [
        {
            "file_name": r["file_name"],
            "order_id": r["order_id"],
            "gcs_archive_path": r.get("gcs_archive_path"),
            "document_type": "INVOICE",
            "filename_order_date": r["filename_order_date"],
            "content_order_date": None,
            "status": r["status"],
            "run_id": context["run_id"],
            "processed_at": datetime.utcnow().isoformat(),
        }
        for r in results
    ]

    errors = bq_client.insert_rows_json(table=f"{PROJECT}.ops.pdf_index", json_rows=rows)
    if errors:
        raise AirflowFailException(f"Failed to insert rows into ops.pdf_index: {errors}")

    failed_count = sum(1 for r in results if r["status"] == "FAILED")
    if failed_count:
        print(f"{failed_count} file(s) failed to move; will retry next run.")


def log_dag_run(**context):
    ti = context["ti"]
    dag_run = context["dag_run"]
    bq_client = BigQueryHook(gcp_conn_id="google_cloud_default", location=LOCATION).get_client(
        project_id=PROJECT
    )

    short_circuit_state = ti.xcom_pull(task_ids="skip_if_no_new_files", key="return_value")
    index_state = dag_run.get_task_instance("index_to_bigquery")
    move_valid_state = dag_run.get_task_instance("move_valid_to_archive")
    move_invalid_state = dag_run.get_task_instance("move_invalid_to_quarantine")

    task_states = [
        t.state for t in (index_state, move_valid_state, move_invalid_state) if t is not None
    ]

    if short_circuit_state is False:
        status = "NO_OP"
    elif any(s == "failed" for s in task_states):
        status = "FAILED"
    elif any(s == "upstream_failed" for s in task_states):
        status = "UPSTREAM_FAILED"
    else:
        status = "SUCCESS"

    row = {
        "run_id": dag_run.run_id,
        "dag_id": dag_run.dag_id,
        "status": status,
        "execution_date": context["ds"],
        "logged_at": datetime.utcnow().isoformat(),
    }
    errors = bq_client.insert_rows_json(table=f"{PROJECT}.ops.audit_log", json_rows=[row])
    if errors:
        raise AirflowFailException(f"Failed to write audit log: {errors}")


with DAG(
    dag_id="pdf_archival",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["archival", "unstructured"],
) as dag:

    t0_preflight = PythonOperator(task_id="preflight_check", python_callable=preflight_check)
    t1_list = PythonOperator(task_id="list_new_pdfs", python_callable=list_new_pdfs)
    t1b_short_circuit = ShortCircuitOperator(
        task_id="skip_if_no_new_files",
        python_callable=has_new_pdfs,
        ignore_downstream_trigger_rules=False,
    )
    t2_parse = PythonOperator(task_id="parse_and_validate", python_callable=parse_and_validate)
    t3_index = PythonOperator(
        task_id="index_to_bigquery",
        python_callable=index_to_bigquery,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )
    t4a_archive = PythonOperator(task_id="move_valid_to_archive", python_callable=move_valid_to_archive)
    t4b_quarantine = PythonOperator(task_id="move_invalid_to_quarantine", python_callable=move_invalid_to_quarantine)
    t5_audit = PythonOperator(
        task_id="audit_log",
        python_callable=log_dag_run,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    t0_preflight >> t1_list >> t1b_short_circuit >> t2_parse
    t2_parse >> [t4a_archive, t4b_quarantine]
    [t4a_archive, t4b_quarantine] >> t3_index >> t5_audit
