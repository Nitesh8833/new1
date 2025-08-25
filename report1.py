# -*- coding: utf-8 -*-
from __future__ import annotations

import os, json, logging
from datetime import datetime
import pendulum
from google.cloud import storage

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.state import State
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

logging.basicConfig(level=logging.INFO)

# ------------------------ Load config from GCS ------------------------ #
def load_config_from_gcs(bucket_name: str, blob_name: str) -> dict:
    client = storage.Client()
    blob = client.bucket(bucket_name).blob(blob_name)
    return json.loads(blob.download_as_text())

CFG_BUCKET = "us-east4-cmp-dev-pdi-ink-05-5e6953c0-bucket"
CFG_BLOB   = "dags/pdi-ingestion-gcp/Dev/config/db_config.json"

cfg_all = load_config_from_gcs(CFG_BUCKET, CFG_BLOB)
cfg             = cfg_all["config"]
cluster_block   = cfg_all["cluster_config"]
CLUSTER_CONFIG  = cluster_block["config"]
BASE_CLUSTER_NAME = cluster_block["cluster_name"]
REGION = cfg_all.get("region", cfg["REGION"])

# ---------------------------- Convenience ---------------------------- #
PROJECT_ID = os.environ.get("GCP_PROJECT") or cfg["PROJECT_ID"]
DAG_TAGS   = cfg.get("DAG_TAGS", [])
CONNECT_SA = cfg["CONNECT_SA"]

TZ_NAME = cfg.get("DAG_TZ", "Asia/Kolkata")
TZ = pendulum.timezone(TZ_NAME)

def final_status(**kwargs):
    for ti in kwargs["dag_run"].get_task_instances():
        if ti.task_id != kwargs["task_instance"].task_id and ti.current_state() != State.SUCCESS:
            raise Exception(f"Task {ti.task_id} failed. Failing this DAG run")

def build_pyspark_job(main_py_uri: str) -> dict:
    return {
        "reference": {"project_id": PROJECT_ID},
        # NOTE: cluster_name is set per-DAG right before submission
        "pyspark_job": {
            "main_python_file_uri": main_py_uri,
            "args": [
                "--ENV",               f"{cfg['ENVIRONMENT']}",
                "--LOGIN_URL",         f"{cfg['DEA_LOGIN_URL']}",
                "--LGN_EMAIL",         f"{cfg['DEA_LGN_ID']}",
                "--LGN_PD",            f"{cfg['DEA_LGN_PD']}",
                "--GCS_BUCKET",        f"{cfg['STG_STORAGE_BUCKET']}",
                "--GCS_BLOB_NAME",     f"{cfg['DEA_BLOB_NAME']}",
                "--DB_INSTANCE",       f"{cfg['DEA_DB_INSTANCE']}",
                "--DB_USER",           f"{cfg['DEA_DB_USER']}",
                "--DB_PD",             f"{cfg['DEA_DB_PWD']}",
                "--DEA_DB_NAME",       f"{cfg['DEA_DB_NAME']}",
                "--TBL_NAME",          f"{cfg['DEA_TBL_NAME']}",
                "--DEA_BKP_TBL_NAME",  f"{cfg['DEA_BKP_TBL_NAME']}",
                "--DEA_STG_TBL_NAME",  f"{cfg['DEA_STG_TBL_NAME']}",
                "--STORAGE_PROJECT_ID", f"{cfg['STORAGE_PROJECT_ID']}",
                "--STG_STORAGE_BUCKET", f"{cfg['STG_STORAGE_BUCKET']}",
                "--BQ_PROJECT_ID",     f"{cfg['BQ_PROJECT_ID']}",
                "--BQ_PDI_DS",         f"{cfg['BQ_PDI_DS']}",
                "--BQ_TBL_NAME",       f"{cfg['BQ_TBL_NAME']}",
                "--FROM_EMAIL",        f"{cfg['FROM_EMAIL']}",
                "--TO_EMAIL",          f"{cfg['TO_EMAIL']}",
                "--TO_DEAEXPEMAIL",    f"{cfg['TO_DEAEXPEMAIL']}",
                "--SMTP_SERVER",       f"{cfg['SMTP_SERVER']}",
            ],
        },
    }

def make_dataproc_dag(*, dag_id: str, schedule: str, main_py_uri: str, cluster_suffix: str):
    default_args = {"project_id": PROJECT_ID, "retries": 0}
    start = pendulum.datetime(2025, 1, 1, 0, 0, tz=TZ)

    dag = DAG(
        dag_id,
        description=f"{dag_id} — Dataproc PySpark job",
        default_args=default_args,
        schedule=schedule,          # if on older Airflow, use schedule_interval=schedule
        start_date=start,
        catchup=False,
        tags=DAG_TAGS,
        timezone=TZ,                # ensures cron runs at 08:00 in TZ_NAME
    )

    cluster_name = f"{BASE_CLUSTER_NAME}-{cluster_suffix}"

    # Build job and add placement with this DAG's cluster name
    job = build_pyspark_job(main_py_uri)
    job["placement"] = {"cluster_name": cluster_name}

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=cluster_name,
        cluster_config=CLUSTER_CONFIG,
        delete_on_error=True,
        use_if_exists=False,
        impersonation_chain=CONNECT_SA,
        dag=dag,
    )

    submit = DataprocSubmitJobOperator(
        task_id="run_pyspark",
        job=job,
        project_id=PROJECT_ID,
        region=REGION,
        impersonation_chain=CONNECT_SA,
        dag=dag,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=cluster_name,
        impersonation_chain=CONNECT_SA,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )

    final_status_task = PythonOperator(
        task_id="final_status",
        python_callable=final_status,
        trigger_rule=TriggerRule.ALL_DONE,
        dag=dag,
    )

    create_cluster >> submit >> delete_cluster >> final_status_task
    return dag

# -------------------------- Three DAGs w/ schedules -------------------------- #
# Use explicit keys for the three scripts
DAILY_MAIN   = cfg["PY_FILE_DAILY"]
WEEKLY_MAIN  = cfg["PY_FILE_WEEKLY"]
MONTHLY_MAIN = cfg["PY_FILE_MONTHLY"]

daily_schedule   = "0 8 * * *"   # daily 08:00
weekly_schedule  = "0 8 * * 1"   # Mondays 08:00
monthly_schedule = "0 8 1 * *"   # 1st of month 08:00

DAG_ID_PREFIX = cfg["DAG_ID_PREFIX"]

daily_dag   = make_dataproc_dag(
    dag_id=f"{DAG_ID_PREFIX}_daily",
    schedule=daily_schedule,
    main_py_uri=DAILY_MAIN,
    cluster_suffix="daily",
)

weekly_dag  = make_dataproc_dag(
    dag_id=f"{DAG_ID_PREFIX}_weekly",
    schedule=weekly_schedule,
    main_py_uri=WEEKLY_MAIN,
    cluster_suffix="weekly",
)

monthly_dag = make_dataproc_dag(
    dag_id=f"{DAG_ID_PREFIX}_monthly",
    schedule=monthly_schedule,
    main_py_uri=MONTHLY_MAIN,
    cluster_suffix="monthly",
)

globals().update({
    daily_dag.dag_id: daily_dag,
    weekly_dag.dag_id: weekly_dag,
    monthly_dag.dag_id: monthly_dag,
})

*******************************************
from __future__ import annotations

import logging
import os
import smtplib
from pathlib import Path
from typing import Iterable, Optional, Tuple, Union

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# =============== CONFIG (edit as needed) ===============
EMAIL_SENDER: str = os.getenv("EMAIL_SENDER", "noreply@example.com")
EMAIL_TO: list[str] = ["you@example.com"]  # str or list[str] both ok
EMAIL_SUBJECT: str = "Roster Report Ready"
EMAIL_TEXT: str = "Hello,\n\nPlease find the latest report attached.\n\nRegards,\nNitesh"

SMTP_SERVER: str = os.getenv("SMTP_SERVER", "smtp.example.com")
SMTP_PORT: int = int(os.getenv("SMTP_PORT", "587"))  # 25 or 587
USE_TLS: bool = True                                  # True for 587, False for 25
SMTP_USERNAME: Optional[str] = os.getenv("SMTP_USERNAME")  # set if your server needs auth
SMTP_PASSWORD: Optional[str] = os.getenv("SMTP_PASSWORD")

LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
# =======================================================

# Types accepted in `attachments`
Pathish = Union[str, Path]
MemAttach = Tuple[Union[bytes, bytearray], str, str]  # (data, "name.ext", "mime/type")
PathAttach1 = Tuple[Pathish]                          # (path,)
PathAttach2 = Tuple[Pathish, str]                     # (path, "alias.ext")
Attachment = Union[Pathish, MemAttach, PathAttach1, PathAttach2]


def send_email_alert(
    recipient: Union[str, Iterable[str]],
    subject: str,
    message_body: str,
    html_content: Optional[str] = None,
    attachments: Optional[Iterable[Attachment]] = None,
    smtp_server: str = SMTP_SERVER,
    smtp_port: int = SMTP_PORT,
    sender: str = EMAIL_SENDER,
    use_tls: bool = USE_TLS,
    username: Optional[str] = SMTP_USERNAME,
    password: Optional[str] = SMTP_PASSWORD,
) -> bool:
    """
    Send an email with plain text + optional HTML and attachments.

    Attachments accepted:
      - "path/to/file" or Path(...)
      - (path,) or (path, "alias.ext")
      - (bytes_or_bytearray, "name.ext", "mime/type")
    """
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    # Normalize recipients
    if isinstance(recipient, (list, tuple, set)):
        recipients = list(recipient)
        to_header = ", ".join(recipients)
    else:
        recipients = [str(recipient)]
        to_header = recipients[0]

    # Build message
    msg = MIMEMultipart("alternative")
    msg["From"] = sender
    msg["To"] = to_header
    msg["Subject"] = subject

    # Plain + optional HTML
    msg.attach(MIMEText(message_body, "plain"))
    if html_content:
        msg.attach(MIMEText(html_content, "html"))

    # -------- Attachments (paths, tuples, or in-memory bytes) --------
    if attachments:
        for item in attachments:
            # In-memory: (bytes, "name.ext", "mime/type")
            if (
                isinstance(item, (tuple, list))
                and len(item) >= 2
                and isinstance(item[0], (bytes, bytearray))
            ):
                data: Union[bytes, bytearray] = item[0]
                name: str = str(item[1])
                mime: str = str(item[2]) if len(item) >= 3 else "application/octet-stream"
                if "/" in mime:
                    maintype, subtype = mime.split("/", 1)
                else:
                    maintype, subtype = "application", "octet-stream"

                part = MIMEBase(maintype, subtype)
                part.set_payload(data)
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", f'attachment; filename="{name}"')
                msg.attach(part)
                continue

            # Path-like: "path", Path(...), (path,), (path, "alias")
            filename_override: Optional[str] = None
            if isinstance(item, (tuple, list)):
                if not item:
                    continue
                file_path = item[0]
                if len(item) > 1:
                    filename_override = str(item[1])
            else:
                file_path = item

            try:
                p = Path(os.fspath(file_path))
            except TypeError:
                logging.warning("Skipping attachment with invalid type: %r", item)
                continue

            if not p.exists():
                logging.warning("Attachment not found, skipping: %s", p)
                continue

            with p.open("rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            filename = filename_override or p.name
            part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
            msg.attach(part)
    # ------------------------------------------------------------------

    # Send
    try:
        logging.info("Connecting SMTP %s:%s (TLS=%s)", smtp_server, smtp_port, use_tls)
        with smtplib.SMTP(smtp_server, smtp_port, timeout=30) as server:
            if use_tls:
                server.starttls()
            if username and password:
                server.login(username, password)
            logging.info("Sending email to: %s", to_header)
            server.send_message(msg)
        logging.info("Email sent.")
        return True
    except Exception as e:
        logging.error("Email send failed: %s", e)
        return False

*********************************************# at top of file:
import os
from email.mime.application import MIMEApplication

# ... inside send_email_alert(), REPLACE your attachments loop with this:
# ----------------------------------------------------------------------
# Attachments: supports
#  - "path/to/file"
#  - (path,) or (path, "alias.ext")
#  - (bytes_or_bytearray, "name.ext", "mime/type")
if attachments:
    for item in attachments:
        filename_override = None

        # (bytes, "name", "mime/type") → in-memory attach
        if isinstance(item, (tuple, list)) and len(item) >= 2 and isinstance(item[0], (bytes, bytearray)):
            data = item[0]
            name = str(item[1])
            mime = str(item[2]) if len(item) >= 3 else "application/octet-stream"
            maintype, subtype = mime.split("/", 1) if "/" in mime else ("application", "octet-stream")

            part = MIMEBase(maintype, subtype)
            part.set_payload(data)
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f'attachment; filename="{name}"')
            msg.attach(part)
            continue

        # (path,) or (path, "alias.ext")
        if isinstance(item, (tuple, list)):
            if not item:
                continue
            file_path = item[0]
            if len(item) > 1:
                filename_override = str(item[1])
        else:
            file_path = item

        # normalize to Path for filesystem attachment
        try:
            p = Path(os.fspath(file_path))
        except TypeError:
            logging.warning("Skipping attachment with invalid type: %r", item)
            continue

        if not p.exists():
            logging.warning("Attachment not found, skipping: %s", p)
            continue

        with p.open("rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        filename = filename_override or p.name
        part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
        msg.attach(part)
# ----------------------------------------------------------------------

# ---------------- Attachments (paths, DataFrames, bytes) ----------------
if attachments:
    for item in attachments:
        data: bytes
        filename: str
        mime: str | None = None

        # CASE A: user passed a tuple/list like (obj, "name.ext", "mime/type"?)
        if isinstance(item, (tuple, list)):
            if not item:
                continue
            obj = item[0]
            filename = str(item[1]) if len(item) > 1 else "attachment.bin"
            mime = str(item[2]) if len(item) > 2 else None

            # DataFrame -> bytes
            if pd is not None and isinstance(obj, pd.DataFrame):
                if filename.lower().endswith(".xlsx"):
                    buf = io.BytesIO()
                    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
                        obj.to_excel(w, index=False, sheet_name="Sheet1")
                    data = buf.getvalue()
                    mime = mime or "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                else:
                    # default to CSV
                    data = obj.to_csv(index=False).encode("utf-8")
                    mime = mime or "text/csv"

            # file-like (BytesIO or anything with .read())
            elif hasattr(obj, "read"):
                data = obj.read()
                if isinstance(data, str):
                    data = data.encode("utf-8")
                mime = mime or "application/octet-stream"

            # bytes / bytearray
            elif isinstance(obj, (bytes, bytearray)):
                data = bytes(obj)
                mime = mime or "application/octet-stream"

            # fallback: treat as a path
            else:
                p = Path(os.fspath(obj))
                if not p.exists():
                    logging.warning("Attachment not found, skipping: %s", p)
                    continue
                with p.open("rb") as f:
                    data = f.read()
                if not filename or filename == "attachment.bin":
                    filename = p.name
                mime = mime or "application/octet-stream"

        # CASE B: simple path (str/Path)
        else:
            p = Path(os.fspath(item))
            if not p.exists():
                logging.warning("Attachment not found, skipping: %s", p)
                continue
            with p.open("rb") as f:
                data = f.read()
            filename = p.name
            mime = "application/octet-stream"

        # attach
        m = mime or "application/octet-stream"
        main, sub = m.split("/", 1)
        part = MIMEBase(main, sub)
        part.set_payload(data)
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
        msg.attach(part)
# -----------------------------------------------------------------------

********************************
import io

if SEND_EMAIL:
    csv_bytes = out_df.to_csv(index=False).encode("utf-8-sig")
    text = (
        EMAIL_TEXT
        + f"\n\nRows: {len(out_df)} | Columns: {len(out_df.columns)}"
    )
    html = out_df.to_html(index=False)

    send_email_alert(
        recipient=EMAIL_TO,
        subject=EMAIL_SUBJECT,
        message_body=text,
        html_content=html,  # shows the DF as a table in the email body
        attachments=[("roster_report.csv", csv_bytes, "text/csv")],  # in-memory
    )

*******************
import io
import pandas as pd

if SEND_EMAIL:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        out_df.to_excel(writer, index=False, sheet_name="Report")
    buf.seek(0)

    xlsx_bytes = buf.getvalue()
    text = (
        EMAIL_TEXT
        + f"\n\nRows: {len(out_df)} | Columns: {len(out_df.columns)}"
    )
    html = out_df.to_html(index=False)

    send_email_alert(
        recipient=EMAIL_TO,
        subject=EMAIL_SUBJECT,
        message_body=text,
        html_content=html,  # also show the table inline
        attachments=[(
            "roster_report.xlsx",
            xlsx_bytes,
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )],
    )

# ----- Secret names in Secret Manager (no project prefix needed) -----
SECRET_DB_USER     = "pdi_prvrstrcnf_cloud_sql_user"
SECRET_DB_PASSWORD = "pdi_prvrstrcnf_cloud_sql_password"
SECRET_DB_NAME     = "pdi_prvrstrcnf_dbname"   # create this if you haven't already


def get_secret_value(project_id: str, secret_name: str, creds, version: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient(credentials=creds)
    name = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
    logging.info("Fetching secret: %s", secret_name)
    resp = client.access_secret_version(name=name)
    return resp.payload.data.decode("utf-8")

def get_db_credentials(project_id: str, creds) -> dict:
    """Return {'user','password','dbname'} pulled from Secret Manager."""
    return {
        "user":     get_secret_value(project_id, SECRET_DB_USER, creds),
        "password": get_secret_value(project_id, SECRET_DB_PASSWORD, creds),
        "dbname":   get_secret_value(project_id, SECRET_DB_NAME, creds),
    }

def query_df_from_config(config_path: str) -> pd.DataFrame:
    """Connect using host/port from JSON + user/password/dbname from Secret Manager."""
    cfg = _load_json(config_path)
    sql = cfg["sql_query"]
    db  = cfg["database"]              # expects host/port here

    # Pull project & credentials from environment (Composer/Dataproc SA)
    creds, project_id = google.auth.default()

    # Fetch user/password/dbname from Secret Manager
    sm = get_db_credentials(project_id, creds)

    conn = psycopg2.connect(
        dbname=sm["dbname"],
        user=sm["user"],
        password=sm["password"],
        host=db["host"],
        port=int(db.get("port", 5432)),
    )
    try:
        return pd.read_sql_query(sql, conn)
    finally:
        conn.close()


{
  "sql_query": "SELECT * FROM pdipp.prvrostercnf_conformed_file_stats",
  "database": {
    "host": "10.0.0.12",
    "port": 5432
  }
}


*************************
  def get_db_credentials(project_id: str, creds) -> dict:
    return {
        "user":     get_secret_value(project_id, "pdi_prvrstrcnf_cloud_sql_user", creds),
        "password": get_secret_value(project_id, "pdi_prvrstrcnf_cloud_sql_password", creds),
        "dbname":   get_secret_value(project_id, "pdi_prvrstrcnf_dbname", creds),
    }

**************************
  SECRET_DB_USER     = os.getenv("SM_DB_USER_NAME",     "pdi_prvrstrcnf_cloud_sql_user")
SECRET_DB_PASSWORD = os.getenv("SM_DB_PASSWORD_NAME", "pdi_prvrstrcnf_cloud_sql_password")
SECRET_DB_NAME     = os.getenv("SM_DB_NAME_NAME",     "pdi_prvrstrcnf_dbname")
*******************
get_secret_value(
    project_id="other-project",  # or ignore this param and pass full path below
    secret_name="projects/other-project/secrets/pdi_prvrstrcnf_cloud_sql_user/versions/latest",
    creds=creds
)


