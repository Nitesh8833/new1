# fetch_transform_to_gcs.py
from __future__ import annotations
import io
import json
import logging
import re
from pathlib import Path
from typing import Dict, Optional, Tuple, Union
import pandas as pd
import psycopg2
from google.cloud import storage

# ------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------
CONFIG_PATH = "gs://us-east4-cmp-dev-pdi-ink-05-e6953c0-bucket/dags/pdi-ingestion-gcp/Dev/config/db_config.json"
DEFAULT_SQL = "SELECT * FROM pdipp.prvrostercnf_conformed_file_stats"
DEFAULT_OUTPUT_FMT = "xlsx"
DEFAULT_OUTPUT_SHEET = "Sheet1"

MAPPING: Dict[str, str] = {
    "business_owner": "Business Team",
    "group_type": "Group Team",
    "roster_id": "Roster ID",
    "roster_name": "Provider Entity",
    "parent_transaction_type": "Parent Transaction Type",
    "transaction_type": "Transaction Type",
    "total_rows_with_errors": "Total Number Of Rows With Error",
    "critical_error_codes": "Critical Error Codes",
    "error_details": "Error Description",
}

# Global cache to pass data between tasks
TASK_CONTEXT = {}


# ------------------------------------------------------------------
# Utils
# ------------------------------------------------------------------
def parse_gs_uri(gs_uri: str) -> Tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gs_uri}, must start with 'gs://'")
    path = gs_uri[5:]
    sep = path.find("/")
    if sep == -1:
        raise ValueError(f"Invalid GCS URI: {gs_uri}, must contain bucket and object")
    return path[:sep], path[sep + 1:]


def load_config_task(**context):
    """Task: Load JSON config from local path or GCS."""
    ref = str(CONFIG_PATH)
    if ref.startswith("gs://"):
        bucket_name, blob_path = parse_gs_uri(ref)
        client = storage.Client()
        blob = client.bucket(bucket_name).blob(blob_path)
        cfg = json.loads(blob.download_as_text())
    else:
        with open(ref, "r") as f:
            cfg = json.load(f)

    TASK_CONTEXT["config"] = cfg
    logging.info("Config loaded: %s", cfg.keys())
    return cfg


def connect_db_task(**context):
    """Task: Connect to Cloud SQL (Postgres)."""
    cfg = TASK_CONTEXT["config"]
    db_cfg = cfg["database"]
    conn = psycopg2.connect(
        dbname=db_cfg["dbname"],
        user=db_cfg["user"],
        password=db_cfg["password"],
        host=db_cfg["host"],
        port=int(db_cfg.get("port", 5432)),
    )
    TASK_CONTEXT["db_conn"] = conn
    return "DB connection established"


def run_sql_task(**context):
    """Task: Run SQL query and store DataFrame."""
    cfg = TASK_CONTEXT["config"]
    conn = TASK_CONTEXT["db_conn"]

    sql = cfg.get("sql_query", DEFAULT_SQL)
    df = pd.read_sql_query(sql, conn)
    TASK_CONTEXT["df_src"] = df
    logging.info("Query returned rows=%d cols=%d", len(df), len(df.columns))
    return f"Fetched {len(df)} rows"


def transform_task(**context):
    """Task: Apply renaming & mapping transformation."""
    df = TASK_CONTEXT["df_src"]
    df_out = extract_and_rename(df, MAPPING)
    TASK_CONTEXT["df_out"] = df_out
    logging.info("Transformed DataFrame: rows=%d", len(df_out))
    return f"Transformed {len(df_out)} rows"


def write_to_gcs_task(**context):
    """Task: Write DataFrame to GCS and return final path."""
    cfg = TASK_CONTEXT["config"]
    gcs_cfg = cfg["gcs"]
    df_out = TASK_CONTEXT["df_out"]

    out_bucket = gcs_cfg["bucket_name"]
    ts = context.get("ts_nodash") or pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%S")
    ext = cfg.get("output_format", DEFAULT_OUTPUT_FMT).lower()
    out_object = f"reports/prvrostercnf_file_stats-{ts}.{ext}"

    uri = write_df_to_gcs(
        df_out,
        bucket=out_bucket,
        object_name=out_object,
        fmt=ext,
        sheet_name=DEFAULT_OUTPUT_SHEET,
        auto_increment=True,
    )

    logging.info("Data written to: %s", uri)
    return uri


# ------------------------------------------------------------------
# Helpers from your original code (kept as-is)
# ------------------------------------------------------------------
def _normalize(s: str) -> str:
    return re.sub(r"\s+", "_", s.strip()).lower()


def build_renamer(df: pd.DataFrame, mapping_src_to_out: Dict[str, str]) -> Dict[str, str]:
    norm_to_real = {_normalize(c): c for c in df.columns}
    renamer: Dict[str, str] = {}
    for src_label, out_col in mapping_src_to_out.items():
        key = _normalize(src_label)
        if key in norm_to_real:
            renamer[norm_to_real[key]] = out_col
        else:
            df[out_col] = pd.NA
    return renamer


def extract_and_rename(df: pd.DataFrame, mapping_src_to_out: Dict[str, str]) -> pd.DataFrame:
    renamer = build_renamer(df, mapping_src_to_out)
    selected = df[list(renamer.keys())].rename(columns=renamer) if renamer else pd.DataFrame()
    return selected


def write_df_to_gcs(
    df: pd.DataFrame,
    bucket: str,
    object_name: str,
    fmt: str = "xlsx",
    sheet_name: str = "Sheet1",
    auto_increment: bool = False,
    project_id: Optional[str] = None,
) -> str:
    client = storage.Client(project=project_id) if project_id else storage.Client()
    blob = client.bucket(bucket).blob(object_name)

    if fmt == "csv":
        payload = df.to_csv(index=False)
        blob.upload_from_string(payload, content_type="text/csv")
    elif fmt == "xlsx":
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
        bio.seek(0)
        blob.upload_from_file(
            bio,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        raise ValueError("fmt must be 'csv' or 'xlsx'")

    return f"gs://{bucket}/{object_name}"


***************
# dag_fetch_transform_to_gcs.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Import functions from your script
from fetch_transform_to_gcs import (
    load_config_task,
    connect_db_task,
    run_sql_task,
    transform_task,
    write_to_gcs_task,
)

with DAG(
    dag_id="fetch_transform_to_gcs_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["gcs", "postgres", "etl"],
) as dag:

    t1 = PythonOperator(task_id="load_config", python_callable=load_config_task)
    t2 = PythonOperator(task_id="connect_db", python_callable=connect_db_task)
    t3 = PythonOperator(task_id="run_sql", python_callable=run_sql_task)
    t4 = PythonOperator(task_id="transform", python_callable=transform_task)
    t5 = PythonOperator(task_id="write_to_gcs", python_callable=write_to_gcs_task)

    # Define task order
    t1 >> t2 >> t3 >> t4 >> t5
