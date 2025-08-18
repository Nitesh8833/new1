# dags/fetch_transform_to_gcs_dag.py
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from importlib.machinery import SourceFileLoader
import logging

# 1) ABSOLUTE PATH to your python file (fix the case exactly!)
PY_FILE = (
    "/home/airflow/gcs/dags/pdi-ingestion-gcp/Dev/scripts/python_scripts/"
    "fetch_transform_to_gcs.py"
)

# 2) GCS JSON config (what your script needs to read)
GCS_CONFIG_URI = (
    "gs://us-east4-cmp-dev-pdi-ink-05-5e6953c0-bucket/"
    "dags/pdi-ingestion-gcp/Dev/config/db_config.json"
)

def run_wrapper(**context):
    # Load the module from file regardless of package names / hyphens
    mod = SourceFileLoader("fetch_transform_to_gcs", PY_FILE).load_module()

    # If your script uses a global CONFIG_PATH, set it here:
    try:
        mod.CONFIG_PATH = GCS_CONFIG_URI
    except Exception:
        pass

    # Call the entry point. If your main() expects parameters, pass them here.
    # Example if you changed your script to main(gcs_config_uri: str):
    # return mod.main(gcs_config_uri=GCS_CONFIG_URI)
    logging.info("Calling script main() …")
    return mod.main()

with DAG(
    dag_id="fetch_transform_to_gcs",
    description="Fetch/Transform and write to GCS",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    run_job = PythonOperator(
        task_id="run_fetch_transform",
        python_callable=run_wrapper,
        provide_context=True,
    )
******

# dags/fetch_transform_to_gcs_dag.py
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# 1) Point sys.path to the folder that contains your .py script
#    (Composer’s /dags is already on PYTHONPATH; because your folder names have hyphens
#     you can’t import them as packages, so we append the inner directory)
sys.path.append("/home/airflow/gcs/dags/pdi-ingestion-gcp/Dev/scripts/python_scripts")

# 2) Import your function
from fetch_transform_to_gcs import main as job_main

# 3) GCS JSON config URI that your script needs
GCS_CONFIG_URI = (
    "gs://us-east4-cmp-dev-pdi-ink-05-5e6953c0-bucket/"
    "dags/pdi-ingestion-gcp/Dev/config/db_config.json"
)

def run_wrapper(**context):
    """
    Wrapper that injects config and ensures failures fail the task.
    Adjust to fit your script’s signature:
      - If your main() expects kwargs, keep **context,
      - If it expects a param (e.g., gcs_config_uri), pass it explicitly.
    """
    # If your module reads a global CONFIG_PATH, set it here:
    try:
        import fetch_transform_to_gcs as mod
        mod.CONFIG_PATH = GCS_CONFIG_URI
    except Exception:
        # If your script expects a parameter instead, just call job_main(gcs_config_uri=...)
        pass

    # Now call your script’s entry point
    # If main expects context, it will accept **context; otherwise remove it.
    return job_main()

with DAG(
    dag_id="fetch_transform_to_gcs",
    description="Fetch/Transform and write to GCS",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    run_job = PythonOperator(
        task_id="run_fetch_transform",
        python_callable=run_wrapper,
        provide_context=True,   # harmless in A2; allows **context if your main uses it
    )

*******************************************************
# fetch_transform_to_gcs.py
#fetch_transform_to_gcs.py
from __future__ import annotations

import io
import json
import logging
import os
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import psycopg2
from google.cloud import storage

# ------------------------------------------------------------------------------
# CONFIG – where your JSON lives in Composer (synced from your GCS /dags path)
# ------------------------------------------------------------------------------
CONFIG_PATH = Path(
    "/home/airflow/gcs/dags/pdi-ingestion-gcp/Dev/config/db_config.json"
)

# Default query (can be overridden by config)
DEFAULT_SQL = "SELECT * FROM pdipp.prvrostercnf_conformed_file_stats"

# Default output
DEFAULT_OUTPUT_FMT = "xlsx"  # or "csv"
DEFAULT_OUTPUT_SHEET = "Sheet1"

# ------------------------------------------------------------------------------
# MAPPING (source header -> output header)  — your two-arg pipeline mapping
# ------------------------------------------------------------------------------
MAPPING: Dict[str, str] = {
    "business_owner": "Business Team",
    "group_type": "Group Team",
    "roster_id": "Roster ID",
    "roster_name": "Provider Entity",
    "parent_transaction_type": "Parent Transaction Type",
    "transaction_type": "Transaction Type",
    "total_rows_with_errors": "Total Number of Rows With Error",
    "critical_error_codes": "Critical Error Codes",
    "error_details": "Error Description",
}

# ------------------------------------------------------------------------------
# Load config
# ------------------------------------------------------------------------------
def load_config(path: Path = CONFIG_PATH) -> dict:
    with open(path, "r") as f:
        return json.load(f)

# ------------------------------------------------------------------------------
# DB connection (simple psycopg2)
# ------------------------------------------------------------------------------
def get_db_connection_with_gcs_certs(
    dbname: str,
    user: str,
    password: str,
    host: str,
    port: int,
    bucket_name: Optional[str] = None,
    client_cert_gcs: Optional[str] = None,
    client_key_gcs: Optional[str] = None,
    server_ca_gcs: Optional[str] = None,
):
    """
    Minimal connection (no SSL certs used here). If you later want SSL,
    add sslmode/sslcert/sslkey/sslrootcert params.
    """
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    return conn

# ------------------------------------------------------------------------------
# Helpers from your pipeline
# ------------------------------------------------------------------------------
def _normalize(s: str) -> str:
    return re.sub(r"[ \t\-\_\.]+", "", str(s).strip().lower())

def _build_renamer(df: pd.DataFrame, mapping_src_to_out: Dict[str, str]) -> Dict[str, str]:
    norm_to_real = {_normalize(c): c for c in df.columns}
    renamer: Dict[str, str] = {}
    matched = missing = 0
    for src_label, out_col in mapping_src_to_out.items():
        key = _normalize(src_label)
        if key in norm_to_real:
            renamer[norm_to_real[key]] = out_col
            matched += 1
        else:
            missing += 1
            logging.warning("Source column '%s' not found; creating empty '%s'.", src_label, out_col)
    logging.info("[MAP] matched=%d missing=%d", matched, missing)
    return renamer

def extract_and_rename(df: pd.DataFrame, mapping_src_to_out: Dict[str, str]) -> pd.DataFrame:
    output_order: List[str] = list(mapping_src_to_out.values())
    renamer = _build_renamer(df, mapping_src_to_out)
    selected = df[list(renamer.keys())].rename(columns=renamer) if renamer else pd.DataFrame()
    for out_col in output_order:
        if out_col not in selected.columns:
            selected[out_col] = pd.NA
    selected = selected[output_order]
    for c in selected.columns:
        if pd.api.types.is_string_dtype(selected[c]):
            selected[c] = selected[c].astype("string").str.strip()
    return selected

def _parse_gs_uri(gs_uri: str) -> Tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError("gs_uri must start with 'gs://'")
    path = gs_uri[5:]
    bucket, sep, object_name = path.partition("/")
    if not bucket or not sep or not object_name:
        raise ValueError("Invalid gs_uri; expected 'gs://<bucket>/<object>'")
    return bucket, object_name

def _next_available_name(client: storage.Client, bucket: str, object_name: str) -> str:
    bkt = client.bucket(bucket)
    if not bkt.blob(object_name).exists(client=client):
        return object_name
    if "/" in object_name:
        dir_, file_ = object_name.rsplit("/", 1)
        prefix = dir_ + "/"
    else:
        prefix, file_ = "", object_name
    if "." in file_:
        stem, ext = file_.rsplit(".", 1)
        ext = "." + ext
    else:
        stem, ext = file_, ""
    i = 1
    while True:
        candidate = f"{prefix}{stem}_{i:03d}{ext}"
        if not bkt.blob(candidate).exists(client=client):
            return candidate
        i += 1

def _autosize_and_freeze_openpyxl(writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str) -> None:
    ws = writer.sheets[sheet_name]
    from openpyxl.utils import get_column_letter
    for idx, col in enumerate(df.columns, start=1):
        s = df[col].astype("string")
        max_cell = int(s.map(lambda x: len(str(x)) if pd.notna(x) else 0).max()) if len(s) else 0
        max_len = max(len(str(col)), max_cell)
        ws.column_dimensions[get_column_letter(idx)].width = min(max_len + 2, 60)
    ws.freeze_panes = "A2"

def write_df_to_gcs(
    df: pd.DataFrame,
    *,
    gs_uri: Optional[str] = None,
    bucket: Optional[str] = None,
    object_name: Optional[str] = None,
    fmt: str = "xlsx",
    sheet_name: str = "Sheet1",
    auto_increment: bool = False,
) -> str:
    client = storage.Client()
    if gs_uri:
        bucket_name, obj_name = _parse_gs_uri(gs_uri)
    else:
        if not bucket or not object_name:
            raise ValueError("Provide either gs_uri OR (bucket AND object_name).")
        bucket_name, obj_name = bucket, object_name
    if auto_increment:
        obj_name = _next_available_name(client, bucket_name, obj_name)

    blob = client.bucket(bucket_name).blob(obj_name)
    fmt = fmt.lower()

    if fmt == "csv":
        payload = df.to_csv(index=False)
        blob.upload_from_string(payload, content_type="text/csv")
    elif fmt == "xlsx":
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
            _autosize_and_freeze_openpyxl(writer, df, sheet_name)
        bio.seek(0)
        blob.upload_from_file(
            bio,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        raise ValueError("fmt must be 'csv' or 'xlsx'")

    out_uri = f"gs://{bucket_name}/{obj_name}"
    logging.info("[OUT] %s -> %s", fmt.upper(), out_uri)
    return out_uri

# ------------------------------------------------------------------------------
# Core pipeline
# ------------------------------------------------------------------------------
def run_pipeline_from_db(
    *,
    sql: str,
    db_cfg: dict,
    gcs_cfg: dict,
    out_gs_uri: Optional[str] = None,
    out_bucket: Optional[str] = None,
    out_object_name: Optional[str] = None,
    out_fmt: str = DEFAULT_OUTPUT_FMT,
    out_sheet_name: str = DEFAULT_OUTPUT_SHEET,
    out_auto_increment: bool = True,
) -> Tuple[pd.DataFrame, str]:
    """
    - Connects to Cloud SQL (Postgres)
    - Executes SQL into pandas DataFrame
    - Applies mapping/renaming
    - Writes result to GCS
    - Returns (df_out, written_gs_uri)
    """
    logging.info("Connecting to Cloud SQL and running query...")
    conn = get_db_connection_with_gcs_certs(
        dbname=db_cfg["dbname"],
        user=db_cfg["user"],
        password=db_cfg["password"],
        host=db_cfg["host"],
        port=int(db_cfg.get("port", 5432)),
        bucket_name=gcs_cfg.get("bucket_name"),
        client_cert_gcs=gcs_cfg.get("client_cert"),
        client_key_gcs=gcs_cfg.get("client_key"),
        server_ca_gcs=gcs_cfg.get("server_ca"),
    )
    try:
        df_src = pd.read_sql_query(sql, con=conn)
        logging.info("Query returned rows=%d cols=%d", len(df_src), len(df_src.columns))
    finally:
        conn.close()

    df_out = extract_and_rename(df_src, MAPPING)

    written_uri = write_df_to_gcs(
        df_out,
        gs_uri=out_gs_uri,
        bucket=out_bucket,
        object_name=out_object_name,
        fmt=out_fmt,
        sheet_name=out_sheet_name,
        auto_increment=out_auto_increment,
    )
    return df_out, written_uri

# ------------------------------------------------------------------------------
# Entrypoint to call from Airflow PythonOperator
# ------------------------------------------------------------------------------
def main(**context):
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
        force=True,
    )

    cfg = load_config()
    db_cfg = cfg["database"]
    gcs_cfg = cfg["gcs"]

    # SQL to run: prefer config value if provided
    sql = cfg.get("sql_query", DEFAULT_SQL)

    # Where to write in GCS:
    # Option A: use a fully qualified gs_uri from config, e.g. "gs://<bucket>/reports/output.xlsx"
    out_gs_uri = cfg.get("output_gs_uri")

    # Option B: or supply bucket + object_name (e.g., keep outputs under a folder with timestamp)
    out_bucket = None
    out_object = None
    if not out_gs_uri:
        # If not provided, fall back to your config bucket + a default object
        out_bucket = gcs_cfg["bucket_name"]
        # Example: write under /reports/ with timestamp
        ts = context.get("ts_nodash") if context else None
        ts = ts or pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%S")
        ext = "xlsx" if DEFAULT_OUTPUT_FMT.lower() == "xlsx" else "csv"
        out_object = f"reports/prvrostercnf_file_stats_{ts}.{ext}"

    df_out, written_uri = run_pipeline_from_db(
        sql=sql,
        db_cfg=db_cfg,
        gcs_cfg=gcs_cfg,
        out_gs_uri=out_gs_uri,
        out_bucket=out_bucket,
        out_object_name=out_object,
        out_fmt=cfg.get("output_format", DEFAULT_OUTPUT_FMT),
        out_sheet_name=cfg.get("output_sheet", DEFAULT_OUTPUT_SHEET),
        out_auto_increment=cfg.get("output_auto_increment", True),
    )

    logging.info("DONE. Rows written: %d -> %s", len(df_out), written_uri)
    # Return path for downstream tasks if needed
    return written_uri

# Allow local run for testing
if __name__ == "__main__":
    print(main())
*************************************************************************************
#db_config.json
{
  "database": {
    "dbname": "pdigpgsd1_db",
    "user": "pdigpgsd1_nh_user",
    "password": "pdigpgsd1_c7#6H_a8wd",
    "host": "10.247.163.124",
    "port": 5432
  },
  "gcs": {
    "bucket_name": "usmedphcb-pdi-intake-devstg",
    "client_cert": "prv_rstrcnf_conformed_files/cloudsql_instance/client-cert.pem",
    "client_key": "prv_rstrcnf_conformed_files/cloudsql_instance/client-key.pem",
    "server_ca": "prv_rstrcnf_conformed_files/cloudsql_instance/server-ca.pem"
  },
  "sql_query": "SELECT * FROM pdipp.prvrostercnf_conformed_file_stats",
  "output_format": "xlsx",
  "output_sheet": "Sheet1",
  "output_auto_increment": true,
  "output_gs_uri": "gs://usmedphcb-pdi-intake-devstg/reports/prvrostercnf_file_stats.xlsx"
}
****************************************
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Path where the script lives in /dags
SCRIPT_PATH = "/home/airflow/gcs/dags/pdi-ingestion-gcp/Dev/scripts/python_scripts/fetch_transform_to_gcs.py"

with DAG(
    "fetch_transform_to_gcs",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    run_job = PythonOperator(
        task_id="run_fetch_transform",
        python_callable=lambda: exec(open(SCRIPT_PATH).read(), {}),
        provide_context=True,
    )

