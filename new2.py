from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def gcs_smoke_test():
    from google.cloud import storage
    client = storage.Client(project="<YOUR_PROJECT_ID>")

    bucket = client.bucket("<BUCKET>")
    obj = "dags/pdi-ingestion-gcp/Dev/config/db_config.json"

    # Test read
    assert bucket.blob(obj).exists(client=client), f"Config object not found: {obj}"
    print("✅ Can read config OK")

    # Test write
    out = bucket.blob("reports/_smoke_test.txt")
    out.upload_from_string("hello from Composer")
    print("✅ Can write to reports/ OK")


with DAG(
    dag_id="gcs_smoke_test_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # only run manually
    catchup=False,
    tags=["debug"],
) as dag:

    test_task = PythonOperator(
        task_id="gcs_smoke_test",
        python_callable=gcs_smoke_test,
    )
*******************************************************








from __future__ import annotations

import io
import json
import logging
import re
from pathlib import Path
from typing import Dict, Optional, Tuple, Union, List

import pandas as pd
import psycopg2
from google.cloud import storage
from google.cloud import secretmanager  # keep for future SSL/secret use


# ── CONFIG ────────────────────────────────────────────────────────────────────
# CONFIG_PATH can be local or GCS (gs://...)
CONFIG_PATH = (
    "gs://us-east4-cmp-dev-pdi-ink-05-e6953c0-bucket/"
    "dags/pdi-ingestion-gcp/Dev/config/db_config.json"
)
DEFAULT_SQL = "SELECT * FROM pdipp.prvrostercnf_conformed_file_stats"
DEFAULT_OUTPUT_FMT = "xlsx"           # or "csv"
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


# ── Config loader ─────────────────────────────────────────────────────────────
def load_config(ref: Union[str, Path] = CONFIG_PATH) -> dict:
    ref = str(ref)
    if ref.startswith("gs://"):
        bucket_name, blob_path = ref[5:].split("/", 1)
        client = storage.Client()
        text = client.bucket(bucket_name).blob(blob_path).download_as_text()
        return json.loads(text)
    with open(Path(ref), "r") as f:
        return json.load(f)


# ── DB connect (no SSL yet) ───────────────────────────────────────────────────
def db_connection_with_gcs_certs(
    dbname: str,
    user: str,
    password: str,
    host: str,
    port: int,
    bucket_name: Optional[str] = None,
    client_cert_gcs: Optional[str] = None,
    client_key_gcs: Optional[str] = None,
    server_ca_gcs: Optional[str] = None,
) -> psycopg2.extensions.connection:
    # Add ssl params later if needed
    return psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)


# ── Transform helpers ─────────────────────────────────────────────────────────
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
            logging.warning("Source column not found: creating empty '%s' -> '%s'", src_label, out_col)
    return renamer

def extract_and_rename(
    df: pd.DataFrame,
    mapping_src_to_out: Dict[str, str],
    output_order: Optional[List[str]] = None,
) -> pd.DataFrame:
    renamer = build_renamer(df, mapping_src_to_out)
    out = df[list(renamer.keys())].rename(columns=renamer) if renamer else pd.DataFrame()
    if output_order:
        for col in output_order:
            if col not in out.columns:
                out[col] = pd.NA
        out = out[output_order]
    for c in out.columns:
        if pd.api.types.is_string_dtype(out[c]):
            out[c] = out[c].astype("string")
    return out

def parse_gs_uri(gs_uri: str) -> Tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gs_uri}")
    path = gs_uri[5:]
    sep = path.find("/")
    if sep == -1:
        raise ValueError(f"Invalid GCS URI: {gs_uri}")
    bucket, object_name = path[:sep], path[sep + 1:]
    if not bucket or not object_name:
        raise ValueError(f"Invalid GCS URI: {gs_uri}")
    return bucket, object_name


# ── GCS helpers & Excel formatting ────────────────────────────────────────────
def next_available_name(client: storage.Client, bucket: str, object_name: str) -> str:
    bkt = client.bucket(bucket)
    if not bkt.blob(object_name).exists(client=client):
        return object_name
    if "/" in object_name:
        dir_, file_ = object_name.rsplit("/", 1); prefix = dir_ + "/"
    else:
        prefix, file_ = "", object_name
    if "." in file_:
        stem, ext = file_.rsplit(".", 1); ext = "." + ext
    else:
        stem, ext = file_, ""
    i = 1
    while True:
        candidate = f"{prefix}{stem}_{i:03d}{ext}"
        if not bkt.blob(candidate).exists(client=client):
            return candidate
        i += 1

def autosize_and_freeze(writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str) -> None:
    from openpyxl.utils import get_column_letter
    ws = writer.sheets[sheet_name]
    df = df.astype("string")
    for idx, col in enumerate(df.columns, 1):
        max_len = max([len(str(x)) if pd.notna(x) else 0 for x in df[col]] + [len(str(col))])
        ws.column_dimensions[get_column_letter(idx)].width = min(max_len + 2, 60)
    ws.freeze_panes = "A2"


# ── Write DataFrame to GCS ────────────────────────────────────────────────────
def write_df_to_gcs(
    df: pd.DataFrame,
    gs_uri: Optional[str] = None,
    bucket: Optional[str] = None,
    object_name: Optional[str] = None,
    fmt: str = "xlsx",
    sheet_name: str = "Sheet1",
    auto_increment: bool = False,
) -> str:
    client = storage.Client()
    if gs_uri:
        bucket_name, obj_name = parse_gs_uri(gs_uri)
    elif bucket and object_name:
        bucket_name, obj_name = bucket, object_name
    else:
        raise ValueError("Must pass either gs_uri OR (bucket and object_name)")
    if auto_increment:
        obj_name = next_available_name(client, bucket_name, obj_name)

    blob = client.bucket(bucket_name).blob(obj_name)
    fmt = fmt.lower()
    if fmt == "csv":
        blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")
    elif fmt == "xlsx":
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)
            autosize_and_freeze(writer, df, sheet_name)
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


# ── Pipeline ──────────────────────────────────────────────────────────────────
def run_pipeline_from_db(
    *,
    sql: str,
    db_cfg: dict,
    gcs_cfg: dict,
    out_gs_uri: Optional[str] = None,
    out_bucket: Optional[str] = None,
    out_object_name: Optional[str] = None,
    out_fmt: str = "xlsx",
    out_sheet_name: str = "Sheet1",
    out_auto_increment: bool = True,
) -> tuple[pd.DataFrame, str]:
    logging.info("Connecting to Cloud SQL and running query...")
    conn = db_connection_with_gcs_certs(
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
        df_src = pd.read_sql_query(sql, conn)
        logging.info("Query returned rows=%d cols=%d", len(df_src), len(df_src.columns))
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
    finally:
        conn.close()
    return df_out, written_uri


# ── Entry point to call from Airflow (no context injection needed) ────────────
def run(ts_nodash: str | None = None) -> str:
    """
    Simple entrypoint for PythonOperator.
    Pass `ts_nodash` from Airflow as op_kwargs if you want timestamped filenames.
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s", force=True)
    cfg = load_config()
    db_cfg = cfg["database"]
    gcs_cfg = cfg["gcs"]

    sql = cfg.get("sql_query", DEFAULT_SQL)
    out_gs_uri = cfg.get("output_gs_uri")
    out_bucket = None
    out_object = None

    if not out_gs_uri:
        out_bucket = gcs_cfg["bucket_name"]
        ts = ts_nodash or pd.Timestamp.utcnow().strftime("%Y%m%dT%H%M%S")
        ext = "xlsx" if cfg.get("output_format", DEFAULT_OUTPUT_FMT).lower() == "xlsx" else "csv"
        out_object = f"reports/prvrostercnf_file_stats-{ts}.{ext}"

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
    return written_uri
******************
    # dags/fetch_transform_to_gcs_dag.py
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Make sure Python can import from dags/python_scripts
sys.path.append("/home/airflow/gcs/dags/python_scripts")

from fetch_transform_to_gcs import run  # IMPORTANT: no parentheses here

with DAG(
    dag_id="fetch_transform_to_gcs",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    fetch_transform = PythonOperator(
        task_id="fetch_transform",
        python_callable=run,                       # not run()
        op_kwargs={"ts_nodash": "{{ ts_nodash }}"},  # pass what we need explicitly
    )

