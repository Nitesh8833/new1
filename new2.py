from __future__ import annotations

import io
import json
import logging
import re
from typing import Dict, List, Optional, Tuple

import pandas as pd
import psycopg2
from google.cloud import storage
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Embedded configuration (previously in db_config.json)
CONFIG = {
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
    "output_auto_increment": True,
    "output_gs_uri": "gs://usmedphcb-pdi-intake-devstg/reports/prvrostercnf_file_stats.xlsx"
}

# Default query and output settings
DEFAULT_SQL = "SELECT * FROM pdipp.prvrostercnf_conformed_file_stats"
DEFAULT_OUTPUT_FMT = "xlsx"
DEFAULT_OUTPUT_SHEET = "Sheet1"

# Column mapping
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

# DB connection
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
    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
    )
    return conn

# Helpers
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

def main(**context):
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
        force=True,
    )

    cfg = CONFIG
    db_cfg = cfg["database"]
    gcs_cfg = cfg["gcs"]

    sql = cfg.get("sql_query", DEFAULT_SQL)
    out_gs_uri = cfg.get("output_gs_uri")
    out_bucket = None
    out_object = None
    if not out_gs_uri:
        out_bucket = gcs_cfg["bucket_name"]
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
    return written_uri

# Define the DAG
with DAG(
    "fetch_transform_to_gcs",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    run_job = PythonOperator(
        task_id="run_fetch_transform",
        python_callable=main,
        provide_context=True,
    )
