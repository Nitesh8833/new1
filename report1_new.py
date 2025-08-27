# run_two_queries_to_gcs.py  (with error-code enrichment)
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
from google.cloud import secretmanager
from google.auth import default as gauth_default
from collections import OrderedDict

# -------------------------- logging & defaults --------------------------

LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format="%(levelname)s %(message)s")

CONFIG_PATH = "gs://YOUR_BUCKET/dags/pdi-ingestion-gcp/Dev/config/db_config.json"

DEFAULT_OUTPUT_FMT   = "xlsx"
DEFAULT_OUTPUT_SHEET = "Sheet1"

# -------------------------- helpers --------------------------

def _parse_gs_uri(gs_uri: str) -> Tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError("gs_uri must start with gs://")
    bucket, _, obj = gs_uri[5:].partition("/")
    if not bucket or not obj:
        raise ValueError("Invalid gs:// URI")
    return bucket, obj

def _next_available_name(client: storage.Client, bucket: str, obj: str) -> str:
    m = re.match(r"^(.*?)(\.[^./\\]+)?$", obj)
    stem, ext = (m.group(1), m.group(2) or "")
    i = 1
    b = client.bucket(bucket)
    while b.blob(obj).exists(client):
        obj = f"{stem}_{i:03d}{ext}"
        i += 1
    return obj

def _suffix_path(obj: str, suffix: str) -> str:
    """Insert suffix before extension: a/b/c.xlsx -> a/b/c_suffix.xlsx"""
    m = re.match(r"^(.*?)(\.[^./\\]+)?$", obj)
    stem, ext = (m.group(1), m.group(2) or "")
    return f"{stem}{suffix}{ext or ''}"

# -------------------------- config & secrets --------------------------

def load_config(ref: Union[str, Path] = CONFIG_PATH) -> dict:
    ref = str(ref)
    if ref.startswith("gs://"):
        bucket_name, blob_path = _parse_gs_uri(ref)
        client = storage.Client()
        txt = client.bucket(bucket_name).blob(blob_path).download_as_text()
        return json.loads(txt)
    with open(ref, "r") as f:
        return json.load(f)

def _secret_fullname(project_id: str, secret_name_or_full: str, version: str) -> str:
    if "/secrets/" in secret_name_or_full:
        if "/versions/" in secret_name_or_full:
            return secret_name_or_full
        return f"{secret_name_or_full}/versions/{version}"
    return f"projects/{project_id}/secrets/{secret_name_or_full}/versions/{version}"

def get_secret_value(project_id: str, secret_name: str, creds, version: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient(credentials=creds)
    name = _secret_fullname(project_id, secret_name, version)
    logging.info("Fetching secret: %s", name)
    resp = client.access_secret_version(name=name)
    return resp.payload.data.decode("utf-8")

def get_db_credentials(project_id: str, creds, secrets_cfg: dict) -> dict:
    return {
        "user":     get_secret_value(project_id, secrets_cfg["user"], creds),
        "password": get_secret_value(project_id, secrets_cfg["password"], creds),
        "host":     get_secret_value(project_id, secrets_cfg["host"], creds),
    }

# -------------------------- DB connection --------------------------

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
    # Minimal (non-SSL). Extend if you need SSL certs from GCS.
    return psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

# -------------------------- transformations --------------------------

MAPPING: Dict[str, str] = {
    "business_owner": "Medicare_Commercial",
    "group_type": "Group Team",
    "roster_id": "Roster ID",
    "roster_name": "Provider Entity",
    "parent_transaction_type": "Parent Transaction Type",
    "case_number": "Case Number",
    "transaction_type": "Transaction Type",
    "input_rec_count": "Total Number Of Rows",
    "total_rows_with_errors": "Total Rows With Errors",
    "critical_error_codes": "Error Code",
    "error_details": "Error Description",
}

def _apply_transformations(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    df = df.copy()
    if "file_path" in df.columns and "File Name" not in mapping.values():
        df["File Name"] = df["file_path"].astype(str).str.extract(r"([^/\\]+)$")
    present_map = {src: dst for src, dst in mapping.items() if src in df.columns}
    df = df.rename(columns=present_map)
    ordered = [present_map.get(k, None) for k in mapping.keys() if k in df.columns]
    ordered += [c for c in df.columns if c not in ordered]
    df = df[ordered]
    return df

# -------------------------- GCS writers --------------------------

def _autosize_and_freeze_openpyxl(writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str = DEFAULT_OUTPUT_SHEET):
    from openpyxl.utils import get_column_letter
    ws = writer.book[sheet_name]
    ws.freeze_panes = "A2"
    for idx, col in enumerate(df.columns, 1):
        try:
            max_len = max([len(str(x)) for x in df[col].astype(str).values] + [len(col)]) + 2
        except Exception:
            max_len = len(col) + 2
        ws.column_dimensions[get_column_letter(idx)].width = min(max_len, 80)

def write_df_to_gcs(
    df: pd.DataFrame,
    *,
    gs_uri: Optional[str] = None,
    bucket: Optional[str] = None,
    object_name: Optional[str] = None,
    fmt: str = "xlsx",
    sheet_name: str = DEFAULT_OUTPUT_SHEET,
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
    logging.info("[OUT %s -> %s]", fmt.upper(), out_uri)
    return out_uri

# -------------------------- core runner --------------------------

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
    mapping: Dict[str, str] = MAPPING,
) -> Tuple[pd.DataFrame, str]:
    logging.info("Connecting to Cloud SQL and running query...")
    creds, project_id = gauth_default()
    sm = get_db_credentials(project_id, creds, db_cfg["secrets"])

    conn = get_db_connection_with_gcs_certs(
        dbname=db_cfg["dbname"],
        user=sm["user"],
        password=sm["password"],
        host=sm["host"],
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

    df_out = _apply_transformations(df_src, mapping)

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

# -------------------------- enrichment (Q1 + Q2) --------------------------

def _codes_to_descriptions_builder(error_codes_df: pd.DataFrame):
    """Return a function codes_str -> 'desc1, desc2' using codes map from df."""
    code_to_desc = error_codes_df.set_index("error_code")["description"].to_dict()

    def codes_to_descriptions(codes_str: str) -> str:
        if pd.isna(codes_str) or not str(codes_str).strip():
            return ""
        seen = OrderedDict()
        for raw in str(codes_str).split(","):
            code = raw.strip()
            if code and code in code_to_desc:
                seen[code_to_desc[code]] = None
        return ", ".join(seen.keys())

    return codes_to_descriptions

# -------------------------- multi-query coordinator --------------------------

def run_both_queries_from_config(config_path: Union[str, Path] = CONFIG_PATH):
    cfg = load_config(config_path)

    db_cfg  = cfg["db"]
    gcs_cfg = cfg.get("gcs", {})
    mapping = cfg.get("mapping", MAPPING)

    outs = cfg.get("outputs", {}) or {}
    out_cfg_q1 = outs.get("q1", {})
    out_cfg_q2 = outs.get("q2", {})
    out_cfg_q1_enriched = outs.get("q1_enriched", {})  # optional

    # -------- Query 1 (roster/stat rows) --------
    df1, uri1 = run_pipeline_from_db(
        sql=cfg["sql_query1"],
        db_cfg=db_cfg,
        gcs_cfg=gcs_cfg,
        out_gs_uri=out_cfg_q1.get("gs_uri"),
        out_bucket=out_cfg_q1.get("bucket"),
        out_object_name=out_cfg_q1.get("object_name"),
        out_fmt=out_cfg_q1.get("fmt", DEFAULT_OUTPUT_FMT),
        out_sheet_name=out_cfg_q1.get("sheet_name", "Query1"),
        out_auto_increment=out_cfg_q1.get("auto_increment", True),
        mapping=mapping,
    )
    logging.info("Q1 written to %s", uri1)

    # -------- Query 2 (error code reference) --------
    df2, uri2 = run_pipeline_from_db(
        sql=cfg["sql_query2"],
        db_cfg=db_cfg,
        gcs_cfg=gcs_cfg,
        out_gs_uri=out_cfg_q2.get("gs_uri"),
        out_bucket=out_cfg_q2.get("bucket"),
        out_object_name=out_cfg_q2.get("object_name"),
        out_fmt=out_cfg_q2.get("fmt", DEFAULT_OUTPUT_FMT),
        out_sheet_name=out_cfg_q2.get("sheet_name", "Query2"),
        out_auto_increment=out_cfg_q2.get("auto_increment", True),
        mapping=cfg.get("mapping_q2", {}),  # usually no renames needed for codes table
    )
    logging.info("Q2 written to %s", uri2)

    # -------- Enrich Q1 with "Error Descriptions" using Q2 --------
    # Build converter and add the new column on a copy of df1
    codes_to_desc = _codes_to_descriptions_builder(df2)
    df1_enriched = df1.copy()
    if "Error Code" in df1_enriched.columns:
        df1_enriched["Error Descriptions"] = df1_enriched["Error Code"].apply(codes_to_desc)
    else:
        logging.warning("Column 'Error Code' not found in Q1 output; skipping enrichment.")
        df1_enriched["Error Descriptions"] = ""

    # Destination for enriched file:
    if out_cfg_q1_enriched:
        enriched_uri = write_df_to_gcs(
            df1_enriched,
            gs_uri=out_cfg_q1_enriched.get("gs_uri"),
            bucket=out_cfg_q1_enriched.get("bucket"),
            object_name=out_cfg_q1_enriched.get("object_name"),
            fmt=out_cfg_q1_enriched.get("fmt", DEFAULT_OUTPUT_FMT),
            sheet_name=out_cfg_q1_enriched.get("sheet_name", "Query1_Enriched"),
            auto_increment=out_cfg_q1_enriched.get("auto_increment", True),
        )
    else:
        # If no explicit target given, mirror q1’s target and add “_enriched”
        if out_cfg_q1.get("gs_uri"):
            bkt, obj = _parse_gs_uri(out_cfg_q1["gs_uri"])
            enriched_uri = write_df_to_gcs(
                df1_enriched,
                bucket=bkt,
                object_name=_suffix_path(obj, "_enriched"),
                fmt=out_cfg_q1.get("fmt", DEFAULT_OUTPUT_FMT),
                sheet_name="Query1_Enriched",
                auto_increment=True,
            )
        else:
            # If q1 used (bucket, object_name), reuse them
            bkt = out_cfg_q1.get("bucket")
            obj = out_cfg_q1.get("object_name")
            if not (bkt and obj):
                raise ValueError(
                    "Provide outputs.q1 or outputs.q1_enriched destination in config."
                )
            enriched_uri = write_df_to_gcs(
                df1_enriched,
                bucket=bkt,
                object_name=_suffix_path(obj, "_enriched"),
                fmt=out_cfg_q1.get("fmt", DEFAULT_OUTPUT_FMT),
                sheet_name="Query1_Enriched",
                auto_increment=True,
            )

    logging.info("Q1 (enriched) written to %s", enriched_uri)

    return {"q1_uri": uri1, "q2_uri": uri2, "q1_enriched_uri": enriched_uri}

# -------------------------- optional: single Excel with two sheets --------------------------

def write_two_sheets_one_file(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    *,
    sheet1: str = "Query1",
    sheet2: str = "Query2",
    gs_uri: Optional[str] = None,
    bucket: Optional[str] = None,
    object_name: Optional[str] = None,
    auto_increment: bool = True,
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

    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df1.to_excel(writer, index=False, sheet_name=sheet1)
        _autosize_and_freeze_openpyxl(writer, df1, sheet1)
        df2.to_excel(writer, index=False, sheet_name=sheet2)
        _autosize_and_freeze_openpyxl(writer, df2, sheet2)
    bio.seek(0)
    storage.Client().bucket(bucket_name).blob(obj_name).upload_from_file(
        bio,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    out_uri = f"gs://{bucket_name}/{obj_name}"
    logging.info("[OUT XLSX -> %s]", out_uri)
    return out_uri

# -------------------------- main --------------------------

if __name__ == "__main__":
    uris = run_both_queries_from_config(CONFIG_PATH)
    print(uris)
