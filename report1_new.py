def strip_tz_from_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of df with all timezone-aware datetimes converted to tz-naive."""
    out = df.copy()

    # 1) Handle datetime64[ns, tz] dtypes
    for col in out.select_dtypes(include=["datetimetz"]).columns:
        out[col] = out[col].dt.tz_convert("UTC").dt.tz_localize(None)

    # 2) Handle object columns that may contain tz-aware datetime objects
    for col in out.columns[out.dtypes.eq("object")]:
        mask = out[col].map(lambda x: getattr(getattr(x, "tzinfo", None), "utcoffset", None) is not None)
        if mask.any():
            tmp = pd.to_datetime(out.loc[mask, col], errors="coerce", utc=True)
            out.loc[mask, col] = tmp.dt.tz_localize(None)

    return out

*********************
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

LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format="%(levelname)s %(message)s")
CONFIG_PATH = "gs://us-east4-cmp-dev-pdi-ink-05-5e6953c0-bucket/dags/pdi-ingestion-gcp/Dev/config/db_config1.json"
DEFAULT_OUTPUT_FMT = "xlsx"
DEFAULT_OUTPUT_SHEET = "Sheet1"

# --- helpers ---
def parse_gs_uri(gs_uri: str) -> Tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError("gs_uri must start with gs://")
    bucket, _, obj = gs_uri[5:].partition("/")
    if not bucket or not obj:
        raise ValueError("Invalid gs:// URI")
    return bucket, obj

def next_available_name(client: storage.Client, bucket: str, obj: str) -> str:
    m = re.match(r"^(.*?)(\.[^.]*)?$", obj)
    stem, ext = (m.group(1), m.group(2) or "")
    i = 1
    b = client.bucket(bucket)
    while b.blob(obj).exists():
        obj = f"{stem}_{i:03d}{ext}"
        i += 1
    return obj

def suffix_path(obj: str, suffix: str) -> str:
    """Insert suffix before extension: a/b/c.xlsx -> a/b/c_suffix.xlsx"""
    m = re.match(r"^(.*?)(\.[^.]*)?$", obj)
    stem, ext = (m.group(1), m.group(2) or "")
    return f"{stem}{suffix}{ext}"

# --- config & secrets ---
def load_config(ref: Union[str, Path] = CONFIG_PATH) -> dict:
    ref = str(ref)
    if ref.startswith("gs://"):
        bucket_name, blob_path = ref[5:].split("/", 1)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        text = bucket.blob(blob_path).download_as_text()
        return json.loads(text)
    else:
        with open(ref, "r") as f:
            return json.load(f)

def get_secret_value(project_id: str, secret_name: str, creds, version: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient(credentials=creds)
    name = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
    logging.info("Fetching secret: %s", secret_name)
    resp = client.access_secret_version(name=name)
    return resp.payload.data.decode("utf-8")

def get_db_credentials(project_id: str, creds) -> dict:
    return {
        "user": get_secret_value(project_id, "pdi_prvrstrcnf_cloud_sql_user", creds),
        "password": get_secret_value(project_id, "pdi_prvrstrcnf_cloud_sql_password", creds),
        "host": get_secret_value(project_id, "pdi_prvrstrcnf_cloud_sql_host_ip", creds),
    }

# --- DB connection ---
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
    # Get credentials using the default authentication
    creds, project_id = gauth_default()
    sm = get_db_credentials(project_id, creds)
    
    """
    Minimal connection (no SSL certs used here).
    """
    conn = psycopg2.connect(
        dbname=dbname,
        user=sm["user"],
        password=sm["password"],
        host=sm["host"],
        port=port,
    )
    return conn

# --- transformations ---
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

def _autosize_and_freeze_openpyxl(writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str = DEFAULT_OUTPUT_SHEET):
    from openpyxl.utils import get_column_letter
    ws = writer.sheets[sheet_name]
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
        bucket_name, obj_name = parse_gs_uri(gs_uri)
    else:
        if not bucket or not object_name:
            raise ValueError("Provide either gs_uri OR (bucket AND object_name).")
        bucket_name, obj_name = bucket, object_name

    if auto_increment:
        obj_name = next_available_name(client, bucket_name, obj_name)

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

# --- core runner ---
def run_query_to_df(*, sql: str, db_cfg: dict, gcs_cfg: dict, mapping: Dict[str, str]) -> pd.DataFrame:
    """Executes SQL -> DataFrame, applies mapping (and File Name derivation) in-memory, no GCS write."""
    creds, project_id = gauth_default()
    sm = get_db_credentials(project_id, creds)

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
    finally:
        conn.close()

    return _apply_transformations(df_src, mapping)

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

def run_both_queries_from_config(config_path: Union[str, Path] = CONFIG_PATH):
    cfg = load_config(config_path)

    db_cfg = cfg["database"]
    gcs_cfg = cfg.get("gcs", {})
    mapping_q1 = cfg.get("mapping", MAPPING)
    mapping_q2 = cfg.get("mapping_q2", {})  # usually empty for codes table

    outs = cfg.get("outputs", {}) or {}
    out_cfg_q1 = outs.get("q1", {})  # optional
    out_cfg_q2 = outs.get("q2", {})  # optional
    out_cfg_q1_enriched = outs.get("q1_enriched", {})  # recommended

    # --- 1) Run both queries INTO MEMORY (no writes yet) ---
    df1 = run_query_to_df(sql=cfg["sql_query1"], db_cfg=db_cfg, gcs_cfg=gcs_cfg, mapping=mapping_q1)
    df2 = run_query_to_df(sql=cfg["sql_query2"], db_cfg=db_cfg, gcs_cfg=gcs_cfg, mapping=mapping_q2)

    # --- 2) Build code -> description map and enrich df1 ---
    codes_to_desc = _codes_to_descriptions_builder(df2)
    df1_enriched = df1.copy()
    if "Error Code" in df1_enriched.columns:
        df1_enriched["Error Descriptions"] = df1_enriched["Error Code"].apply(codes_to_desc)
    else:
        logging.warning("Column 'Error Code' not found in Q1 output; skipping enrichment.")
        df1_enriched["Error Descriptions"] = ""

    # --- 3) Write only what you want ---
    uris = {}
    
    # (optional) write raw Q1
    if out_cfg_q1:
        uris["q1_uri"] = write_df_to_gcs(
            df1,
            gs_uri=out_cfg_q1.get("gs_uri"),
            bucket=out_cfg_q1.get("bucket"),
            object_name=out_cfg_q1.get("object_name"),
            fmt=out_cfg_q1.get("fmt", DEFAULT_OUTPUT_FMT),
            sheet_name=out_cfg_q1.get("sheet_name", "Query1"),
            auto_increment=out_cfg_q1.get("auto_increment", True),
        )

    # (optional) write Q2 reference table
    if out_cfg_q2:
        uris["q2_uri"] = write_df_to_gcs(
            df2,
            gs_uri=out_cfg_q2.get("gs_uri"),
            bucket=out_cfg_q2.get("bucket"),
            object_name=out_cfg_q2.get("object_name"),
            fmt=out_cfg_q2.get("fmt", DEFAULT_OUTPUT_FMT),
            sheet_name=out_cfg_q2.get("sheet_name", "Query2"),
            auto_increment=out_cfg_q2.get("auto_increment", True),
        )

    # (recommended) write enriched Q1
    if out_cfg_q1_enriched:
        uris["q1_enriched_uri"] = write_df_to_gcs(
            df1_enriched,
            gs_uri=out_cfg_q1_enriched.get("gs_uri"),
            bucket=out_cfg_q1_enriched.get("bucket"),
            object_name=out_cfg_q1_enriched.get("object_name"),
            fmt=out_cfg_q1_enriched.get("fmt", DEFAULT_OUTPUT_FMT),
            sheet_name=out_cfg_q1_enriched.get("sheet_name", "Query1_Enriched"),
            auto_increment=out_cfg_q1_enriched.get("auto_increment", True),
        )
    else:
        # If no explicit target for enriched, but q1 destination exists, mirror it with `enriched`
        if out_cfg_q1 and out_cfg_q1.get("gs_uri"):
            bkt, obj = parse_gs_uri(out_cfg_q1["gs_uri"])
            uris["q1_enriched_uri"] = write_df_to_gcs(
                df1_enriched,
                bucket=bkt,
                object_name=suffix_path(obj, "_enriched"),
                fmt=out_cfg_q1.get("fmt", DEFAULT_OUTPUT_FMT),
                sheet_name="Query1_Enriched",
                auto_increment=True,
            )
        elif out_cfg_q1 and out_cfg_q1.get("bucket") and out_cfg_q1.get("object_name"):
            uris["q1_enriched_uri"] = write_df_to_gcs(
                df1_enriched,
                bucket=out_cfg_q1["bucket"],
                object_name=suffix_path(out_cfg_q1["object_name"], "_enriched"),
                fmt=out_cfg_q1.get("fmt", DEFAULT_OUTPUT_FMT),
                sheet_name="Query1_Enriched",
                auto_increment=True,
            )
        else:
            # no destination provided at all; help in-memory only.
            logging.info("No outputs.q1_enriched and no outputs.q1 destination; enriched result kept in-memory only.")

    return uris

# --- main ---
if __name__ == "__main__":
    uris = run_both_queries_from_config(CONFIG_PATH)
    print(uris)
