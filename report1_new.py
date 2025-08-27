
if keep_unmapped:
        # keep everything, but put mapped first, then extras, then the rest
        mapped_order = [present_map[s] for s in mapping if s in present_map]
        extras = [c for c in (extra_output_cols or []) if c in out.columns and c not in mapped_order]
        rest = [c for c in out.columns if c not in mapped_order and c not in extras]
        # ensure File Name appears (if present) near the front (after extras)
        if "File Name" in out.columns and "File Name" not in mapped_order + extras:
            extras.append("File Name")
        return out[mapped_order + extras + rest]
    else:
        # SELECT-ONLY: mapped columns + extras + ALWAYS include "File Name"
        selected = [present_map[s] for s in mapping if s in present_map]

        # always include File Name if present
        if "File Name" in out.columns and "File Name" not in selected:
            selected.append("File Name")

        # include any requested extra columns (e.g., "Error Descriptions")
        if extra_output_cols:
            selected += [c for c in extra_output_cols if c in out.columns and c not in selected]

        return out[selected]
***************************************************************
from __future__ import annotations

import io
import json
import logging
import re
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple, Union

import pandas as pd
import psycopg2
from google.cloud import storage, secretmanager
from google.auth import default as gauth_default

# -------------------- logging & defaults --------------------

LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR
logging.basicConfig(level=getattr(logging, LOG_LEVEL), format="%(levelname)s %(message)s")

CONFIG_PATH = "gs://<bucket>/path/to/db_config.json"  # <-- set or pass to run_both_queries_from_config
DEFAULT_OUTPUT_FMT = "xlsx"
DEFAULT_OUTPUT_SHEET = "Sheet1"

# -------------------- helpers --------------------

def parse_gs_uri(gs_uri: str) -> Tuple[str, str]:
    if not gs_uri.startswith("gs://"):
        raise ValueError("gs_uri must start with gs://")
    bucket, _, obj = gs_uri[5:].partition("/")
    if not bucket or not obj:
        raise ValueError("Invalid gs:// URI")
    return bucket, obj

def next_available_name(client: storage.Client, bucket: str, obj: str) -> str:
    m = re.match(r"^(.*?)(\.[^.]+)?$", obj)
    stem, ext = (m.group(1), m.group(2) or "")
    i = 1
    b = client.bucket(bucket)
    while b.blob(obj).exists():
        obj = f"{stem}_{i:03d}{ext}"
        i += 1
    return obj

def suffix_path(obj: str, suffix: str) -> str:
    m = re.match(r"^(.*?)(\.[^.]+)?$", obj)
    stem, ext = (m.group(1), m.group(2) or "")
    return f"{stem}{suffix}{ext}"

# -------------------- config & secrets --------------------

def load_config(ref: Union[str, Path] = CONFIG_PATH) -> dict:
    ref = str(ref)
    if ref.startswith("gs://"):
        bucket_name, blob_path = ref[5:].split("/", 1)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        text = bucket.blob(blob_path).download_as_text()
        return json.loads(text)
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

# -------------------- DB connection --------------------

def get_db_connection_with_gcs_certs(
    dbname: str,
    user: Optional[str],
    password: Optional[str],
    host: Optional[str],
    port: int,
    bucket_name: Optional[str] = None,
    client_cert_gcs: Optional[str] = None,
    client_key_gcs: Optional[str] = None,
    server_ca_gcs: Optional[str] = None,
):
    creds, project_id = gauth_default()
    sm = get_db_credentials(project_id, creds)
    conn = psycopg2.connect(
        dbname=dbname,
        user=user or sm["user"],
        password=password or sm["password"],
        host=host or sm["host"],
        port=port,
        # If you need SSL, add sslmode/sslrootcert/sslcert/sslkey here using GCS paths above.
    )
    return conn

# -------------------- transformations --------------------

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
    "error_details": "Error Description",  # optional passthrough if present
    # add as needed...
}

def _apply_transformations(
    df: pd.DataFrame,
    mapping: dict,
    *,
    keep_unmapped: bool = False,
    extra_output_cols: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    out = df.copy()

    # Derive "File Name" if file_path exists and not already mapped
    if "file_path" in out.columns and "File Name" not in mapping.values():
        out["File Name"] = out["file_path"].astype(str).str.extract(r"([^/\\]+)$")

    # Rename only present sources
    present_map = {src: dst for src, dst in mapping.items() if src in out.columns}
    out = out.rename(columns=present_map)

    # Avoid scientific notation issues for very large IDs
    for big_id in ("roster_file_id", "conformed_file_id"):
        if big_id in out.columns:
            out[big_id] = out[big_id].astype("string")

    if keep_unmapped:
        mapped_order = [present_map[s] for s in mapping if s in present_map]
        rest = [c for c in out.columns if c not in mapped_order]
        extras = [c for c in (extra_output_cols or []) if c in out.columns and c not in mapped_order]
        rest = [c for c in rest if c not in extras]
        return out[mapped_order + extras + rest]

    # SELECT-ONLY
    selected = [present_map[s] for s in mapping if s in present_map]
    if extra_output_cols:
        selected += [c for c in extra_output_cols if c in out.columns and c not in selected]
    return out[selected]

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

def strip_tz_from_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of df with tz-aware datetimes converted to tz-naive (Excel-safe)."""
    out = df.copy()
    # tz-aware datetime64 dtypes
    for col in out.select_dtypes(include=["datetimetz"]).columns:
        out[col] = out[col].dt.tz_convert("UTC").dt.tz_localize(None)
    # object columns that may hold datetime objects
    for col in out.columns[out.dtypes.eq("object")]:
        mask = out[col].map(lambda x: getattr(getattr(x, "tzinfo", None), "utcoffset", None) is not None)
        if mask.any():
            tmp = pd.to_datetime(out.loc[mask, col], errors="coerce", utc=True)
            out.loc[mask, col] = tmp.dt.tz_localize(None)
    return out

# -------------------- write to GCS --------------------

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
        safe_df = strip_tz_from_datetime(df)
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            safe_df.to_excel(writer, index=False, sheet_name=sheet_name)
            _autosize_and_freeze_openpyxl(writer, safe_df, sheet_name)
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

# -------------------- core runners --------------------

def run_query_to_df(
    *,
    sql: str,
    db_cfg: dict,
    gcs_cfg: dict,
) -> pd.DataFrame:
    """Executes SQL -> DataFrame; no write."""
    creds, project_id = gauth_default()
    sm = get_db_credentials(project_id, creds)

    conn = get_db_connection_with_gcs_certs(
        dbname=db_cfg["dbname"],
        user=db_cfg.get("user", sm["user"]),
        password=db_cfg.get("password", sm["password"]),
        host=db_cfg.get("host", sm["host"]),
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
    return df_src

def _find_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    """Return actual column name matching any candidate (case/space-insensitive)."""
    norm = {c.lower().replace(" ", ""): c for c in df.columns}
    for cand in candidates:
        key = cand.lower().replace(" ", "")
        if key in norm:
            return norm[key]
    return None

def _codes_to_descriptions_map(df_codes: pd.DataFrame) -> Dict[str, str]:
    """Build {error_code: description} robustly."""
    err_col  = _find_col(df_codes, ["error_code", "errorcode", "code"])
    desc_col = _find_col(df_codes, ["description", "desc", "error_description", "errordescription"])
    if not (err_col and desc_col):
        logging.warning("Codes table missing columns. Present: %s", list(df_codes.columns))
        return {}
    tmp = (
        df_codes[[err_col, desc_col]]
        .dropna(subset=[err_col])
        .assign(**{
            err_col:  lambda d: d[err_col].astype(str).str.strip(),
            desc_col: lambda d: d[desc_col].astype(str).str.strip(),
        })
        .drop_duplicates(subset=[err_col])
    )
    return tmp.set_index(err_col)[desc_col].to_dict()

def run_both_queries_from_config(config_path: Union[str, Path] = CONFIG_PATH):
    cfg = load_config(config_path)

    db_cfg  = cfg["database"]
    gcs_cfg = cfg.get("gcs", {})
    mapping_q1 = cfg.get("mapping", MAPPING)

    outs = cfg.get("outputs", {}) or {}
    out_cfg_q1_enriched = outs.get("q1_enriched", {})  # ONLY this is written

    # 1) Run both queries
    df1 = run_query_to_df(sql=cfg["sql_query1"], db_cfg=db_cfg, gcs_cfg=gcs_cfg)
    df2 = run_query_to_df(sql=cfg["sql_query2"], db_cfg=db_cfg, gcs_cfg=gcs_cfg)

    # 2) Enrich df1
    code_map = _codes_to_descriptions_map(df2)
    df1["Error Descriptions"] = ""
    if code_map and "critical_error_codes" in df1.columns:
        def _map_codes(raw):
            if pd.isna(raw) or not str(raw).strip():
                return ""
            parts = [p.strip() for p in str(raw).replace(";", ",").split(",") if p.strip()]
            return ", ".join([code_map.get(p, p) for p in parts])
        df1["Error Descriptions"] = df1["critical_error_codes"].map(_map_codes)
    elif code_map and "Error Code" in df1.columns:
        def _map_codes2(raw):
            if pd.isna(raw) or not str(raw).strip():
                return ""
            parts = [p.strip() for p in str(raw).replace(";", ",").split(",") if p.strip()]
            return ", ".join([code_map.get(p, p) for p in parts])
        df1["Error Descriptions"] = df1["Error Code"].map(_map_codes2)
    else:
        logging.info("No error code column or empty codes map; enrichment left blank.")

    # 3) Select ONLY mapped columns (+ Error Descriptions)
    df1_enriched = _apply_transformations(
        df1,
        mapping_q1,
        keep_unmapped=False,                        # select-only
        extra_output_cols=["Error Descriptions"],   # force enrichment column
    )

    # 4) Write ONLY enriched output
    if out_cfg_q1_enriched.get("gs_uri") or (out_cfg_q1_enriched.get("bucket") and out_cfg_q1_enriched.get("object_name")):
        enriched_uri = write_df_to_gcs(
            df1_enriched,
            gs_uri=out_cfg_q1_enriched.get("gs_uri"),
            bucket=out_cfg_q1_enriched.get("bucket"),
            object_name=out_cfg_q1_enriched.get("object_name"),
            fmt=out_cfg_q1_enriched.get("fmt", DEFAULT_OUTPUT_FMT),
            sheet_name=out_cfg_q1_enriched.get("sheet", "Query1_Enriched"),
            auto_increment=out_cfg_q1_enriched.get("auto_increment", True),
        )
    else:
        # Mirror a base Q1 destination if present
        out_cfg_q1 = outs.get("q1", {})
        if out_cfg_q1.get("gs_uri"):
            bkt, obj = parse_gs_uri(out_cfg_q1["gs_uri"])
            gs_uri = f"gs://{bkt}/{suffix_path(obj, '_enriched')}"
        elif out_cfg_q1.get("bucket") and out_cfg_q1.get("object_name"):
            gs_uri = f"gs://{out_cfg_q1['bucket']}/{suffix_path(out_cfg_q1['object_name'], '_enriched')}"
        else:
            raise ValueError("outputs.q1_enriched not set and no outputs.q1 to mirror.")
        enriched_uri = write_df_to_gcs(
            df1_enriched,
            gs_uri=gs_uri,
            fmt=out_cfg_q1.get("fmt", DEFAULT_OUTPUT_FMT),
            sheet_name="Query1_Enriched",
            auto_increment=True,
        )

    logging.info("Enriched written: %s", enriched_uri)
    return {"q1_enriched_uri": enriched_uri, "df1_enriched": df1_enriched}

# -------------------- main --------------------

if __name__ == "__main__":
    result = run_both_queries_from_config(CONFIG_PATH)
    print("Wrote:", result["q1_enriched_uri"])
