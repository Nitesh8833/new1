import pandas as pd

def add_changed_roster_formats_count_desc(
    df: pd.DataFrame,
    owner_col: str | None = None,
    status_col: str = "header_version_status",
    version_col: str = "header_version_number",
    ts_col: str = "insert_timestamp",
    out_col: str = "# Changed Roster Formats (Count)",
) -> pd.DataFrame:
    """
    Count *all* transitions per business owner where:
      - Current row has header_version_status = NEW
      - Current row has header_version_number = 1
      - Immediately previous row (in descending timestamp order) is EXISTING
    """

    out = df.copy()

    # Find business_owner column if not specified
    if owner_col is None:
        for cand in ["business_owner", "Business Team", "owner", "Owner"]:
            if cand in out.columns:
                owner_col = cand
                break

    if owner_col is None or any(c not in out.columns for c in [owner_col, status_col, version_col, ts_col]):
        out[out_col] = 0
        return out

    # Normalize timestamp + status
    out[ts_col] = pd.to_datetime(out[ts_col], errors="coerce")
    out["_status_norm"] = out[status_col].astype(str).str.strip().str.upper()
    out["_version_num"] = pd.to_numeric(out[version_col], errors="coerce")

    # Sort within owner DESCENDING
    out = out.sort_values([owner_col, ts_col], ascending=[True, False], kind="mergesort")

    # Previous row in descending order → shift(-1)
    out["_prev_status"] = out.groupby(owner_col)["_status_norm"].shift(-1)

    # Detect changes
    change_mask = (
        (out["_status_norm"] == "NEW") &
        (out["_version_num"] == 1) &
        (out["_prev_status"] == "EXISTING")
    )

    # Count per owner
    per_owner_counts = change_mask.groupby(out[owner_col]).sum().astype(int)

    # Map results back
    out[out_col] = out[owner_col].map(per_owner_counts).fillna(0).astype(int)

    # Clean helpers
    out.drop(columns=["_status_norm", "_version_num", "_prev_status"], inplace=True)

    return out

***********************************************************
from __future__ import annotations
import io, re, json, os
from typing import Optional, Dict, List, Any, Iterable, Tuple, Union
import numpy as np
import pandas as pd
from datetime import datetime
import logging
import psycopg2
from google.cloud import storage
from google.cloud import secretmanager
from google.auth import default
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path

# ******** CONFIG ********
OUT_URI = "gs://pdf-prvrstr-stg-hcb-dev/report_stg/output_kpi_05.xlsx"
SHEET1_NAME = "summary"
SHEET2_NAME = "roster-KPIs"
CONFIG_PATH = "gs://us-east4-cmp-dev-pdi-ink-05-5e6953c0-bucket/dags/pdi-ingestion-gcp/Dev/config/db_config.json"
LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR

# Canonical -> Friendly
FRIENDLY_HEADERS: Dict[str, str] = {
    "business_owner": "Business Team",
    "group_type": "Group Type",
    "roster_id": "Roster ID",
    "roster_name": "Provider Entity",
    "parent_transaction_type": "Parent Transaction Type",
    "transaction_type": "Transaction Type",
    "case_number": "Case Number",
    "roster_date": "Roster Date",
}

SHEET1_COLS = [
    "Business Team", "Group Type", "Roster ID", "Provider Entity",
    "Parent Transaction Type", "Transaction Type", "Case Number", "Roster Date",
    "Conformance TAT", "# of rows in", "# of rows out",
    "# of unique NPI's in Input", "# of unique NPI's in Output",
]

BUSINESS_OWNER_COL = "Business Team"
SHEET2_COLS_WITH_OWNER = [
    BUSINESS_OWNER_COL,
    "# New Roster Formats",
    "# Changed Roster Formats",
    "# of Rosters with no Set up or Format Change",
    "# Complex Rosters",
    "All Rosters",
]

SEND_EMAIL: bool = True  # set True to send email
EMAIL_SENDER: str = "Nitesh.Kumar3@aetna.com"
EMAIL_TO: Union[str, Iterable[str]] = "Nitesh.Kumar3@aetna.com"
EMAIL_SUBJECT: str = "Roster Report Ready"
EMAIL_TEXT: str = "Hello,\n\nPlease find the latest roster report attached.\n\nRegards,"
EMAIL_HTML: Optional[str] = "<p>Hello,<br><br>Please find the latest <b>Roster Report</b> attached.<br><br>Regards,<br>Nitesh</p>"
SMTP_SERVER: str = "extmail.aetna.com"
SMTP_PORT: int = 25
USE_TLS: bool = False
SMTP_USERNAME: Optional[str] = None
SMTP_PASSWORD: Optional[str] = None

# Type definitions
Pathish = Union[str, Path]
MemAttach = Tuple[Union[bytes, bytearray], str, str]
PathAttach1 = Tuple[Pathish]
PathAttach2 = Tuple[Pathish, str]
Attachment = Union[Pathish, MemAttach, PathAttach1, PathAttach2]

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# Secret Manager
def get_secret_value(project_id: str, secret_name: str, creds, version: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient(credentials=creds)
    name = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
    logging.info(f'Fetching secret {secret_name}')
    resp = client.access_secret_version(name=name)
    return resp.payload.data.decode("utf-8")

def get_db_credentials(project_id: str, creds) -> dict:
    return {
        "user": get_secret_value(project_id, "pdi_prvrstrcnf_cloud_sql_user", creds),
        "password": get_secret_value(project_id, "pdi_prvrstrcnf_cloud_sql_password", creds),
        "host": get_secret_value(project_id, "pdi_prvrstrcnf_cloud_sql_host_ip", creds),
    }

def load_json(path: str) -> dict:
    """Load JSON from GCS (gs://...) or local file path."""
    if path.startswith("gs://"):
        bucket, blob_path = path[5:].split("/", 1)
        text = storage.Client().bucket(bucket).blob(blob_path).download_as_text()
        return json.loads(text)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def query_df_from_config(config_path: str) -> pd.DataFrame:
    """Connect using credentials from JSON and return SQL result as DataFrame."""
    cfg = load_json(config_path)
    sql = cfg["sql_query"]
    db = cfg["database"]
    creds, project_id = default()
    sm = get_db_credentials(project_id, creds)
    conn = psycopg2.connect(
        dbname=db["dbname"],
        user=sm["user"],
        password=sm["password"],
        host=sm["host"],
        port=int(db["port"]),
    )
    try:
        return pd.read_sql_query(sql, conn)
    finally:
        conn.close()

# ====== HELPERS ======
def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).lower())

def find_col(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    """Return a column matching any candidate (normalized exact, then substring)."""
    by_norm = {_norm(c): c for c in df.columns}
    for c in candidates:
        k = _norm(c)
        if k in by_norm:
            return by_norm[k]
    for c in candidates:
        ck = _norm(c)
        for col in df.columns:
            if ck and ck in _norm(col):
                return col
    return None

def _business_owner_col(df: pd.DataFrame) -> Optional[str]:
    return find_col(df, ["business_owner", "Business Owner", "business owner"])

def _version_status_series(df: pd.DataFrame) -> Optional[pd.Series]:
    col = find_col(df, ["version_status", "Version Status", "status"])
    if not col:
        return None
    return (
        df[col]
        .astype(str)
        .str.strip()
        .str.upper()
    )

def _to_text_series(s: pd.Series) -> pd.Series:
    """Robust text view for sizing/export; avoids pandas StringDtype pitfalls."""
    return s.astype(str).map(lambda v: "" if pd.isna(v) else str(v))

def handle_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename duplicate columns by adding _1, _2, etc. suffixes while keeping the first occurrence unchanged."""
    cols = pd.Series(df.columns)
    for dup in cols[cols.duplicated()].unique():
        dup_indices = cols[cols == dup].index
        for i, idx in enumerate(dup_indices):
            if i > 0:
                cols.iloc[idx] = f"{dup}_{i}"
    df.columns = cols
    return df

def add_case_and_roster_date(summary_df: pd.DataFrame, src_df: pd.DataFrame) -> pd.DataFrame:
    out = summary_df.copy()
    out["case_number"] = src_df["case_number"] if "case_number" in src_df.columns else pd.NA
    out["roster_date"] = src_df["roster_date"] if "roster_date" in src_df.columns else pd.NA
    return out

def make_excel_safe(df: pd.DataFrame) -> pd.DataFrame:
    """Remove tz from datetimes and replace NaT before writing to Excel."""
    out = df.copy()
    for col in out.select_dtypes(include=["datetimetz"]).columns:
        out[col] = out[col].dt.tz_localize(None)
    for col in out.columns[out.dtypes.eq("object")]:
        try:
            if out[col].apply(lambda x: hasattr(x, "tzinfo") and x.tzinfo is not None).any():
                out[col] = pd.to_datetime(out[col], errors="coerce", utc=True).dt.tz_localize(None)
        except:
            pass
    out = out.replace({pd.NaT: ""})
    return out

def autosize_and_freeze_openpyxl(writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str) -> None:
    """Auto-size columns and freeze header row, safely."""
    ws = writer.sheets[sheet_name]
    from openpyxl.utils import get_column_letter
    for idx, col in enumerate(df.columns, start=1):
        series = _to_text_series(df[col])
        max_len = max(series.map(len).max() if len(series) else 0, len(str(col)))
        ws.column_dimensions[get_column_letter(idx)].width = min(int(max_len) + 2, 60)
    ws.freeze_panes = "A2"

def add_new_roster_formats(df: pd.DataFrame) -> pd.DataFrame:
    """Count new roster formats per business owner."""
    out = df.copy()
    owner_col = _business_owner_col(out)
    if not owner_col:
        out["# New Roster Formats"] = 0
        return out
    
    required_cols = [owner_col, "version_status", "version_number"]
    if not all(col in out.columns for col in required_cols):
        out["# New Roster Formats"] = 0
        return out
    
    is_new = (
        out["version_status"].astype(str).str.upper().str.startswith("NEW") &
        (out["version_number"] == 1)
    )
    out["# New Roster Formats"] = is_new.groupby(out[owner_col]).transform("sum").astype(int)
    return out

def add_changed_roster_formats(df: pd.DataFrame) -> pd.DataFrame:
    out, owner = df.copy(), _business_owner_col(df)
    if not owner:
        out["# Changed Roster Formats"] = 0
        return out
    
    vs = _version_status_series(out)
    if vs is None:
        out["# Changed Roster Formats"] = 0
        return out
    
    prev = vs.groupby(out[owner]).shift(1)
    changed = vs.eq("ACTIVE") & prev.notna() & prev.ne("ACTIVE")
    out["# Changed Roster Formats"] = changed.groupby(out[owner]).transform("sum").astype(int)
    return out

def add_no_setup_or_format_change(df: pd.DataFrame) -> pd.DataFrame:
    out, owner = df.copy(), _business_owner_col(df)
    colname = "# of Rosters with no Set up or Format Change"
    if not owner:
        out[colname] = 0
        return out
    
    # Count unique rosters per owner
    roster_counts = out.groupby(owner).size()
    out[colname] = out[owner].map(roster_counts).fillna(0).astype(int)
    return out

def add_complex_rosters(df: pd.DataFrame) -> pd.DataFrame:
    out, owner = df.copy(), _business_owner_col(df)
    if not owner:
        out["# Complex Rosters"] = 0
        return out
    
    cx_col = find_col(out, ["complexity"])
    if not cx_col:
        out["# Complex Rosters"] = 0
        return out
    
    # Handle case where cx_col might return multiple columns
    if isinstance(out[cx_col], pd.DataFrame):
        col_data = out[cx_col].iloc[:, 0]
    else:
        col_data = out[cx_col]
    
    is_cx = col_data.astype(str).str.upper().str.contains("COMPLEX", na=False)
    out["# Complex Rosters"] = is_cx.groupby(out[owner]).transform("sum").astype(int)
    return out

def add_all_rosters(df: pd.DataFrame) -> pd.DataFrame:
    out, owner = df.copy(), _business_owner_col(df)
    if not owner:
        out["All Rosters"] = 1
        return out
    
    vc = out[owner].value_counts(dropna=False)
    out["All Rosters"] = out[owner].map(vc).astype(int)
    return out

def add_conformance_tat(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    start_col = find_col(out, ["prms_posted_timestamp"])
    end_col = find_col(out, ["file_ingestion_timestamp"])
    
    if not start_col or not end_col:
        out["Conformance TAT"] = None
        return out
    
    out[start_col] = pd.to_datetime(out[start_col], errors="coerce", utc=True)
    out[end_col] = pd.to_datetime(out[end_col], errors="coerce", utc=True)
    
    valid = out[start_col].notna() & out[end_col].notna()
    tat = pd.Series(pd.NaT, index=out.index, dtype="timedelta64[ns]")
    tat.loc[valid] = (out.loc[valid, end_col] - out.loc[valid, start_col]).dt.round("S")
    tat = tat.mask(tat < pd.Timedelta(0), pd.Timedelta(0))
    
    comps = tat.dt.components
    tat_str = (
        comps["days"].astype("Int64").astype(str).str.zfill(2) + "/" +
        comps["hours"].astype("Int64").astype(str).str.zfill(2) + "/" +
        comps["minutes"].astype("Int64").astype(str).str.zfill(2) + "/" +
        comps["seconds"].astype("Int64").astype(str).str.zfill(2)
    ).where(tat.notna(), None)
    
    out["Conformance TAT"] = tat_str
    return out

def add_rows_counts(df_src: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    col_in = find_col(df_src, ["input_rec_count"])
    col_out = find_col(df_src, ["conformed_rec_count"])
    
    if col_in and len(df_src) == len(out):
        out["# of rows in"] = df_src[col_in].values
    else:
        out["# of rows in"] = len(df_src)
    
    if col_out and len(df_src) == len(out):
        out["# of rows out"] = df_src[col_out].values
    else:
        out["# of rows out"] = len(out)
    
    return out

def add_unique_npi_counts(df_src: pd.DataFrame, df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    in_cnt = find_col(df_src, ["input_unique_npi_count"])
    out_cnt = find_col(df_src, ["conformed_unique_npi_count"])
    
    if in_cnt and len(df_src) == len(out):
        out["# of unique NPI's in Input"] = df_src[in_cnt].values
    else:
        out["# of unique NPI's in Input"] = pd.NA
    
    if out_cnt and len(df_src) == len(out):
        out["# of unique NPI's in Output"] = df_src[out_cnt].values
    else:
        out["# of unique NPI's in Output"] = pd.NA
    
    return out

def apply_friendly_headers(df: pd.DataFrame, columns_mapping: Dict[str, str]) -> pd.DataFrame:
    """Rename mapped columns and KEEP KPI columns."""
    if not isinstance(df, pd.DataFrame):
        return pd.DataFrame()
    
    df2 = df.rename(columns=columns_mapping)
    friendly_cols = [columns_mapping[c] for c in columns_mapping if c in df.columns]
    kpi_cols = [c for c in df2.columns if c.startswith("#") or c in ("Conformance TAT", "All Rosters")]
    keep_cols = [c for c in (friendly_cols + kpi_cols) if c in df2.columns]
    return df2[keep_cols]

def write_two_sheet_excel_gcs(df: pd.DataFrame, gcs_url: str) -> None:
    """Sheet1 detail, Sheet2 aggregated by Business Team, upload to GCS."""
    # Handle duplicate column names
    df = handle_duplicate_columns(df)
    
    # Sheet 1
    df1 = df.reindex(columns=[c for c in SHEET1_COLS if c in df.columns])
    
    # Sheet 2
    missing_owner = BUSINESS_OWNER_COL not in df.columns
    kpis_present = [c for c in SHEET2_COLS_WITH_OWNER if c in df.columns and c != BUSINESS_OWNER_COL]
    
    if missing_owner:
        df2 = pd.DataFrame(columns=SHEET2_COLS_WITH_OWNER)
    else:
        if kpis_present:
            df2 = (
                df[[BUSINESS_OWNER_COL] + kpis_present]
                .groupby(BUSINESS_OWNER_COL)
                .first()
                .reset_index()
                .reindex(columns=SHEET2_COLS_WITH_OWNER)
            )
        else:
            df2 = df[[BUSINESS_OWNER_COL]].drop_duplicates()
            for k in SHEET2_COLS_WITH_OWNER:
                if k != BUSINESS_OWNER_COL and k not in df2.columns:
                    df2[k] = pd.NA
            df2 = df2[SHEET2_COLS_WITH_OWNER]
    
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        if not df1.empty:
            df1.to_excel(writer, sheet_name=SHEET1_NAME, index=False)
            autosize_and_freeze_openpyxl(writer, df1, SHEET1_NAME)
        else:
            pd.DataFrame(columns=SHEET1_COLS).to_excel(writer, sheet_name=SHEET1_NAME, index=False)
        
        df2.to_excel(writer, sheet_name=SHEET2_NAME, index=False)
        autosize_and_freeze_openpyxl(writer, df2, SHEET2_NAME)
    
    buf.seek(0)
    
    if not gcs_url.startswith("gs://"):
        raise ValueError(f"gcs_url must start with gs://, got: {gcs_url}")
    
    bucket_name, blob_path = gcs_url[5:].split("/", 1)
    storage.Client().bucket(bucket_name).blob(blob_path).upload_from_file(
        buf, content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

def run_pipeline_to_gcs(
    src_df: Optional[pd.DataFrame] = None,
    out_uri: str = OUT_URI,
    config_path: Optional[str] = None,
) -> pd.DataFrame:
    """Runs the full KPI pipeline and uploads the Excel to GCS."""
    if src_df is None:
        cfg = config_path or CONFIG_PATH
        src = query_df_from_config(cfg)
    else:
        src = src_df.copy()
    
    # === KPI build sequence ===
    df = src.copy()
    df = add_new_roster_formats(df)
    df = add_changed_roster_formats(df)
    df = add_no_setup_or_format_change(df)
    df = add_case_and_roster_date(df, src)
    df = add_complex_rosters(df)
    df = add_all_rosters(df)
    df = add_conformance_tat(df)
    df = add_rows_counts(src, df)
    df = add_unique_npi_counts(src, df)
    
    # Apply friendly headers then excel-safe
    df = apply_friendly_headers(df, FRIENDLY_HEADERS)
    df = make_excel_safe(df)
    
    # Write two-sheet Excel and upload
    write_two_sheet_excel_gcs(df, out_uri)
    return df

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
    """Send an email with plain text + optional HTML and attachments."""
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
    
    # Attachments
    if attachments:
        for item in attachments:
            if isinstance(item, (tuple, list)) and len(item) >= 2 and isinstance(item[0], (bytes, bytearray)):
                data, name = item[0], str(item[1])
                mime = str(item[2]) if len(item) >= 3 else "application/octet-stream"
                maintype, subtype = mime.split("/", 1) if "/" in mime else ("application", "octet-stream")
                
                part = MIMEBase(maintype, subtype)
                part.set_payload(data)
                encoders.encode_base64(part)
                part.add_header("Content-Disposition", f'attachment; filename="{name}"')
                msg.attach(part)
            else:
                file_path = item[0] if isinstance(item, (tuple, list)) else item
                filename_override = item[1] if isinstance(item, (tuple, list)) and len(item) > 1 else None
                
                try:
                    p = Path(os.fspath(file_path))
                    if p.exists():
                        with p.open("rb") as f:
                            part = MIMEBase("application", "octet-stream")
                            part.set_payload(f.read())
                        encoders.encode_base64(part)
                        filename = filename_override or p.name
                        part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
                        msg.attach(part)
                    else:
                        logging.warning("Attachment not found, skipping: %s", p)
                except Exception as e:
                    logging.warning("Skipping attachment: %s", e)
    
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

if __name__ == "__main__":
    current_timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    src_df = None
    out_uri = f"gs://pdi-prvrstr-stg-hcb-dev/report_stg/daily/Conformance_kpi_{current_timestamp}.xlsx"
    config_path = "gs://us-east4-cmp-dev-pdi-int-05-5e6953c9-bucket/dags/pdi-ingestion-gcp/Dev/config/db_config.json"
    
    # Run pipeline
    result_df = run_pipeline_to_gcs(src_df=src_df, out_uri=out_uri, config_path=config_path)
    
    if SEND_EMAIL:
        csv_bytes = result_df.to_csv(index=False).encode("utf-8-sig")
        text = EMAIL_TEXT + f"\n\nRows: {len(result_df)} | Columns: {len(result_df.columns)}"
        ok = send_email_alert(
            recipient=EMAIL_TO,
            subject=EMAIL_SUBJECT,
            message_body=text,
            html_content=EMAIL_HTML,
            attachments=[(csv_bytes, "roster_report.csv", "text/csv")]
        )
        if not ok:
            logging.error("Email send failed")
    
    print(f"Pipeline completed. Result DataFrame shape: {result_df.shape}")
