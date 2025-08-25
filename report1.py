# at top of file:
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


