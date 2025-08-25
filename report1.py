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


