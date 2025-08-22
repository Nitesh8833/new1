

# file: simple_db_query_min.py
import json
import pandas as pd
import psycopg2
from google.cloud import storage


def _load_json(path: str) -> dict:
    """Load JSON from GCS (gs://...) or local file path."""
    if path.startswith("gs://"):
        bucket, blob_path = path[5:].split("/", 1)
        client = storage.Client()
        text = client.bucket(bucket).blob(blob_path).download_as_text()
        return json.loads(text)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def query_df_from_config(config_path: str) -> pd.DataFrame:
    """Connect using credentials from JSON and return the SQL result as a DataFrame."""
    cfg = _load_json(config_path)

    # Required fields only
    sql = cfg["sql_query"]
    db = cfg["database"]
    conn = psycopg2.connect(
        dbname=db["dbname"],
        user=db["user"],
        password=db["password"],
        host=db["host"],
        port=int(db["port"]),
    )
    try:
        return pd.read_sql_query(sql, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    CONFIG_PATH = "gs://YOUR_BUCKET/path/to/db_config.json"  # or local path
    df = query_df_from_config(CONFIG_PATH)
    print(df.head())
    print("Rows:", len(df))

**************************************
# file: simple_db_query_from_config.py
import json
import pandas as pd
import psycopg2

# If your CONFIG_PATH is a gs:// URI, this import is used
from google.cloud import storage


CONFIG_PATH = "gs://YOUR_BUCKET/path/to/db_config.json"  # or "/path/local/db_config.json"


def load_config(path: str) -> dict:
    """Load JSON config from GCS (gs://...) or local filesystem."""
    if path.startswith("gs://"):
        bucket_name, blob_path = path[5:].split("/", 1)
        client = storage.Client()
        text = client.bucket(bucket_name).blob(blob_path).download_as_text()
        return json.loads(text)
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def query_df_from_config(config_path: str) -> pd.DataFrame:
    """
    Expects JSON like:
    {
      "sql_query": "SELECT ...",
      "database": {
        "dbname": "...",
        "user": "...",
        "password": "...",
        "host": "...",
        "port": 5432
      }
    }
    (If "database" is omitted, top-level keys are used.)
    """
    cfg = load_config(config_path)

    # Pull SQL
    sql = cfg.get("sql_query") or cfg.get("SQL") or "SELECT 1"

    # Pull DB creds (nested under "database" or top-level)
    db_cfg = cfg.get("database", cfg)
    dbname = db_cfg["dbname"]
    user = db_cfg["user"]
    password = db_cfg["password"]
    host = db_cfg["host"]
    port = int(db_cfg.get("port", 5432))

    # Connect & query
    with psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
        # If needed: sslmode="require"
    ) as conn:
        df = pd.read_sql_query(sql, conn)

    return df


if __name__ == "__main__":
    df = query_df_from_config(CONFIG_PATH)
    print(df.head())
    print("Rows:", len(df))
