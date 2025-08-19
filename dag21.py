from __future__ import annotations
import logging
from datetime import datetime
import pandas as pd
import sqlalchemy

import google.auth
from google.auth import impersonated_credentials
from google.cloud import secretmanager
from google.cloud.sql.connector import Connector, IPTypes  # <-- IPTypes for PRIVATE
from airflow import DAG
from airflow.operators.python import PythonOperator

# -------- Airflow Variables (with sane defaults) --------
try:
    from airflow.models import Variable
    _GET = lambda k, d=None: Variable.get(k, default_var=d)
except Exception:
    _GET = lambda k, d=None: d

PROJECT_ID = _GET("PROJECT_ID", "edp-dev-pdi-intake")
INSTANCE_CONN_NAME = _GET("CLOUDSQL_INSTANCE_CONNECTION_NAME",
                          "edp-dev-hcbstorage:us-east4:pdigpgsd1")  # <project>:<region>:<instance>
DB_NAME = _GET("CLOUDSQL_DB_NAME", "pdigpgsd1_db")
SECRET_DB_USER = _GET("CLOUDSQL_DB_USER_SECRET", "pdi_prvrstrcnf_cloud_sql_user")
SECRET_DB_PASSWORD = _GET("CLOUDSQL_DB_PASSWORD_SECRET", "pdi_prvrstrcnf_cloud_sql_password")
IMPERSONATION_SERVICE_ACCT = _GET("IMPERSONATION_SERVICE_ACCOUNT",
                                  "ghchbpdtinkconpdd@edp-dev-serviceops.iam.gserviceaccount.com")
SQL_QUERY = _GET("SQL_QUERY", "SELECT * FROM pdipp.prvrostercnf_conformed_file_stats")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# -------- Impersonation --------
def get_impersonated_credentials(target_sa: str, scopes=None, lifetime: int = 600):
    scopes = scopes or ["https://www.googleapis.com/auth/cloud-platform"]
    source_credentials, _ = google.auth.default()
    return impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=target_sa,
        target_scopes=scopes,
        lifetime=lifetime,
    )

# -------- Secret Manager helpers --------
def get_secret_value(project_id: str, secret_name: str, creds, version: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient(credentials=creds)
    name = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
    logging.info("Fetching secret: %s", secret_name)
    return client.access_secret_version(name=name).payload.data.decode("utf-8")

def get_db_credentials(project_id: str, creds) -> dict:
    return {
        "user": get_secret_value(project_id, SECRET_DB_USER, creds),
        "password": get_secret_value(project_id, SECRET_DB_PASSWORD, creds),
        "db": DB_NAME,
    }

# -------- Main task (PRIVATE IP only) --------
def run_pipeline(**context):
    logging.info("Starting Cloud SQL Postgres task (PRIVATE IP only)")

    imp_creds = get_impersonated_credentials(IMPERSONATION_SERVICE_ACCT)
    logging.info("Impersonated Service Account: %s", imp_creds._target_principal)

    db_cfg = get_db_credentials(PROJECT_ID, imp_creds)

    connector = None
    engine = None
    try:
        connector = Connector(credentials=imp_creds)

        def getconn_private_only():
            # Force PRIVATE IP; this will fail if instance lacks private IP or network isn’t reachable
            logging.info("Connecting via PRIVATE IP to %s", INSTANCE_CONN_NAME)
            return connector.connect(
                INSTANCE_CONN_NAME,
                driver="pg8000",
                user=db_cfg["user"],
                password=db_cfg["password"],
                db=db_cfg["db"],
                ip_type=IPTypes.PRIVATE,   # <-- enforce PRIVATE IP only
            )

        engine = sqlalchemy.create_engine(
            "postgresql+pg8000://",
            creator=getconn_private_only,
            pool_pre_ping=True,
        )

        with engine.connect() as conn:
            logging.info("Executing: %s", SQL_QUERY)
            df = pd.read_sql_query(SQL_QUERY, conn)
            logging.info("Rows fetched: %d", len(df))
            if not df.empty:
                logging.info("Sample output:\n%s", df.head().to_string(index=False))

    except Exception as e:
        logging.error("PRIVATE-IP connection failed: %s", e, exc_info=True)
        raise RuntimeError(
            "Cloud SQL PRIVATE IP connect failed. "
            "Ensure the instance has Private IP enabled AND your Composer environment "
            "is in a VPC/subnet with routing to that Private IP (same VPC or peered)."
        ) from e
    finally:
        if engine is not None:
            engine.dispose()
        if connector is not None:
            connector.close()
        logging.info("Closed SQL engine & connector.")

# -------- DAG --------
with DAG(
    dag_id="cloudsql_postgres_query_dag_private",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["cloudsql", "postgres", "secretmanager", "impersonation", "private-ip"],
) as dag:
    run_query_task = PythonOperator(
        task_id="run_cloudsql_query_private",
        python_callable=run_pipeline,
        provide_context=True,
    )
