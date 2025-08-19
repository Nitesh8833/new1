# dags/cloudsql_postgres_query_dag.py

from __future__ import annotations

import logging
from datetime import datetime

import pandas as pd
import sqlalchemy

import google.auth
from google.auth import impersonated_credentials
from google.cloud import secretmanager
from google.cloud.sql.connector import Connector

from airflow import DAG
from airflow.operators.python import PythonOperator

# Optional: read config from Airflow Variables (fallback to hardcoded defaults)
try:
    from airflow.models import Variable
    _GET = lambda k, d=None: Variable.get(k, default_var=d)
except Exception:
    _GET = lambda k, d=None: d  # if Variables not available during parse


# ───────────────────────────────────────────────────────────────────────────────
# Configuration (set via Airflow Variables or edit defaults here)
# ───────────────────────────────────────────────────────────────────────────────
PROJECT_ID                = _GET("PROJECT_ID", "your-gcp-project-id")
INSTANCE_CONN_NAME        = _GET("CLOUDSQL_INSTANCE_CONNECTION_NAME", "project:region:instance")  # e.g. spicemoney:us-east4:pdi-pgsql
DB_NAME                   = _GET("CLOUDSQL_DB_NAME", "your_db_name")
SECRET_DB_USER            = _GET("CLOUDSQL_DB_USER_SECRET", "pdi_prvrstrcnf_cloud_sql_user")
SECRET_DB_PASSWORD        = _GET("CLOUDSQL_DB_PASSWORD_SECRET", "pdi_prvrstrcnf_cloud_sql_password")
IMPERSONATION_SERVICE_ACCT= _GET("IMPERSONATION_SERVICE_ACCOUNT", "ghchbpdtinkconpdd@edp-dev-serviceops.iam.gserviceaccount.com")
SQL_QUERY                 = _GET("SQL_QUERY", "SELECT NOW();")  # replace with your query

# Logging format
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ───────────────────────────────────────────────────────────────────────────────
# IAM Impersonation
# ───────────────────────────────────────────────────────────────────────────────
def get_impersonated_credentials(
    target_sa: str,
    scopes: list[str] | None = None,
    lifetime: int = 600,
):
    """Impersonate target service account using Composer's default credentials."""
    scopes = scopes or ["https://www.googleapis.com/auth/cloud-platform"]
    source_credentials, _ = google.auth.default()
    return impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=target_sa,
        target_scopes=scopes,
        lifetime=lifetime,
    )


# ───────────────────────────────────────────────────────────────────────────────
# Secret Manager helpers (use impersonated creds)
# ───────────────────────────────────────────────────────────────────────────────
def get_secret_value(project_id: str, secret_name: str, creds, version: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient(credentials=creds)
    name = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
    logging.info("Fetching secret: %s", secret_name)
    resp = client.access_secret_version(name=name)
    return resp.payload.data.decode("utf-8")


def get_db_credentials(project_id: str, creds) -> dict:
    return {
        "user": get_secret_value(project_id, SECRET_DB_USER, creds),
        "password": get_secret_value(project_id, SECRET_DB_PASSWORD, creds),
        "db": DB_NAME,
    }


# ───────────────────────────────────────────────────────────────────────────────
# Main task (runs inside the PythonOperator)
# ───────────────────────────────────────────────────────────────────────────────
def run_pipeline(**context):
    logging.info("Starting Cloud SQL Postgres task via Connector + impersonation")

    # 1) Impersonate pipeline SA (must have roles/cloudsql.client + secret accessor)
    imp_creds = get_impersonated_credentials(IMPERSONATION_SERVICE_ACCT)

    # 2) Fetch DB user/password from Secret Manager using impersonated creds
    db_cfg = get_db_credentials(PROJECT_ID, imp_creds)

    connector = None
    engine = None
    try:
        # 3) Initialize Connector with impersonated creds
        connector = Connector(credentials=imp_creds)

        # 4) Creator function for SQLAlchemy to obtain a pg8000 connection
        def getconn():
            return connector.connect(
                INSTANCE_CONN_NAME,   # "project:region:instance"
                driver="pg8000",
                user=db_cfg["user"],
                password=db_cfg["password"],
                db=db_cfg["db"],
            )

        # 5) Create SQLAlchemy engine
        engine = sqlalchemy.create_engine(
            "postgresql+pg8000://",
            creator=getconn,
            pool_pre_ping=True,
        )

        # 6) Execute query
        with engine.connect() as conn:
            logging.info("Executing: %s", SQL_QUERY)
            df = pd.read_sql_query(SQL_QUERY, conn)
            logging.info("Rows fetched: %d", len(df))
            # Print a small preview for logs
            if not df.empty:
                logging.info("Sample output:\n%s", df.head().to_string(index=False))

    except Exception as e:
        logging.error("Pipeline failed: %s", e, exc_info=True)
        raise
    finally:
        # 7) Cleanup
        if engine is not None:
            engine.dispose()
        if connector is not None:
            connector.close()
        logging.info("Closed SQL engine & connector.")


# ───────────────────────────────────────────────────────────────────────────────
# DAG
# ───────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="cloudsql_postgres_query_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["cloudsql", "postgres", "secretmanager", "impersonation"],
) as dag:

    run_query_task = PythonOperator(
        task_id="run_cloudsql_query",
        python_callable=run_pipeline,
    )

*************************************************
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import pandas as pd

from google.cloud import secretmanager
from google.cloud.sql.connector import Connector
import sqlalchemy


# -------------------------------
# Helper: Fetch Secret Manager values
# -------------------------------
def get_secret_value(project_id: str, secret_name: str, version: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
    response = client.access_secret_version(name=secret_path)
    return response.payload.data.decode("UTF-8")


def get_db_config(project_id: str) -> dict:
    """Fetch Cloud SQL credentials from Secret Manager"""
    return {
        "user": get_secret_value(project_id, "pdi_prvrstrcnf_cloud_sql_user"),
        "password": get_secret_value(project_id, "pdi_prvrstrcnf_cloud_sql_password"),
        "db": "your_db_name",  # 🔹 Replace with your actual DB name
    }


# -------------------------------
# Main Task
# -------------------------------
def run_pipeline(**context):
    PROJECT_ID = "your-gcp-project-id"   # 🔹 Replace with your project ID
    INSTANCE = "PROJECT_ID:REGION:INSTANCE_NAME"  # 🔹 e.g. spicemoney:us-east4:my-postgres
    SQL_QUERY = "SELECT NOW();"          # 🔹 Replace with your query

    connector = Connector()
    conn = None
    try:
        # 1. Get DB credentials
        db_cfg = get_db_config(PROJECT_ID)

        # 2. Define connection creator for connector
        def getconn():
            return connector.connect(
                INSTANCE,
                "pg8000",
                user=db_cfg["user"],
                password=db_cfg["password"],
                db=db_cfg["db"],
            )

        # 3. Create SQLAlchemy engine
        pool = sqlalchemy.create_engine(
            "postgresql+pg8000://",
            creator=getconn,
        )

        # 4. Run query
        with pool.connect() as conn:
            df = pd.read_sql(SQL_QUERY, conn)
            logging.info("Query executed. Rows fetched: %d", len(df))
            logging.info("Sample output:\n%s", df.head().to_string(index=False))

    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        raise
    finally:
        if conn:
            conn.close()
        connector.close()
        logging.info("Cloud SQL connection closed.")


# -------------------------------
# DAG Definition
# -------------------------------
with DAG(
    dag_id="cloudsql_postgres_query_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["cloudsql", "postgres", "secretmanager"],
) as dag:

    run_query_task = PythonOperator(
        task_id="run_cloudsql_query",
        python_callable=run_pipeline,
    )

************************************************************
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import psycopg2
import pandas as pd
from google.cloud import secretmanager


# -------------------------------
# Helper Functions (from your script)
# -------------------------------
def get_secret_value(project_id: str, secret_name: str, version: str = "latest") -> str:
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
    response = client.access_secret_version(name=secret_path)
    return response.payload.data.decode("UTF-8")


def get_db_config(project_id: str) -> dict:
    db_config = {
        "user": get_secret_value(project_id, "pdi_prvrstrcnf_cloud_sql_user"),
        "password": get_secret_value(project_id, "pdi_prvrstrcnf_cloud_sql_password"),
        "host": get_secret_value(project_id, "pdi_prvrstrcnf_cloud_sql_host_ip"),
        "dbname": "your_db_name",  # 🔹 Replace with your actual DB name
        "port": 5432,
    }
    return db_config


def run_pipeline(**context):
    PROJECT_ID = "your-gcp-project-id"  # 🔹 Replace with your project ID
    SQL_QUERY = "SELECT * FROM your_table LIMIT 10;"  # 🔹 Replace with your SQL query

    conn = None
    try:
        # 1. Fetch secrets
        db_config = get_db_config(PROJECT_ID)

        # 2. Connect to DB
        logging.info("Connecting to Cloud SQL...")
        conn = psycopg2.connect(
            dbname=db_config["dbname"],
            user=db_config["user"],
            password=db_config["password"],
            host=db_config["host"],
            port=db_config["port"],
        )
        logging.info("Connection established.")

        # 3. Run query
        df = pd.read_sql_query(SQL_QUERY, conn)
        logging.info("Query executed. Rows fetched: %d", len(df))
        logging.info("Sample output:\n%s", df.head().to_string(index=False))

    except Exception as e:
        logging.error(f"Pipeline failed: {e}", exc_info=True)
        raise

    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")


# -------------------------------
# DAG Definition
# -------------------------------
with DAG(
    dag_id="cloudsql_postgres_query_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",  # 🔹 Run once daily
    catchup=False,
    tags=["cloudsql", "postgres", "secretmanager"],
) as dag:

    run_query_task = PythonOperator(
        task_id="run_cloudsql_query",
        python_callable=run_pipeline,
    )
