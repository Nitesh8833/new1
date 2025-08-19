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
