from __future__ import annotations
import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.utils.state import State
import pendulum
import json
from google.cloud import storage

# Function to load config from GCS
def load_config_from_gcs(bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    config_data = blob.download_as_text()
    return json.loads(config_data)

# Load configuration from GCS
config = load_config_from_gcs('edp-dev-hcbstorage', 'config/dea_load_config.json')
CLUSTER_CONFIG = config['cluster_config']

# Convenience getters with defaults
PROJECT_ID = os.environ.get("GCP_PROJECT", config['config'].get('PROJECT_ID', 'edp-dev-hcbstorage'))
DAG_TAG = config['config'].get('DAG_TAGS', ['dea', 'data_pipeline'])
DAG_ID = config['config'].get('DAG_ID_PREFIX', 'dea_u1deav02')
GCE_CLUSTER_NAME = config['config'].get('CLUSTER_NAME', 'dea-cluster')
REGION = config['config'].get('REGION', 'us-central1')
CONNECT_SA = config['config'].get('CONNECT_SA', None)

def get_curr_date(date_format_for):
    """Return the current date in either 'file' or 'folder' formats."""
    dt_now = pendulum.now("America/New_York")
    if date_format_for == "file":
        dt = dt_now.format("MMDDYYYY_HHmmss")
    else:
        dt = dt_now.format("YYYYMMDDHHmm")
    return dt

def final_status(**kwargs):
    """Fail the DAG if any upstream task failed."""
    for task_instance in kwargs['dag_run'].get_task_instances():
        if task_instance.current_state() != State.SUCCESS and task_instance.task_id != kwargs['task_instance'].task_id:
            raise Exception(f"Task {task_instance.task_id} failed. Failing this DAG run")

# Default arguments for the DAG
default_args = {
    "project_id": PROJECT_ID,
    "retries": 0,
}

# PySpark job configuration
PYSPARK_JOB_DEA = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": GCE_CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"{config['config'].get('PYTHON_FILE_URIS', 'gs://your-bucket/pyspark')}/dea_cloudsql_load.py",
        "args": [
            "--ENV", config['config'].get('ENVIRONMENT', 'dev'),
            "--LOGIN_URL", config['config'].get('DEA_LOGIN_URL', 'https://example.com/login'),
            "--LGN_EMAIL", config['config'].get('DEA_LGN_ID', 'user@example.com'),
            "--LGN_PD", config['config'].get('DEA_LGN_PD', 'password'),
            "--GCS_BUCKET", config['config'].get('STG_STORAGE_BUCKET', 'edp-dev-hcbstorage'),
            "--GCS_BLOB_NAME", config['config'].get('DEA_BLOB_NAME', f"reports/prvrostercnf_file_stats_{get_curr_date('file')}.xlsx").format(get_curr_date('file')),
            "--DEA_DB_INSTANCE", config['config'].get('DEA_DB_INSTANCE', 'dea-instance'),
            "--DEA_DB_USER", config['config'].get('DEA_DB_USER', 'dea_user'),
            "--DEA_DB_PD", config['config'].get('DEA_DB_PWD', 'dea_password'),
            "--DEA_DB_NAME", config['config'].get('DEA_DB_NAME', 'dea_db'),
            "--DEA_TBL_NAME", config['config'].get('DEA_TBL_NAME', 'dea_table'),
            "--DEA_BKP_TBL_NAME", config['config'].get('DEA_BKP_TBL_NAME', 'dea_backup'),
            "--DEA_STG_TBL_NAME", config['config'].get('DEA_STG_TBL_NAME', 'dea_staging'),
            "--STORAGE_PROJECT_ID", config['config'].get('STORAGE_PROJECT_ID', PROJECT_ID),
            "--STG_STORAGE_BUCKET", config['config'].get('STG_STORAGE_BUCKET', 'edp-dev-hcbstorage'),
            "--BQ_PROJECT_ID", config['config'].get('BQ_PROJECT_ID', PROJECT_ID),
            "--BQ_PDI_DS", config['config'].get('BQ_PDI_DS', 'dea_dataset'),
            "--BQ_TBL_NAME", config['config'].get('BQ_TBL_NAME', 'dea_bq_table'),
            "--FROM_EMAIL", config['config'].get('FROM_EMAIL', 'sender@example.com'),
            "--TO_EMAIL", config['config'].get('TO_EMAIL', 'recipient@example.com'),
            "--TO_DEAEXPEMAIL", config['config'].get('TO_DEAEXPEMAIL', 'error_recipient@example.com'),
            "--SMTP_SERVER", config['config'].get('SMTP_SERVER', 'smtp.example.com'),
        ],
        "jar_file_uris": [
            f"{config['config'].get('JAR_FILE_URIS_PATH', 'gs://your-bucket/jars')}/spark-bigquery-with-dependencies_2.12-0.28.0.jar",
            f"{config['config'].get('JAR_FILE_URIS_PATH', 'gs://your-bucket/jars')}/play-json_2.12-2.6.0.jar",
            f"{config['config'].get('JAR_FILE_URIS_PATH', 'gs://your-bucket/jars')}/google-cloud-storage-2.30.1.jar",
        ]
    }
}

# Define the DAG
dag = DAG(
    DAG_ID,
    description="Creates DEA extract and loads into GCP Cloud SQL and BigQuery",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=DAG_TAG,
    params={
        "file_date": f"{get_curr_date('file')}",
        "folder_date": f"{get_curr_date('folder')}",
        "delete_src_flag": "no"
    }
)

# Task: Create Dataproc cluster
create_cluster = DataprocCreateClusterOperator(
    dag=dag,
    task_id="create_cluster",
    delete_on_error=True,
    use_if_exists=True,
    impersonation_chain=CONNECT_SA,
    project_id=PROJECT_ID,
    cluster_config=CLUSTER_CONFIG,
    region=REGION,
    cluster_name=GCE_CLUSTER_NAME
)

# Task: Submit PySpark job
dea_refresh_gcp = DataprocSubmitJobOperator(
    dag=dag,
    task_id="extract_dea_and_refresh_gcp",
    impersonation_chain=CONNECT_SA,
    job=PYSPARK_JOB_DEA,
    region=REGION,
    project_id=PROJECT_ID
)

# Task: Delete Dataproc cluster
delete_cluster = DataprocDeleteClusterOperator(
    dag=dag,
    task_id="delete_cluster",
    impersonation_chain=CONNECT_SA,
    project_id=PROJECT_ID,
    cluster_name=GCE_CLUSTER_NAME,
    region=REGION,
    trigger_rule=TriggerRule.ALL_DONE
)

# Task: Check final status
final_status_task = PythonOperator(
    dag=dag,
    task_id='final_status',
    provide_context=True,
    python_callable=final_status,
    trigger_rule=TriggerRule.ALL_DONE
)

# Task dependencies
create_cluster >> dea_refresh_gcp >> delete_cluster >> final_status_task
********************************************************
{
    "config": {
        "PROJECT_ID": "edp-dev-hcbstorage",
        "DAG_TAGS": ["dea", "data_pipeline"],
        "DAG_ID_PREFIX": "dea_u1deav02",
        "CLUSTER_NAME": "dea-cluster",
        "REGION": "us-central1",
        "CONNECT_SA": "service-account@edp-dev-hcbstorage.iam.gserviceaccount.com",
        "PYTHON_FILE_URIS": "gs://your-bucket/pyspark",
        "JAR_FILE_URIS_PATH": "gs://your-bucket/jars",
        "ENVIRONMENT": "dev",
        "DEA_LOGIN_URL": "https://example.com/login",
        "DEA_LGN_ID": "user@example.com",
        "DEA_LGN_PD": "password",
        "STG_STORAGE_BUCKET": "edp-dev-hcbstorage",
        "DEA_BLOB_NAME": "reports/prvrostercnf_file_stats_{}.xlsx",
        "DEA_DB_INSTANCE": "dea-instance",
        "DEA_DB_USER": "dea_user",
        "DEA_DB_PWD": "dea_password",
        "DEA_DB_NAME": "dea_db",
        "DEA_TBL_NAME": "dea_table",
        "DEA_BKP_TBL_NAME": "dea_backup",
        "DEA_STG_TBL_NAME": "dea_staging",
        "STORAGE_PROJECT_ID": "edp-dev-hcbstorage",
        "BQ_PROJECT_ID": "edp-dev-hcbstorage",
        "BQ_PDI_DS": "dea_dataset",
        "BQ_TBL_NAME": "dea_bq_table",
        "FROM_EMAIL": "sender@example.com",
        "TO_EMAIL": "recipient@example.com",
        "TO_DEAEXPEMAIL": "error_recipient@example.com",
        "SMTP_SERVER": "smtp.example.com"
    },
    "cluster_config": {
        "project_id": "edp-dev-hcbstorage",
        "cluster_name": "dea-cluster",
        "config": {
            "gce_cluster_config": {
                "zone_uri": "us-central1-a",
                "service_account": "service-account@edp-dev-hcbstorage.iam.gserviceaccount.com",
                "tags": ["dea", "data_pipeline"]
            },
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n2-standard-4",
                "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 100}
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n2-standard-4",
                "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 100}
            },
            "software_config": {
                "image_version": "2.2-debian12",
                "properties": {}
            },
            "endpoint_config": {"enable_http_port_access": true},
            "lifecycle_config": {
                "idle_delete_ttl": {"seconds": 1800}
            }
        },
        "region": "us-central1"
    }
}
***************************************************
from __future__ import annotations
import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.utils.state import State
import pendulum

# Add script directory to path for importing config
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
import dea_load_config as conf

# Load configuration
config = conf.dea_config
CLUSTER_CONFIG = conf.cluster_config

# Convenience getters with defaults
PROJECT_ID = os.environ.get("GCP_PROJECT", config['config'].get('PROJECT_ID', 'edp-dev-hcbstorage'))
TENANT = config['config'].get('TENANT', 'default_tenant')
OWNER_NAME = config['config'].get('OWNER_NAME', 'data_team')
DAG_TAG = config['config'].get('DAG_TAGS', ['dea', 'data_pipeline'])
DAG_ID = config['config'].get('DAG_ID_PREFIX', 'dea_u1deav02')
GCE_CLUSTER_NAME = config['config'].get('CLUSTER_NAME', 'dea-cluster')
REGION = config['config'].get('REGION', 'us-central1')
CONNECT_SA = config['config'].get('CONNECT_SA', None)
VOLTAGE_SA = config['config'].get('VOLTAGE_SA_ENC', None)

def get_curr_date(date_format_for):
    """Return the current date in either 'file' or 'folder' formats."""
    dt_now = pendulum.now("America/New_York")
    if date_format_for == "file":
        dt = dt_now.format("MMDDYYYY_HHmmss")
    else:
        dt = dt_now.format("YYYYMMDDHHmm")
    return dt

def final_status(**kwargs):
    """Fail the DAG if any upstream task failed."""
    for task_instance in kwargs['dag_run'].get_task_instances():
        if task_instance.current_state() != State.SUCCESS and task_instance.task_id != kwargs['task_instance'].task_id:
            raise Exception(f"Task {task_instance.task_id} failed. Failing this DAG run")

# Default arguments for the DAG
default_args = {
    "project_id": PROJECT_ID,
    "retries": 0,
}

# PySpark job configuration
PYSPARK_JOB_DEA = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": GCE_CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": f"{config['config'].get('PYTHON_FILE_URIS', 'gs://your-bucket/pyspark')}/dea_cloudsql_load.py",
        "args": [
            "--ENV", config['config'].get('ENVIRONMENT', 'dev'),
            "--LOGIN_URL", config['config'].get('DEA_LOGIN_URL', 'https://example.com/login'),
            "--LGN_EMAIL", config['config'].get('DEA_LGN_ID', 'user@example.com'),
            "--LGN_PD", config['config'].get('DEA_LGN_PD', 'password'),
            "--GCS_BUCKET", config['config'].get('STG_STORAGE_BUCKET', 'edp-dev-hcbstorage'),
            "--GCS_BLOB_NAME", config['config'].get('DEA_BLOB_NAME', f"reports/prvrostercnf_file_stats_{get_curr_date('file')}.xlsx"),
            "--DEA_DB_INSTANCE", config['config'].get('DEA_DB_INSTANCE', 'dea-instance'),
            "--DEA_DB_USER", config['config'].get('DEA_DB_USER', 'dea_user'),
            "--DEA_DB_PD", config['config'].get('DEA_DB_PWD', 'dea_password'),
            "--DEA_DB_NAME", config['config'].get('DEA_DB_NAME', 'dea_db'),
            "--DEA_TBL_NAME", config['config'].get('DEA_TBL_NAME', 'dea_table'),
            "--DEA_BKP_TBL_NAME", config['config'].get('DEA_BKP_TBL_NAME', 'dea_backup'),
            "--DEA_STG_TBL_NAME", config['config'].get('DEA_STG_TBL_NAME', 'dea_staging'),
            "--STORAGE_PROJECT_ID", config['config'].get('STORAGE_PROJECT_ID', PROJECT_ID),
            "--STG_STORAGE_BUCKET", config['config'].get('STG_STORAGE_BUCKET', 'edp-dev-hcbstorage'),
            "--BQ_PROJECT_ID", config['config'].get('BQ_PROJECT_ID', PROJECT_ID),
            "--BQ_PDI_DS", config['config'].get('BQ_PDI_DS', 'dea_dataset'),
            "--BQ_TBL_NAME", config['config'].get('BQ_TBL_NAME', 'dea_bq_table'),
            "--FROM_EMAIL", config['config'].get('FROM_EMAIL', 'sender@example.com'),
            "--TO_EMAIL", config['config'].get('TO_EMAIL', 'recipient@example.com'),
            "--TO_DEAEXPEMAIL", config['config'].get('TO_DEAEXPEMAIL', 'error_recipient@example.com'),
            "--SMTP_SERVER", config['config'].get('SMTP_SERVER', 'smtp.example.com'),
        ],
        "jar_file_uris": [
            f"{config['config'].get('JAR_FILE_URIS_PATH', 'gs://your-bucket/jars')}/spark-bigquery-with-dependencies_2.12-0.28.0.jar",
            f"{config['config'].get('JAR_FILE_URIS_PATH', 'gs://your-bucket/jars')}/play-json_2.12-2.6.0.jar",
            f"{config['config'].get('JAR_FILE_URIS_PATH', 'gs://your-bucket/jars')}/google-cloud-storage-2.30.1.jar",
        ]
    }
}

# Define the DAG
dag = DAG(
    DAG_ID,
    description="Creates DEA extract and loads into GCP Cloud SQL and BigQuery",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=DAG_TAG,
    params={
        "file_date": f"{get_curr_date('file')}",
        "folder_date": f"{get_curr_date('folder')}",
        "delete_src_flag": "no"
    }
)

# Task: Create Dataproc cluster
create_cluster = DataprocCreateClusterOperator(
    dag=dag,
    task_id="create_cluster",
    delete_on_error=True,
    use_if_exists=True,
    impersonation_chain=CONNECT_SA,
    project_id=PROJECT_ID,
    cluster_config=CLUSTER_CONFIG,
    region=REGION,
    cluster_name=GCE_CLUSTER_NAME
)

# Task: Submit PySpark job
dea_refresh_gcp = DataprocSubmitJobOperator(
    dag=dag,
    task_id="extract_dea_and_refresh_gcp",
    impersonation_chain=CONNECT_SA,
    job=PYSPARK_JOB_DEA,
    region=REGION,
    project_id=PROJECT_ID
)

# Task: Delete Dataproc cluster
delete_cluster = DataprocDeleteClusterOperator(
    dag=dag,
    task_id="delete_cluster",
    impersonation_chain=CONNECT_SA,
    project_id=PROJECT_ID,
    cluster_name=GCE_CLUSTER_NAME,
    region=REGION,
    trigger_rule=TriggerRule.ALL_DONE
)

# Task: Check final status
final_status_task = PythonOperator(
    dag=dag,
    task_id='final_status',
    provide_context=True,
    python_callable=final_status,
    trigger_rule=TriggerRule.ALL_DONE
)

# Task dependencies
create_cluster >> dea_refresh_gcp >> delete_cluster >> final_status_task
