# conformance_daily_dag.py  (only the changed/added parts)

# ... [imports + helpers + config exactly as you have] ...

# --------- small helper to avoid repeating the job dict ----------
def make_pyspark_job(main_uri: str, extra_args: list[str] | None = None) -> dict:
    """Return a Dataproc pyspark_job payload using our common args."""
    base_args = [
        f"--ENV={cfg['ENVIRONMENT']}",
        f"--GCS_BUCKET={cfg['STG_STORAGE_BUCKET']}",
        f"--GCS_BLOB_NAME={cfg['BLOB_NAME']}",
        f"--DB_INSTANCE={cfg['DB_INSTANCE']}",
        f"--DB_USER={user}",
        f"--DB_HOST={host}",
        f"--DB_PW={password}",
        f"--DB_NAME={cfg['DB_NAME']}",
        f"--TBL_NAME={cfg['TBL_NAME']}",
        f"--STORAGE_PROJECT_ID={cfg['STORAGE_PROJECT_ID']}",
        f"--STG_STORAGE_BUCKET={cfg['STG_STORAGE_BUCKET']}",
        f"--FROM_EMAIL={cfg['FROM_EMAIL']}",
        f"--TO_EMAIL={cfg['TO_EMAIL']}",
        f"--SMTP_SERVER={cfg['SMTP_SERVER']}",
    ]
    if extra_args:
        base_args.extend(extra_args)

    return {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": GCE_CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": main_uri,
            # optional: ship helper modules alongside your mains
            # "python_file_uris": [cfg["COMMON_LIB_ZIP"], cfg["UTILS_PY"]],
            "args": base_args,
        },
    }

# URIs for the two Python entrypoints (put these in your JSON config if you prefer)
PY_MAIN_1 = cfg["PYTHON_FILE_URIS"]          # existing first script
PY_MAIN_2 = cfg["PYTHON_FILE_URIS_2"]        # add this key in your config

with DAG(
    DAG_ID,
    description="Creates conformance report, extract from Cloud SQL and load to GCS",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=DAG_TAGS,
    params={
        "file_date": get_curr_date('file'),
        "folder_date": get_curr_date('folder'),
        "delete_src_flag": "no",
    },
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=GCE_CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
        delete_on_error=True,
        use_if_exists=False,
        impersonation_chain=CONNECT_SA,
    )

    # --- Job 1: your existing Python/PySpark main ---
    job1 = DataprocSubmitJobOperator(
        task_id="job_1_extract_and_enrich",
        job=make_pyspark_job(PY_MAIN_1, extra_args=["--STEP=extract_enrich"]),
        project_id=PROJECT_ID,
        region=REGION,
        impersonation_chain=CONNECT_SA,
    )

    # --- Job 2: the next Python main, runs AFTER job 1 ---
    job2 = DataprocSubmitJobOperator(
        task_id="job_2_publish_and_email",
        job=make_pyspark_job(PY_MAIN_2, extra_args=["--STEP=publish_email"]),
        project_id=PROJECT_ID,
        region=REGION,
        impersonation_chain=CONNECT_SA,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=GCE_CLUSTER_NAME,
        impersonation_chain=CONNECT_SA,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    final_status_task = PythonOperator(
        task_id="final_status",
        python_callable=final_status,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Run in sequence on the same cluster:
    create_cluster >> job1 >> job2 >> delete_cluster >> final_status_task
