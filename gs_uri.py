def _parse_gs_uri(config: dict) -> Tuple[str, str]:
    """
    Build and parse a gs:// URI.
    - If config has output_gs_prefix + output_gs_suffix → build gs_uri with today_str.
    - Otherwise use gs_uri directly.
    Returns: (bucket, object_name)
    """
    # --- Decide how to build the URI ---
    if "output_gs_prefix" in config and "output_gs_suffix" in config:
        today_str = datetime.today().strftime("%Y%m%d")
        gs_uri = f"{config['output_gs_prefix']}{today_str}{config['output_gs_suffix']}"
    elif "gs_uri" in config:
        gs_uri = config["gs_uri"]
    else:
        raise ValueError("Config must contain either 'gs_uri' or (output_gs_prefix + output_gs_suffix)")

    # --- Parse the URI into bucket + object name ---
    if not gs_uri.startswith("gs://"):
        raise ValueError("gs_uri must start with 'gs://'")

    path = gs_uri[5:]
    bucket, sep, object_name = path.partition("/")
    if not bucket or not sep or not object_name:
        raise ValueError("Invalid gs_uri, expected 'gs://<bucket>/<object>'")

    return bucket, object_name
