# db_config.yaml

database:
  dbname: "pdigpgsd1_db"
  user: "pdigpgsd1_nh_user"
  password: "pdigpgsd1_c7#6H_a8wd"
  host: "10.247.163.124"
  port: 5432

gcs:
  bucket_name: "usmedphcb-pdi-intake-devstg"
  client_cert: "prv_rstrcnf_conformed_files/cloudsql_instance/client-cert.pem"
  client_key: "prv_rstrcnf_conformed_files/cloudsql_instance/client-key.pem"
  server_ca: "prv_rstrcnf_conformed_files/cloudsql_instance/server-ca.pem"
*****************************************
import json
from pathlib import Path

# Path inside Composer bucket
CONFIG_PATH = Path("/home/airflow/gcs/dags/pdi-ingestion-gcp/Dev/config/db_config.json")

def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

# Example usage
cfg = load_config()
print("DB Name:", cfg["database"]["dbname"])
print("Bucket:", cfg["gcs"]["bucket_name"])
