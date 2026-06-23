"""
LUMBUNG — Scheduler cron untuk batch ingestion
Owner: Yasykur

DEPRECATED: Penjadwalan batch ingestion telah dipindahkan menggunakan 
Apache Airflow. Silakan merujuk ke direktori `dags/lumbung_batch_dag.py`
dan konfigurasi `docker-compose-airflow.yml`.
"""

import sys

if __name__ == "__main__":
    print("Scheduler ini telah dinonaktifkan. Silakan gunakan Apache Airflow.")
    sys.exit(0)
