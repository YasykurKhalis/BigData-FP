from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'lumbung',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'lumbung_batch_ingest',
    default_args=default_args,
    description='DAG untuk batch ingestion data pasokan pertanian (LUMBUNG)',
    schedule_interval=timedelta(days=1), # Berjalan harian
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['lumbung', 'batch'],
) as dag:

    # Task untuk memanggil script batch ingestion menggunakan Python
    ingest_produksi = BashOperator(
        task_id='ingest_bps_produksi',
        bash_command='python /opt/airflow/batch_ingest/ingest_bps_produksi.py',
    )

    ingest_imporekspor = BashOperator(
        task_id='ingest_bps_imporekspor',
        bash_command='python /opt/airflow/batch_ingest/ingest_bps_imporekspor.py',
    )

    ingest_stok_bulog = BashOperator(
        task_id='ingest_bulog_stok',
        bash_command='python /opt/airflow/batch_ingest/ingest_bulog_stok.py',
    )

    ingest_pupuk = BashOperator(
        task_id='ingest_pupuk_harga',
        bash_command='python /opt/airflow/batch_ingest/ingest_pupuk_harga.py',
    )

    # Menentukan urutan eksekusi (semuanya bisa berjalan paralel)
    [ingest_produksi, ingest_imporekspor, ingest_stok_bulog, ingest_pupuk]
