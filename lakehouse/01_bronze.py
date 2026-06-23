"""
LUMBUNG — Bronze layer: raw ingest + metadata
Owner: Yasykur

Membaca data raw JSONL dari sink HDFS/Lokal, lalu menambah metadata ingest
dan menyimpan sebagai tabel append-only di layer Bronze (Delta Lake).
"""

from __future__ import annotations
import logging
import sys
from pathlib import Path
from pyspark.sql.functions import current_timestamp

# Karena berjalan secara lokal, kita tambahkan sys.path agar bisa import utils
sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bronze_layer")

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_STREAMING_DIR = BASE_DIR / "temp_buffer" / "streaming"
RAW_BATCH_DIR = BASE_DIR / "temp_buffer" / "batch"
BRONZE_DIR = BASE_DIR / "temp_buffer" / "lakehouse" / "bronze"

DATA_STREAMS = {
    "price_bapanas": RAW_STREAMING_DIR / "prices" / "bapanas",
    "price_pihps": RAW_STREAMING_DIR / "prices" / "pihps",
    "price_siskaperbapo": RAW_STREAMING_DIR / "prices" / "siskaperbapo",
    "weather": RAW_STREAMING_DIR / "weather",
    "news": RAW_STREAMING_DIR / "news",
    "kurs": RAW_STREAMING_DIR / "kurs",
    "batch_produksi": RAW_BATCH_DIR / "bps_produksi",
    "batch_imporekspor": RAW_BATCH_DIR / "bps_imporekspor",
    "batch_bulog_stok": RAW_BATCH_DIR / "bulog_stok",
    "batch_pupuk_harga": RAW_BATCH_DIR / "pupuk_harga",
}

def process_bronze():
    spark = get_spark_session("Lumbung_Bronze")
    
    for name, path in DATA_STREAMS.items():
        if not path.exists():
            log.warning(f"Path tidak ditemukan, lewati: {path}")
            continue
            
        log.info(f"Memproses {name} dari {path} ...")
        try:
            # Membaca seluruh file JSONL di dalam direktori
            # Pengaturan pathGlobFilter membantu hanya membaca jsonl
            df = spark.read.json(f"{path}/*/*.jsonl") if "streaming" in str(path) else spark.read.json(f"{path}/*.jsonl")
            
            # Tambahkan metadata
            df_bronze = df.withColumn("_ingested_to_bronze_at", current_timestamp())
            
            # Simpan ke format Delta Lake, mode append
            bronze_table_path = str(BRONZE_DIR / name)
            df_bronze.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(bronze_table_path)
                
            log.info(f"Berhasil menulis {name} ke {bronze_table_path}")
        except Exception as e:
            log.error(f"Gagal memproses {name}: {e}")

    spark.stop()

if __name__ == "__main__":
    process_bronze()
