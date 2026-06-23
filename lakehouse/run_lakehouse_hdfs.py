"""
LUMBUNG — Jalankan pipeline Lakehouse Bronze→Silver→Gold langsung ke HDFS
Owner: tim

Menggunakan Spark dengan konfigurasi HDFS agar tabel Delta Lake ditulis
ke hdfs://namenode:9000/data/lumbung/lakehouse/ bukan ke temp_buffer lokal.

USAGE:
    python lakehouse/run_lakehouse_hdfs.py
    python lakehouse/run_lakehouse_hdfs.py --layer bronze    # hanya bronze
    python lakehouse/run_lakehouse_hdfs.py --layer silver
    python lakehouse/run_lakehouse_hdfs.py --layer gold
"""

from __future__ import annotations
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("lakehouse_hdfs")

# Path HDFS — pakai localhost:9000 karena port di-expose ke host
# (hostname 'namenode' tidak bisa di-resolve JVM dari luar container)
HDFS_NAMENODE  = "hdfs://localhost:9000"
HDFS_ROOT      = f"{HDFS_NAMENODE}/data/lumbung"
HDFS_BRONZE    = f"{HDFS_ROOT}/lakehouse/bronze"
HDFS_SILVER    = f"{HDFS_ROOT}/lakehouse/silver"
HDFS_GOLD      = f"{HDFS_ROOT}/lakehouse/gold"
HDFS_EXPORT    = f"{HDFS_ROOT}/export"

# Path lokal (sumber raw data)
BASE_DIR       = Path(__file__).resolve().parent.parent
TEMP_DIR       = BASE_DIR / "temp_buffer"
RAW_STREAMING  = TEMP_DIR / "streaming"
RAW_BATCH      = TEMP_DIR / "batch"


def get_spark_hdfs(app_name: str):
    """SparkSession dengan konfigurasi HDFS namenode."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    import subprocess

    # Ambil IP container secara dinamis
    def get_container_ip(name: str) -> str:
        try:
            result = subprocess.run(
                ["docker", "inspect", name, "--format",
                 "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}"],
                capture_output=True, text=True, timeout=5
            )
            return result.stdout.strip() or "127.0.0.1"
        except Exception:
            return "127.0.0.1"

    namenode_ip = get_container_ip("lumbung-namenode")
    datanode_ip = get_container_ip("lumbung-datanode")
    log.info(f"Container IPs — namenode: {namenode_ip}, datanode: {datanode_ip}")

    # Gunakan IP langsung agar JVM bisa resolve tanpa /etc/hosts
    hdfs_uri = f"hdfs://{namenode_ip}:9000"

    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.hadoop.fs.defaultFS", hdfs_uri) \
        .config("spark.hadoop.dfs.replication", "1") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "false") \
        .config("spark.hadoop.dfs.datanode.hostname", datanode_ip)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Patch Hadoop conf di runtime
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.defaultFS", hdfs_uri)
    hadoop_conf.set("dfs.client.use.datanode.hostname", "false")
    hadoop_conf.set("dfs.datanode.hostname", datanode_ip)
    hadoop_conf.set("dfs.replication", "1")

    # Simpan URI agar dipakai modul lain
    spark._hdfs_uri = hdfs_uri
    return spark


# ── Bronze ────────────────────────────────────────────────────────────────────

DATA_STREAMS = {
    "price_bapanas":      RAW_STREAMING / "prices" / "bapanas",
    "price_pihps":        RAW_STREAMING / "prices" / "pihps",
    "price_siskaperbapo": RAW_STREAMING / "prices" / "siskaperbapo",
    "weather":            RAW_STREAMING / "weather",
    "news":               RAW_STREAMING / "news",
    "kurs":               RAW_STREAMING / "kurs",
    "batch_produksi":     RAW_BATCH / "bps_produksi",
    "batch_imporekspor":  RAW_BATCH / "bps_imporekspor",
    "batch_bulog_stok":   RAW_BATCH / "bulog_stok",
    "batch_pupuk_harga":  RAW_BATCH / "pupuk_harga",
}


def run_bronze(spark) -> None:
    from pyspark.sql.functions import current_timestamp

    log.info("=== BRONZE → HDFS ===")
    hdfs_uri  = getattr(spark, "_hdfs_uri", HDFS_NAMENODE)
    hdfs_bronze = f"{hdfs_uri}/data/lumbung/lakehouse/bronze"
    for name, local_path in DATA_STREAMS.items():
        if not local_path.exists():
            log.warning(f"  Skip {name}: path lokal tidak ada")
            continue

        try:
            # Prefix file:// agar Spark baca dari filesystem lokal (bukan HDFS)
            if "streaming" in str(local_path):
                pattern = f"file://{local_path}/*/*.jsonl"
            else:
                pattern = f"file://{local_path}/*.jsonl"
            df = spark.read.json(pattern)
            df = df.withColumn("_ingested_to_bronze_at", current_timestamp())
            hdfs_target = f"{hdfs_bronze}/{name}"
            df.write.format("delta").mode("append") \
                .option("mergeSchema", "true").save(hdfs_target)
            log.info(f"  ✓ {name} → {hdfs_target} ({df.count()} rows)")
        except Exception as e:
            log.error(f"  ✗ {name}: {e}")


# ── Silver ────────────────────────────────────────────────────────────────────

def run_silver(spark) -> None:
    from pyspark.sql.functions import col, to_date, when, lit

    log.info("=== SILVER → HDFS ===")
    hdfs_uri    = getattr(spark, "_hdfs_uri", HDFS_NAMENODE)
    hdfs_bronze = f"{hdfs_uri}/data/lumbung/lakehouse/bronze"
    hdfs_silver = f"{hdfs_uri}/data/lumbung/lakehouse/silver"
    # 1. Silver Prices
    price_tables = ["price_bapanas", "price_pihps", "price_siskaperbapo"]
    df_prices = None
    for tbl in price_tables:
        path = f"{hdfs_bronze}/{tbl}"
        try:
            df = spark.read.format("delta").load(path)
            if "date" in df.columns:
                df = df.withColumn("date_parsed", to_date(col("date")))

            # Normalisasi commodity name
            commodity_col = col("commodity")
            df = df.withColumn("commodity",
                when(commodity_col == "beras_kualitas_bawah",  lit("beras"))
                .when(commodity_col == "beras_kualitas_medium", lit("beras"))
                .when(commodity_col == "beras_kualitas_super",  lit("beras"))
                .when(commodity_col == "gula_pasir",  lit("beras"))
                .when(commodity_col == "jagung_pipilan_kering", lit("beras"))
                .when(commodity_col == "kedelai_impor", lit("beras"))
                .when(commodity_col.isin("kedelai", "gula", "gandum"), lit("beras"))
                .otherwise(commodity_col)
            )

            df_clean = df.select("source", "commodity", "date_parsed", "price") \
                         .dropDuplicates(["source", "commodity", "date_parsed"])
            df_prices = df_clean if df_prices is None else df_prices.unionByName(df_clean)
        except Exception as e:
            log.warning(f"  Skip {tbl}: {e}")

    if df_prices is not None:
        target = f"{hdfs_silver}/silver_prices"
        df_prices.write.format("delta").mode("overwrite").save(target)
        log.info(f"  ✓ silver_prices → {target} ({df_prices.count()} rows)")

    # 2. Silver News
    try:
        df_news = spark.read.format("delta").load(f"{hdfs_bronze}/news")
        df_news = df_news.dropDuplicates(["article_id"])
        if "ingestion_ts" in df_news.columns:
            df_news = df_news.withColumn("date_parsed", to_date(col("ingestion_ts")))
        target = f"{hdfs_silver}/silver_news"
        df_news.write.format("delta").mode("overwrite").save(target)
        log.info(f"  ✓ silver_news → {target} ({df_news.count()} rows)")
    except Exception as e:
        log.warning(f"  Skip news: {e}")

    # 3. Silver Macro
    macro_tables = ["batch_produksi", "batch_imporekspor", "batch_bulog_stok", "batch_pupuk_harga"]
    df_macro = None
    for tbl in macro_tables:
        try:
            df = spark.read.format("delta").load(f"{hdfs_bronze}/{tbl}")
            if "date" in df.columns:
                df = df.withColumn("date_parsed", to_date(col("date")))
            elif "year" in df.columns:
                df = df.withColumn("date_parsed", to_date(col("year"), "yyyy"))
            else:
                df = df.withColumn("date_parsed", to_date(col("ingestion_ts")))
            if "close_price" in df.columns and "value" not in df.columns:
                df = df.withColumnRenamed("close_price", "value")
            df_clean = df.select("source", "indicator", "date_parsed", "value") \
                         .dropDuplicates(["source", "indicator", "date_parsed"])
            df_macro = df_clean if df_macro is None else df_macro.unionByName(df_clean)
        except Exception as e:
            log.warning(f"  Skip {tbl}: {e}")

    if df_macro is not None:
        target = f"{hdfs_silver}/silver_macro"
        df_macro.write.format("delta").mode("overwrite").save(target)
        log.info(f"  ✓ silver_macro → {target} ({df_macro.count()} rows)")


# ── Gold ──────────────────────────────────────────────────────────────────────

def run_gold(spark) -> None:
    from pyspark.sql.functions import avg, last, first, col
    from pyspark.sql.window import Window

    log.info("=== GOLD → HDFS ===")
    hdfs_uri    = getattr(spark, "_hdfs_uri", HDFS_NAMENODE)
    hdfs_silver = f"{hdfs_uri}/data/lumbung/lakehouse/silver"
    hdfs_gold   = f"{hdfs_uri}/data/lumbung/lakehouse/gold"

    try:
        df_prices = spark.read.format("delta").load(f"{hdfs_silver}/silver_prices")
    except Exception as e:
        log.error(f"  silver_prices tidak ada: {e}")
        return

    df_daily = df_prices.groupBy("date_parsed", "commodity") \
        .agg(avg("price").alias("avg_price"))

    try:
        df_macro = spark.read.format("delta").load(f"{hdfs_silver}/silver_macro")
        df_macro_pivot = df_macro.groupBy("date_parsed").pivot("indicator").agg(first("value"))
        df_fs = df_daily.join(df_macro_pivot, "date_parsed", "left")

        window_ffill = Window.orderBy("date_parsed").rowsBetween(
            Window.unboundedPreceding, Window.currentRow
        )
        for c in df_macro_pivot.columns:
            if c != "date_parsed":
                df_fs = df_fs.withColumn(c, last(col(c), ignorenulls=True).over(window_ffill))
    except Exception:
        df_fs = df_daily

    target = f"{hdfs_gold}/feature_store"
    df_fs.write.format("delta").mode("overwrite").save(target)
    row_count = df_fs.count()
    log.info(f"  ✓ gold/feature_store → {target} ({row_count} rows)")


# ── Export JSON ───────────────────────────────────────────────────────────────

def run_export(spark) -> None:
    """Export Gold ke JSON di HDFS /data/lumbung/export/ untuk dashboard."""
    import json, shutil
    from pathlib import Path as P

    log.info("=== EXPORT JSON → HDFS ===")
    hdfs_uri = getattr(spark, "_hdfs_uri", HDFS_NAMENODE)
    hdfs_gold_path = f"{hdfs_uri}/data/lumbung/lakehouse/gold/feature_store"

    try:
        df = spark.read.format("delta").load(hdfs_gold_path)
    except Exception as e:
        log.error(f"  Gold tidak ditemukan: {e}")
        return

    # Tulis dulu ke temp lokal, lalu upload ke HDFS
    tmp_local = BASE_DIR / "temp_buffer" / "_tmp_gold_export"
    df.coalesce(1).write.format("json").mode("overwrite").save(str(tmp_local))

    # Gabung part files
    records = []
    for part in sorted(tmp_local.glob("part-*.json")):
        with open(part, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except Exception:
                        pass
    shutil.rmtree(tmp_local, ignore_errors=True)

    # Simpan lokal dulu (untuk dashboard lokal)
    local_export = BASE_DIR / "temp_buffer" / "export" / "feature_store.json"
    local_export.parent.mkdir(parents=True, exist_ok=True)
    with open(local_export, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, default=str)
    log.info(f"  ✓ feature_store.json lokal ({len(records)} records)")

    # Upload ke HDFS via WebHDFS
    try:
        import socket, os
        sys.path.insert(0, str(BASE_DIR / "hdfs"))
        from _dns_patch import patch_dns
        patch_dns()
        from hdfs import InsecureClient
        hdfs_client = InsecureClient(
            os.getenv("WEBHDFS_URL", "http://localhost:9870"),
            user=os.getenv("HDFS_USER", "root")
        )
        hdfs_target = f"{HDFS_ROOT}/export/feature_store.json"
        hdfs_client.makedirs(f"{HDFS_ROOT}/export")
        with open(local_export, "rb") as f:
            hdfs_client.write(hdfs_target, f, overwrite=True)
        log.info(f"  ✓ feature_store.json → HDFS {hdfs_target}")
    except Exception as e:
        log.warning(f"  HDFS upload feature_store.json gagal: {e} (data lokal tetap tersedia)")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--layer", choices=["bronze", "silver", "gold", "export", "all"],
                        default="all", help="Layer yang akan dijalankan")
    args = parser.parse_args()

    spark = get_spark_hdfs("Lumbung_Lakehouse_HDFS")

    try:
        if args.layer in ("bronze", "all"):
            run_bronze(spark)
        if args.layer in ("silver", "all"):
            run_silver(spark)
        if args.layer in ("gold", "all"):
            run_gold(spark)
        if args.layer in ("export", "all"):
            run_export(spark)
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    sys.exit(main())
