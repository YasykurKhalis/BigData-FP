"""
LUMBUNG — Gold layer: feature store + analitik
Owner: Yasykur

Menggabungkan (JOIN) tabel Silver untuk membuat master feature store.
Tabel Gold ini siap digunakan untuk Machine Learning dan Dashboard.
"""

from __future__ import annotations
import logging
import sys
from pathlib import Path
from pyspark.sql.functions import col, avg, sum as _sum, last, first
from pyspark.sql.window import Window

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("gold_layer")

BASE_DIR = Path(__file__).resolve().parent.parent
SILVER_DIR = BASE_DIR / "temp_buffer" / "lakehouse" / "silver"
GOLD_DIR = BASE_DIR / "temp_buffer" / "lakehouse" / "gold"

def process_gold():
    spark = get_spark_session("Lumbung_Gold")
    
    silver_price_path = SILVER_DIR / "silver_prices"
    silver_macro_path = SILVER_DIR / "silver_macro"
    
    if not silver_price_path.exists():
        log.error("Tabel silver_prices tidak ditemukan, skip Gold.")
        return
        
    df_prices = spark.read.format("delta").load(str(silver_price_path))
    
    # 1. Agregasi Harga Harian per Komoditas (rata-rata jika ada banyak source)
    df_daily_price = df_prices.groupBy("date_parsed", "commodity") \
        .agg(avg("price").alias("avg_price"))
        
    # 2. Ambil data Makro jika ada
    if silver_macro_path.exists():
        df_macro = spark.read.format("delta").load(str(silver_macro_path))
        
        # Pivot indikator makro agar jadi kolom
        df_macro_pivoted = df_macro.groupBy("date_parsed") \
            .pivot("indicator") \
            .agg(first("value"))
            
        # Join ke prices (Left Join)
        df_feature_store = df_daily_price.join(df_macro_pivoted, "date_parsed", "left")
        
        # Lakukan Forward Fill untuk nilai makro yang kosong
        # karena data makro mungkin tahunan/bulanan, sedangkan harga harian
        window_ffill = Window.orderBy("date_parsed").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        
        for c in df_macro_pivoted.columns:
            if c != "date_parsed":
                df_feature_store = df_feature_store.withColumn(c, last(col(c), ignorenulls=True).over(window_ffill))
    else:
        df_feature_store = df_daily_price
        
    # Simpan ke Gold
    gold_feature_path = str(GOLD_DIR / "feature_store")
    df_feature_store.write.format("delta").mode("overwrite").save(gold_feature_path)
    log.info(f"Berhasil membuat tabel Gold (Feature Store) di {gold_feature_path}")

    spark.stop()

if __name__ == "__main__":
    process_gold()
