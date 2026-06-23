"""
LUMBUNG — Demo Delta Lake time-travel
Owner: Yasykur

Skrip ini mendemonstrasikan kapabilitas Time Travel dari Delta Lake,
yang memungkinkan kita melihat riwayat perubahan tabel dan mengakses
versi data historis.
"""

from __future__ import annotations
import sys
from pathlib import Path

# Tambahkan sys.path untuk utils
sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import get_spark_session

BASE_DIR = Path(__file__).resolve().parent.parent
GOLD_DIR = BASE_DIR / "temp_buffer" / "lakehouse" / "gold"

def run_time_travel_demo():
    print("=" * 60)
    print("DEMO: Delta Lake Time Travel pada Tabel Gold (Feature Store)")
    print("=" * 60)
    
    spark = get_spark_session("Lumbung_TimeTravel")
    
    feature_store_path = str(GOLD_DIR / "feature_store")
    
    try:
        from delta.tables import DeltaTable
        
        if not DeltaTable.isDeltaTable(spark, feature_store_path):
            print(f"[!] Tabel bukan merupakan tabel Delta: {feature_store_path}")
            return
            
        dt = DeltaTable.forPath(spark, feature_store_path)
        
        print("\n1. Melihat Riwayat Perubahan (DESCRIBE HISTORY):")
        history_df = dt.history()
        history_df.select("version", "timestamp", "operation", "operationMetrics").show(truncate=False)
        
        # Ambil versi terakhir
        versions = history_df.select("version").rdd.flatMap(lambda x: x).collect()
        if len(versions) > 1:
            old_version = versions[-1] # Versi paling awal biasanya yang terakhir di daftar desc
            print(f"\n2. Mengakses Versi Historis (VERSION AS OF {old_version}):")
            df_old = spark.read.format("delta").option("versionAsOf", old_version).load(feature_store_path)
            print(f"   Jumlah baris pada versi {old_version}: {df_old.count()}")
            
            latest_version = versions[0]
            print(f"\n3. Mengakses Versi Terbaru (VERSION AS OF {latest_version}):")
            df_new = spark.read.format("delta").option("versionAsOf", latest_version).load(feature_store_path)
            print(f"   Jumlah baris pada versi terbaru: {df_new.count()}")
            
            print("\n4. Menampilkan sampel data versi lama:")
            df_old.show(5)
        else:
            print("\n[!] Tabel hanya memiliki 1 versi. Jalankan proses Silver dan Gold lagi untuk membuat versi baru.")
            
    except Exception as e:
        print(f"\n[!] Error saat demonstrasi Time Travel: {e}")
        print("Pastikan pustaka delta-spark terpasang dengan benar dan tabel Gold sudah terbuat.")
        
    spark.stop()
    print("\nDemo Selesai.")

if __name__ == "__main__":
    run_time_travel_demo()
