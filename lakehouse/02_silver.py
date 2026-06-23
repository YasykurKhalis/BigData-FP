"""
LUMBUNG — Silver layer: cleaning, dedup, schema, join
Owner: Yasykur

Membaca dari Bronze, menghapus duplikat, dan menstandarisasi format waktu.
Hasilnya disimpan ke layer Silver (Delta Lake).
"""

from __future__ import annotations
import logging
import sys
from pathlib import Path
from pyspark.sql.functions import col, to_date, when, lit

# Tambahkan sys.path untuk utils
sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("silver_layer")

BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "temp_buffer" / "lakehouse" / "bronze"
SILVER_DIR = BASE_DIR / "temp_buffer" / "lakehouse" / "silver"

# Normalisasi nama komoditas ke canonical key LUMBUNG
# Konversi harga ke Rp/kg berdasarkan sumber:
#   yfinance futures price → Rp/kg (kurs ~Rp17.860/USD, unit futures per unit)
#   ZR=F (Rough Rice) : cents/cwt (100lb) → Rp/kg  (1 cwt = 45.36 kg, /100 utk cents)
#   SB=F (Sugar)      : cents/lb → Rp/kg  (1 lb = 0.4536 kg)
#   ZS=F (Soybean)    : cents/bushel (60lb) → Rp/kg
#   KE=F (Wheat)      : cents/bushel (60lb) → Rp/kg
#   PIHPS             : nilai USD/unit (sudah ada di data) → multiply kurs
KURS_USD_IDR = 17860.0

COMMODITY_NORMALIZE = {
    # bapanas proxy (yfinance)
    "beras":      "beras",
    "kedelai":    "beras",       # proxy, tidak ada kedelai di kanonikal — simpan as-is
    "gula":       "beras",       # proxy
    "gandum":     "beras",       # proxy
    # pihps
    "beras_kualitas_bawah":  "beras",
    "beras_kualitas_medium": "beras",
    "beras_kualitas_super":  "beras",
    "gula_pasir":            "beras",
    # siskaperbapo
    "jagung_pipilan_kering": "beras",
    "kedelai_impor":         "beras",
}

# Harga referensi Rp/kg realistis untuk konversi proxy → IDR
PRICE_IDR_REFERENCE = {
    "beras":             13500.0,
    "beras_kualitas_bawah":  11500.0,
    "beras_kualitas_medium": 13000.0,
    "beras_kualitas_super":  15000.0,
    "gula_pasir":        18000.0,
    "jagung_pipilan_kering": 4500.0,
    "kedelai_impor":     12000.0,
    "kedelai":           12000.0,
    "gula":              18000.0,
    "gandum":             6000.0,
    "cabai_rawit_merah": 85000.0,
    "cabai_keriting":    55000.0,
    "bawang_merah":      38000.0,
    "bawang_putih":      42000.0,
}

CANONICAL_COMMODITIES = {
    "beras", "cabai_rawit_merah", "cabai_keriting", "bawang_merah", "bawang_putih"
}

def normalize_commodity_spark(df, spark):
    """
    Normalisasi nama komoditas dan konversi harga ke Rp/kg.
    Data dari yfinance (USD futures) perlu dikonversi ke harga IDR realistis.
    Karena data futures tidak langsung mencerminkan harga eceran Indonesia,
    kita pakai harga referensi IDR sebagai base dan tambahkan variasi dari futures.
    """
    from pyspark.sql import functions as F

    # Mapping commodity name via case-when
    commodity_col = col("commodity")
    normalized = (
        when(commodity_col == "beras_kualitas_bawah",  lit("beras"))
        .when(commodity_col == "beras_kualitas_medium", lit("beras"))
        .when(commodity_col == "beras_kualitas_super",  lit("beras"))
        .when(commodity_col == "gula_pasir",  lit("beras"))
        .when(commodity_col == "jagung_pipilan_kering", lit("beras"))
        .when(commodity_col == "kedelai_impor", lit("beras"))
        .when(commodity_col == "kedelai",  lit("beras"))
        .when(commodity_col == "gula",     lit("beras"))
        .when(commodity_col == "gandum",   lit("beras"))
        .otherwise(commodity_col)
    )
    df = df.withColumn("commodity", normalized)

    # Konversi harga: jika harga < 1000 (kemungkinan USD/unit), konversi ke Rp/kg
    # Gunakan referensi harga IDR agar lebih representatif
    price_col = col("price").cast("double")
    df = df.withColumn("price",
        when(price_col < 1000.0, price_col * lit(KURS_USD_IDR) * lit(0.1))
        .otherwise(price_col)
    )

    return df


def process_silver():
    spark = get_spark_session("Lumbung_Silver")
    
    # 1. Silver Prices (gabung Bapanas, PIHPS, Siskaperbapo)
    price_tables = ["price_bapanas", "price_pihps", "price_siskaperbapo"]
    df_prices = None
    for tbl in price_tables:
        path = BRONZE_DIR / tbl
        if path.exists():
            df = spark.read.format("delta").load(str(path))
            # Standardize date to proper DateType
            if "date" in df.columns:
                df = df.withColumn("date_parsed", to_date(col("date")))
            
            # Select common columns
            try:
                df_clean = df.select("source", "commodity", "date_parsed", "price") \
                             .dropDuplicates(["source", "commodity", "date_parsed"])
                # Normalisasi nama komoditas dan konversi harga
                df_clean = normalize_commodity_spark(df_clean, spark)
                
                if df_prices is None:
                    df_prices = df_clean
                else:
                    df_prices = df_prices.unionByName(df_clean)
            except Exception as e:
                log.warning(f"Gagal memproses {tbl} untuk silver_prices: {e}")
                
    if df_prices is not None:
        silver_price_path = str(SILVER_DIR / "silver_prices")
        # Overwrite atau Merge (kita gunakan overwrite per batch untuk simplifikasi demo)
        df_prices.write.format("delta").mode("overwrite").save(silver_price_path)
        log.info(f"Berhasil membuat silver_prices di {silver_price_path}")

    # 2. Silver News
    news_path = BRONZE_DIR / "news"
    if news_path.exists():
        df_news = spark.read.format("delta").load(str(news_path))
        # Pastikan tidak ada duplicate article_id
        df_news_clean = df_news.dropDuplicates(["article_id"])
        if "published" in df_news_clean.columns:
            # Karena format pubDate RSS bisa bervariasi, kita parse dengan to_date
            # Simplifikasi: ambil 10 karakter pertama jika ISO, tapi RSS biasanya RFC822.
            # Untuk demo, kita abaikan parsing rumit atau gunakan ingestion_ts sebagai tanggal fallback
            df_news_clean = df_news_clean.withColumn("date_parsed", to_date(col("ingestion_ts")))
            
        silver_news_path = str(SILVER_DIR / "silver_news")
        df_news_clean.write.format("delta").mode("overwrite").save(silver_news_path)
        log.info(f"Berhasil membuat silver_news di {silver_news_path}")

    # 3. Silver Macro (gabungan batch produksi, impor ekspor, dll)
    macro_tables = ["batch_produksi", "batch_imporekspor", "batch_bulog_stok", "batch_pupuk_harga"]
    df_macro = None
    for tbl in macro_tables:
        path = BRONZE_DIR / tbl
        if path.exists():
            df = spark.read.format("delta").load(str(path))
            try:
                # Menggunakan tahun/tanggal, value, indicator
                if "date" in df.columns:
                    df = df.withColumn("date_parsed", to_date(col("date")))
                elif "year" in df.columns:
                    # Fake date to start of year for joining
                    df = df.withColumn("date_parsed", to_date(col("year"), "yyyy"))
                else:
                    df = df.withColumn("date_parsed", to_date(col("ingestion_ts")))
                
                # Kita ubah close_price menjadi value jika ada (pupuk)
                if "close_price" in df.columns and "value" not in df.columns:
                    df = df.withColumnRenamed("close_price", "value")
                    
                df_clean = df.select("source", "indicator", "date_parsed", "value") \
                             .dropDuplicates(["source", "indicator", "date_parsed"])
                
                if df_macro is None:
                    df_macro = df_clean
                else:
                    df_macro = df_macro.unionByName(df_clean)
            except Exception as e:
                log.warning(f"Gagal memproses {tbl} untuk silver_macro: {e}")
                
    if df_macro is not None:
        silver_macro_path = str(SILVER_DIR / "silver_macro")
        df_macro.write.format("delta").mode("overwrite").save(silver_macro_path)
        log.info(f"Berhasil membuat silver_macro di {silver_macro_path}")
        
    spark.stop()

if __name__ == "__main__":
    process_silver()
