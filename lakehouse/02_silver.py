"""
LUMBUNG — Silver layer: cleaning, dedup, schema standardization
Owner: Yasykur (refactored Ryan)

Membaca dari Bronze, menghapus duplikat, standarisasi schema,
dan menyimpan ke layer Silver (Delta Lake).

Menggunakan deltalake + pandas (tanpa PySpark/JVM).
"""

from __future__ import annotations
import logging
import os
import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import read_delta, write_delta, now_utc, BRONZE_DIR, SILVER_DIR, get_hdfs_client, WEBHDFS_URL, HDFS_USER

# Patch DNS untuk resolve container hostname -> localhost
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "hdfs"))
from _dns_patch import patch_dns
patch_dns()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("silver_layer")

CANONICAL_KOMODITAS = {
    "beras", "cabai_rawit_merah", "cabai_keriting", "bawang_merah", "bawang_putih"
}


def process_silver():
    # ── 1. Silver Prices ─────────────────────────────────────────────────
    price_tables = ["price_bapanas", "price_pihps", "price_siskaperbapo"]
    all_prices = []

    for tbl in price_tables:
        path = str(BRONZE_DIR / tbl)
        df = read_delta(path)
        if df.empty:
            log.warning(f"Tidak ada data di {tbl}, lewati.")
            continue

        log.info(f"Memproses {tbl}: {len(df)} rows")
        cols = set(df.columns)

        # Parse tanggal
        if "fetched_at_utc" in cols:
            df["date_parsed"] = pd.to_datetime(df["fetched_at_utc"], format="ISO8601", errors="coerce").dt.date.astype(str)
        elif "ingestion_ts" in cols:
            df["date_parsed"] = pd.to_datetime(df["ingestion_ts"], format="ISO8601", errors="coerce").dt.date.astype(str)
        else:
            df["date_parsed"] = "1970-01-01"

        # Tentukan kolom komoditas dan harga
        komoditas_col = "komoditas" if "komoditas" in cols else "commodity"
        price_col = "price_idr_per_kg" if "price_idr_per_kg" in cols else "price"

        df_clean = df[[
            "source",
            "data_source" if "data_source" in cols else "source",
            komoditas_col,
            price_col,
            "date_parsed",
        ]].copy()

        df_clean.columns = ["source", "data_source", "komoditas", "price_idr_per_kg", "date_parsed"]

        # Cast harga ke float
        df_clean["price_idr_per_kg"] = pd.to_numeric(df_clean["price_idr_per_kg"], errors="coerce")

        # Filter hanya 5 komoditas canonical
        df_clean = df_clean[df_clean["komoditas"].isin(CANONICAL_KOMODITAS)]

        # Dedup
        df_clean = df_clean.drop_duplicates(subset=["source", "komoditas", "date_parsed"])

        all_prices.append(df_clean)

    if all_prices:
        df_prices = pd.concat(all_prices, ignore_index=True)
        silver_price_path = str(SILVER_DIR / "silver_prices")
        write_delta(df_prices, silver_price_path, mode="overwrite")

        # Push the Silver table to HDFS
        try:
            from hdfs import InsecureClient
            hdfs_client = InsecureClient(WEBHDFS_URL, user=HDFS_USER)
            hdfs_target = f"/data/lumbung/lakehouse/silver/{os.path.basename(silver_price_path)}"
            hdfs_client.makedirs(hdfs_target)
            for root, _, files in os.walk(silver_price_path):
                for f in files:
                    local_file = os.path.join(root, f)
                    rel_path = os.path.relpath(local_file, silver_price_path)
                    hdfs_path = f"{hdfs_target}/{rel_path}"
                    hdfs_client.upload(hdfs_path, local_file, overwrite=True)
            log.info(f"Pushed silver_prices to HDFS {hdfs_target}")
        except Exception as e:
            log.warning(f"HDFS push skipped for silver_prices: {e}")
        log.info(f"silver_prices: {len(df_prices)} rows -> {silver_price_path}")
    else:
        log.warning("Tidak ada data harga untuk silver_prices")

    # ── 2. Silver Kurs ───────────────────────────────────────────────────
    kurs_path = str(BRONZE_DIR / "kurs")
    df_kurs = read_delta(kurs_path)
    if not df_kurs.empty:
        log.info(f"Memproses kurs: {len(df_kurs)} rows")
        cols = set(df_kurs.columns)

        if "fetched_at_utc" in cols:
            df_kurs["date_parsed"] = pd.to_datetime(df_kurs["fetched_at_utc"], format="ISO8601", errors="coerce").dt.date.astype(str)
        elif "ingestion_ts" in cols:
            df_kurs["date_parsed"] = pd.to_datetime(df_kurs["ingestion_ts"], format="ISO8601", errors="coerce").dt.date.astype(str)

        keep_cols = ["source", "date_parsed"]
        for c in ["kurs_jual", "kurs_beli", "kurs_tengah", "pair", "data_source"]:
            if c in cols:
                keep_cols.append(c)

        df_kurs_clean = df_kurs[keep_cols].drop_duplicates(subset=["source", "date_parsed"])
        silver_kurs_path = str(SILVER_DIR / "silver_kurs")
        write_delta(df_kurs_clean, silver_kurs_path, mode="overwrite")
        log.info(f"silver_kurs: {len(df_kurs_clean)} rows -> {silver_kurs_path}")

    # ── 3. Silver News ───────────────────────────────────────────────────
    news_path = str(BRONZE_DIR / "news")
    df_news = read_delta(news_path)
    if not df_news.empty:
        log.info(f"Memproses news: {len(df_news)} rows")
        cols = set(df_news.columns)

        if "article_id" in cols:
            df_news = df_news.drop_duplicates(subset=["article_id"])

        if "fetched_at_utc" in cols:
            df_news["date_parsed"] = pd.to_datetime(df_news["fetched_at_utc"], format="ISO8601", errors="coerce").dt.date.astype(str)
        elif "ingestion_ts" in cols:
            df_news["date_parsed"] = pd.to_datetime(df_news["ingestion_ts"], format="ISO8601", errors="coerce").dt.date.astype(str)

        silver_news_path = str(SILVER_DIR / "silver_news")
        write_delta(df_news, silver_news_path, mode="overwrite")
        log.info(f"silver_news: {len(df_news)} rows -> {silver_news_path}")

    # ── 4. Silver Weather ────────────────────────────────────────────────
    weather_path = str(BRONZE_DIR / "weather")
    df_weather = read_delta(weather_path)
    if not df_weather.empty:
        log.info(f"Memproses weather: {len(df_weather)} rows")
        cols = set(df_weather.columns)

        if "fetched_at_utc" in cols:
            df_weather["date_parsed"] = pd.to_datetime(df_weather["fetched_at_utc"], format="ISO8601", errors="coerce").dt.date.astype(str)
        elif "ingestion_ts" in cols:
            df_weather["date_parsed"] = pd.to_datetime(df_weather["ingestion_ts"], format="ISO8601", errors="coerce").dt.date.astype(str)

        if "sentra" in cols:
            df_weather = df_weather.drop_duplicates(subset=["sentra", "date_parsed"])

        silver_weather_path = str(SILVER_DIR / "silver_weather")
        write_delta(df_weather, silver_weather_path, mode="overwrite")
        log.info(f"silver_weather: {len(df_weather)} rows -> {silver_weather_path}")

    # ── 5. Silver Macro ──────────────────────────────────────────────────
    macro_tables = ["batch_produksi", "batch_imporekspor", "batch_bulog_stok", "batch_pupuk_harga"]
    all_macro = []

    for tbl in macro_tables:
        path = str(BRONZE_DIR / tbl)
        df = read_delta(path)
        if df.empty:
            continue

        log.info(f"Memproses {tbl}: {len(df)} rows")
        cols = set(df.columns)

        if "date" in cols:
            df["date_parsed"] = pd.to_datetime(df["date"], errors="coerce").dt.date.astype(str)
        elif "year" in cols:
            df["date_parsed"] = df["year"].astype(str) + "-01-01"
        elif "ingestion_ts" in cols:
            df["date_parsed"] = pd.to_datetime(df["ingestion_ts"], format="ISO8601", errors="coerce").dt.date.astype(str)

        if "close_price" in cols and "value" not in cols:
            df["value"] = df["close_price"]

        needed = ["source", "indicator", "date_parsed", "value"]
        available = [c for c in needed if c in df.columns]
        if len(available) == len(needed):
            df_clean = df[needed].drop_duplicates(subset=["source", "indicator", "date_parsed"])
            all_macro.append(df_clean)

    if all_macro:
        df_macro = pd.concat(all_macro, ignore_index=True)
        silver_macro_path = str(SILVER_DIR / "silver_macro")
        write_delta(df_macro, silver_macro_path, mode="overwrite")
        log.info(f"silver_macro: {len(df_macro)} rows -> {silver_macro_path}")

    log.info("Silver layer selesai.")


if __name__ == "__main__":
    process_silver()
