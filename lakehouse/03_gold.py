"""
LUMBUNG — Gold layer: feature store + analitik
Owner: Yasykur (patched Ryan)

Menggabungkan (JOIN) tabel Silver untuk membuat master feature store.
Tabel Gold ini siap digunakan untuk Machine Learning dan Dashboard.

Menggunakan deltalake + pandas (tanpa PySpark/JVM).
"""

from __future__ import annotations
import json
import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import read_delta, write_delta, now_utc, SILVER_DIR, GOLD_DIR, BASE_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("gold_layer")

EXPORT_DIR = BASE_DIR / "temp_buffer" / "export"


def _parse_nested(value: Any) -> dict:
    """Parse kolom yang mungkin disimpan sebagai string JSON oleh Delta Lake."""
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return {}
    return {}


def _aggregate_weather(df_weather: pd.DataFrame) -> pd.DataFrame:
    """Aggregate weather per date: avg precipitation_sum & temperature_max."""
    if df_weather.empty:
        return pd.DataFrame(columns=["date_parsed", "precipitation_sum", "temperature_max"])

    daily_records = []
    for _, row in df_weather.iterrows():
        daily = _parse_nested(row.get("daily"))
        daily_records.append({
            "date_parsed": row.get("date_parsed"),
            "precipitation_sum": pd.to_numeric(daily.get("precipitation_sum"), errors="coerce"),
            "temperature_max": pd.to_numeric(daily.get("temperature_2m_max"), errors="coerce"),
        })

    df = pd.DataFrame(daily_records)
    df = df.dropna(subset=["date_parsed"])
    df_agg = df.groupby("date_parsed", as_index=False).agg(
        precipitation_sum=("precipitation_sum", "mean"),
        temperature_max=("temperature_max", "mean"),
    )
    df_agg["precipitation_sum"] = df_agg["precipitation_sum"].round(1)
    df_agg["temperature_max"] = df_agg["temperature_max"].round(1)
    return df_agg


def _aggregate_news(df_news: pd.DataFrame) -> pd.DataFrame:
    """Aggregate news per date: news_score (avg relevance) & news_velocity."""
    if df_news.empty:
        return pd.DataFrame(columns=["date_parsed", "news_score", "news_velocity"])

    df_news = df_news.copy()
    df_news["relevance_score"] = pd.to_numeric(df_news.get("relevance_score", 0), errors="coerce").fillna(0)
    df_news["date_parsed"] = df_news["date_parsed"].astype(str)

    daily = df_news.groupby("date_parsed", as_index=False).agg(
        news_score=("relevance_score", "mean"),
        article_count=("relevance_score", "size"),
    )
    daily["news_score"] = daily["news_score"].round(2)

    # Velocity = count today / rolling 7-day mean count
    daily = daily.sort_values("date_parsed").reset_index(drop=True)
    daily["news_velocity"] = (
        daily["article_count"]
        .rolling(window=7, min_periods=1)
        .mean()
    )
    daily["news_velocity"] = daily.apply(
        lambda r: round(r["article_count"] / r["news_velocity"], 2) if r["news_velocity"] > 0 else 0.0,
        axis=1,
    )

    return daily[["date_parsed", "news_score", "news_velocity"]]


def process_gold():
    # ── 1. Baca Silver Prices ────────────────────────────────────────────
    silver_price_path = str(SILVER_DIR / "silver_prices")
    df_prices = read_delta(silver_price_path)

    if df_prices.empty:
        log.error("Tabel silver_prices kosong, skip Gold.")
        return

    log.info(f"Membaca silver_prices: {len(df_prices)} rows")

    # Fix kolom names (backward compat)
    if "commodity" in df_prices.columns and "komoditas" not in df_prices.columns:
        df_prices["komoditas"] = df_prices["commodity"]
    if "price" in df_prices.columns and "price_idr_per_kg" not in df_prices.columns:
        df_prices["price_idr_per_kg"] = df_prices["price"]

    df_prices["price_idr_per_kg"] = pd.to_numeric(df_prices["price_idr_per_kg"], errors="coerce")

    # Agregasi harga harian per komoditas (rata-rata jika ada banyak source)
    df_daily = df_prices.groupby(["date_parsed", "komoditas"], as_index=False).agg(
        avg_price=("price_idr_per_kg", "mean"),
        n_sources=("source", "nunique"),
    )
    df_daily["avg_price"] = df_daily["avg_price"].round(2)

    log.info(f"Agregasi harian: {len(df_daily)} rows, "
             f"{df_daily['komoditas'].nunique()} komoditas")

    # ── 2. Join dengan Silver Macro (jika ada) ───────────────────────────
    silver_macro_path = str(SILVER_DIR / "silver_macro")
    df_macro = read_delta(silver_macro_path)

    if not df_macro.empty:
        log.info(f"Membaca silver_macro: {len(df_macro)} rows")

        # Pivot indikator makro → kolom
        df_macro["value"] = pd.to_numeric(df_macro["value"], errors="coerce")
        df_macro_pivoted = df_macro.pivot_table(
            index="date_parsed",
            columns="indicator",
            values="value",
            aggfunc="first"
        ).reset_index()

        # Join ke prices
        df_feature_store = df_daily.merge(df_macro_pivoted, on="date_parsed", how="left")

        # Forward fill untuk nilai makro (data bulanan/tahunan, harga harian)
        macro_cols = [c for c in df_macro_pivoted.columns if c != "date_parsed"]
        df_feature_store = df_feature_store.sort_values("date_parsed")
        df_feature_store[macro_cols] = df_feature_store[macro_cols].ffill()
    else:
        df_feature_store = df_daily

    # ── 3. Join dengan Silver Kurs (jika ada) ────────────────────────────
    silver_kurs_path = str(SILVER_DIR / "silver_kurs")
    df_kurs = read_delta(silver_kurs_path)

    if not df_kurs.empty and "kurs_tengah" in df_kurs.columns:
        log.info(f"Membaca silver_kurs: {len(df_kurs)} rows")
        df_kurs["kurs_tengah"] = pd.to_numeric(df_kurs["kurs_tengah"], errors="coerce")
        df_kurs_daily = df_kurs.groupby("date_parsed", as_index=False).agg(
            kurs_usd_idr=("kurs_tengah", "mean")
        )
        df_feature_store = df_feature_store.merge(df_kurs_daily, on="date_parsed", how="left")
        df_feature_store["kurs_usd_idr"] = df_feature_store["kurs_usd_idr"].ffill()

    # ── 4. Join dengan Silver Weather ────────────────────────────────────
    silver_weather_path = str(SILVER_DIR / "silver_weather")
    df_weather = read_delta(silver_weather_path)
    if not df_weather.empty:
        log.info(f"Membaca silver_weather: {len(df_weather)} rows")
        df_weather_agg = _aggregate_weather(df_weather)
        df_feature_store = df_feature_store.merge(df_weather_agg, on="date_parsed", how="left")
        df_feature_store["precipitation_sum"] = df_feature_store["precipitation_sum"].fillna(0.0)
        df_feature_store["temperature_max"] = df_feature_store["temperature_max"].fillna(0.0)

    # ── 5. Join dengan Silver News ───────────────────────────────────────
    silver_news_path = str(SILVER_DIR / "silver_news")
    df_news = read_delta(silver_news_path)
    if not df_news.empty:
        log.info(f"Membaca silver_news: {len(df_news)} rows")
        df_news_agg = _aggregate_news(df_news)
        df_feature_store = df_feature_store.merge(df_news_agg, on="date_parsed", how="left")
        df_feature_store["news_score"] = df_feature_store["news_score"].fillna(0.0)
        df_feature_store["news_velocity"] = df_feature_store["news_velocity"].fillna(0.0)

    # ── 6. Tambah metadata & simpan ──────────────────────────────────────
    df_feature_store["_gold_created_at"] = now_utc()

    gold_path = str(GOLD_DIR / "feature_store")
    write_delta(df_feature_store, gold_path, mode="overwrite")
    log.info(f"Gold feature_store: {len(df_feature_store)} rows -> {gold_path}")

    # ── 5. Export JSON untuk dashboard ───────────────────────────────────
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    export_path = EXPORT_DIR / "feature_store.json"
    records = df_feature_store.to_dict(orient="records")
    with open(export_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)
    log.info(f"Exported ke {export_path}")

    log.info("Gold layer selesai.")


if __name__ == "__main__":
    process_gold()
