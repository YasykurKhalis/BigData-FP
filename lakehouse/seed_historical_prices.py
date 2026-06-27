"""
LUMBUNG — Seed Historical Price Data
Owner: tim (refactored Ryan)

Membuat data harga historis realistis (180 hari ke belakang) dalam format JSONL
agar pipeline Lakehouse dan model ML dapat dilatih.

Data menggunakan harga dasar aktual Indonesia (Rp/kg) dengan simulasi volatilitas
realistis berdasarkan pola musiman dan historis masing-masing komoditas.

Schema output sesuai producer baru:
  komoditas, price_idr_per_kg, source, data_source, level_harga,
  currency, unit, country, fetched_at_utc, ingestion_ts

Output: temp_buffer/streaming/prices/bapanas/YYYY-MM-DD/historical_seed.jsonl
"""

from __future__ import annotations
import json
import logging
import os
import sys
import numpy as np
from datetime import datetime, timedelta, timezone
from pathlib import Path

# DNS patch untuk WebHDFS redirect
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "hdfs"))
from _dns_patch import patch_dns
patch_dns()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("seed_historical")

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "temp_buffer" / "streaming" / "prices" / "bapanas"

WEBHDFS_URL = os.getenv("WEBHDFS_URL", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
HDFS_ROOT = "/data/lumbung"

# Harga dasar aktual Juni 2026 (Rp/kg) berdasarkan data PIHPS/Bapanas nyata
BASE_PRICES = {
    "beras":             13500.0,
    "cabai_rawit_merah": 83500.0,
    "cabai_keriting":    52000.0,
    "bawang_merah":      37000.0,
    "bawang_putih":      41000.0,
    "gula_pasir":        20300.0,
    "minyak_goreng":     20550.0,
    "daging_ayam":       37200.0,
    "telur_ayam":        29750.0,
    "daging_sapi":       149200.0,
}

# Volatilitas harian (standar deviasi sebagai persen dari harga)
VOLATILITY = {
    "beras":             0.003,
    "cabai_rawit_merah": 0.045,
    "cabai_keriting":    0.038,
    "bawang_merah":      0.028,
    "bawang_putih":      0.018,
    "gula_pasir":        0.005,
    "minyak_goreng":     0.008,
    "daging_ayam":       0.015,
    "telur_ayam":        0.012,
    "daging_sapi":       0.005,
}


# Pola musiman sederhana (lonjakan di bulan tertentu)
def seasonal_factor(komoditas: str, month: int) -> float:
    seasons = {
        "cabai_rawit_merah": {1: 1.3, 2: 1.2, 6: 1.25, 7: 1.15, 12: 1.2},
        "cabai_keriting":    {1: 1.2, 2: 1.15, 6: 1.1, 12: 1.1},
        "bawang_merah":      {1: 1.1, 2: 1.15, 3: 0.9, 8: 0.9},
        "bawang_putih":      {3: 1.1, 4: 1.05},
        "beras":             {1: 1.0, 8: 0.95, 9: 0.92, 10: 0.95},
        "gula_pasir":        {5: 1.05, 6: 1.08, 7: 1.05},       # giling musim kemarau
        "minyak_goreng":     {1: 1.05, 12: 1.05},                # permintaan akhir tahun
        "daging_ayam":       {1: 1.1, 4: 1.15, 6: 1.1, 12: 1.2},# Lebaran & Natal
        "telur_ayam":        {1: 1.08, 6: 1.1, 12: 1.12},       # hari raya
        "daging_sapi":       {1: 1.1, 4: 1.2, 6: 1.15, 12: 1.15},# Lebaran & akhir tahun
    }
    return seasons.get(komoditas, {}).get(month, 1.0)


def generate_historical_prices(days: int = 180) -> list[dict]:
    rng = np.random.default_rng(seed=2026)
    today = datetime.now(timezone.utc).date()
    records = []

    for komoditas, base_price in BASE_PRICES.items():
        vol = VOLATILITY[komoditas]
        price = base_price

        for i in range(days, 0, -1):
            date = today - timedelta(days=i)
            month = date.month

            sf = seasonal_factor(komoditas, month)
            target = base_price * sf
            shock = rng.normal(0, vol)
            mean_reversion = 0.02 * (target - price) / target
            price = max(base_price * 0.4, price * (1.0 + shock + mean_reversion))

            ts = datetime(date.year, date.month, date.day,
                          12, 0, 0, tzinfo=timezone.utc).isoformat()

            records.append({
                "source":            "bapanas",
                "data_source":       "historical-seed",
                "komoditas":         komoditas,
                "level_harga":       "konsumen",
                "price_idr_per_kg":  round(float(price), 0),
                "currency":          "IDR",
                "unit":              "kg",
                "country":           "ID",
                "fetched_at_utc":    ts,
                "ingestion_ts":      ts,
                "_is_historical_seed": True,
            })

    return records


def _get_hdfs_client():
    """Return WebHDFS client atau None."""
    try:
        from hdfs import InsecureClient
        client = InsecureClient(WEBHDFS_URL, user=HDFS_USER)
        client.status("/")
        log.info(f"HDFS connected: {WEBHDFS_URL}")
        return client
    except Exception as e:
        log.warning(f"HDFS unavailable: {e}")
        return None


def save_historical_prices(records: list[dict]) -> None:
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)

    # Tulis ke HDFS
    client = _get_hdfs_client()
    if client:
        hdfs_dir = f"{HDFS_ROOT}/streaming/prices/bapanas/{today_str}"
        hdfs_path = f"{hdfs_dir}/historical_seed.jsonl"
        try:
            client.makedirs(hdfs_dir)
            with client.write(hdfs_path, encoding="utf-8", overwrite=True) as w:
                w.write(payload)
            log.info(f"Disimpan {len(records)} record historis ke HDFS:{hdfs_path}")
        except Exception as e:
            log.error(f"Gagal tulis ke HDFS: {e}")

    # Juga tulis lokal (backup)
    out_dir = OUTPUT_DIR / today_str
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "historical_seed.jsonl"
    out_path.write_text(payload, encoding="utf-8")
    log.info(f"Disimpan {len(records)} record historis ke {out_path}")


if __name__ == "__main__":
    log.info("Membuat data historis harga 180 hari (realistis)...")
    records = generate_historical_prices(days=180)
    save_historical_prices(records)

    from collections import Counter
    counts = Counter(r["komoditas"] for r in records)
    for kom, n in counts.items():
        prices = [r["price_idr_per_kg"] for r in records if r["komoditas"] == kom]
        log.info(f"  {kom:25s}: {n} hari | min={min(prices):,.0f} avg={sum(prices)/len(prices):,.0f} max={max(prices):,.0f} Rp/kg")
