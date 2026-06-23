"""
LUMBUNG — Seed Historical Price Data
Owner: tim

Membuat data harga historis realistis (180 hari ke belakang) dalam format JSONL
agar pipeline Lakehouse dan model ML dapat dilatih.

Data menggunakan harga dasar aktual Indonesia (Rp/kg) dengan simulasi volatilitas
realistis berdasarkan pola musiman dan historis masing-masing komoditas.

Output: temp_buffer/streaming/prices/bapanas/YYYY-MM-DD/historical_seed.jsonl
"""

from __future__ import annotations
import json
import logging
import numpy as np
from datetime import datetime, timedelta, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("seed_historical")

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "temp_buffer" / "streaming" / "prices" / "bapanas"

# Harga dasar aktual Juni 2026 (Rp/kg) berdasarkan data PIHPS/Bapanas nyata
BASE_PRICES = {
    "beras":             13500.0,
    "cabai_rawit_merah": 83500.0,  # volatil, sedang tinggi Jun 2026
    "cabai_keriting":    52000.0,
    "bawang_merah":      37000.0,
    "bawang_putih":      41000.0,
}

# Volatilitas harian (standar deviasi sebagai persen dari harga)
VOLATILITY = {
    "beras":             0.003,   # sangat stabil
    "cabai_rawit_merah": 0.045,   # sangat volatil
    "cabai_keriting":    0.038,
    "bawang_merah":      0.028,
    "bawang_putih":      0.018,
}

# Pola musiman sederhana (lonjakan di bulan tertentu)
# 1 = normal, >1 = musim mahal, <1 = musim murah
def seasonal_factor(komoditas: str, month: int) -> float:
    seasons = {
        "cabai_rawit_merah": {1: 1.3, 2: 1.2, 6: 1.25, 7: 1.15, 12: 1.2},
        "cabai_keriting":    {1: 1.2, 2: 1.15, 6: 1.1, 12: 1.1},
        "bawang_merah":      {1: 1.1, 2: 1.15, 3: 0.9, 8: 0.9},
        "bawang_putih":      {3: 1.1, 4: 1.05},
        "beras":             {1: 1.0, 8: 0.95, 9: 0.92, 10: 0.95},
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

            # Faktor musiman
            sf = seasonal_factor(komoditas, month)

            # Random walk dengan mean-reversion lemah ke harga dasar musiman
            target = base_price * sf
            shock = rng.normal(0, vol)
            mean_reversion = 0.02 * (target - price) / target
            price = max(base_price * 0.4, price * (1.0 + shock + mean_reversion))

            records.append({
                "source":       "bapanas_proxy",
                "commodity":    komoditas,
                "date":         str(date),
                "price":        round(float(price), 0),
                "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
                "ingestion_ts":   datetime.now(timezone.utc).isoformat(),
                "_is_historical_seed": True,
            })

    return records


def save_historical_prices(records: list[dict]) -> None:
    # Tulis ke direktori tanggal hari ini
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = OUTPUT_DIR / today_str
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "historical_seed.jsonl"

    with open(out_path, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    log.info(f"Disimpan {len(records)} record historis ke {out_path}")


if __name__ == "__main__":
    log.info("Membuat data historis harga 180 hari (realistis)...")
    records = generate_historical_prices(days=180)
    save_historical_prices(records)

    # Statistik
    from collections import Counter
    counts = Counter(r["commodity"] for r in records)
    for kom, n in counts.items():
        prices = [r["price"] for r in records if r["commodity"] == kom]
        log.info(f"  {kom:25s}: {n} hari | min={min(prices):,.0f} avg={sum(prices)/len(prices):,.0f} max={max(prices):,.0f} Rp/kg")
