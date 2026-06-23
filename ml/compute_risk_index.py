"""
LUMBUNG — Hitung Indeks Risiko Lonjakan Harga (0–100)
Owner: Yasykur

Menggabungkan tiga sinyal utama:
  1. Tren Harga (price_signal)   — momentum dan deviasi dari rata-rata 30 hari
  2. Sinyal Cuaca (weather_signal) — anomali cuaca di sentra produksi
  3. Sinyal Berita (news_signal)   — velocity dan skor NLP berita gangguan pasokan

Indeks 0–100:
  < 40  = Aman
  40–60 = Waspada
  60–80 = Siaga
  > 80  = Kritis

Input :
  - temp_buffer/export/feature_store.json  (dari Gold layer)
  - temp_buffer/export/nlp_signals.json    (dari NLP extractor)
  - temp_buffer/streaming/weather/**/*.jsonl

Output:
  - temp_buffer/export/risk_index.json
"""

from __future__ import annotations
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("risk_index")

BASE_DIR   = Path(__file__).resolve().parent.parent
EXPORT_DIR = BASE_DIR / "temp_buffer" / "export"
WEATHER_DIR = BASE_DIR / "temp_buffer" / "streaming" / "weather"

# Bobot fusi tiga sinyal (jumlah = 1.0)
W_PRICE   = 0.50
W_WEATHER = 0.25
W_NEWS    = 0.25

KOMODITAS_LIST = [
    "beras",
    "cabai_rawit_merah",
    "cabai_keriting",
    "bawang_merah",
    "bawang_putih",
]

# Ambang batas anomali cuaca
PRECIP_THRESHOLD_MM  = 50.0   # curah hujan hari ini > 50 mm = anomali
TEMP_DEVIATION_C     = 3.0    # suhu menyimpang > 3°C dari normal = anomali


# ── Loader ────────────────────────────────────────────────────────────────────

def _load_json(path: Path) -> dict | list | None:
    if not path.exists():
        log.warning(f"File tidak ditemukan: {path}")
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _load_weather_jsonl() -> list[dict]:
    records: list[dict] = []
    if not WEATHER_DIR.exists():
        return records
    for f in sorted(WEATHER_DIR.rglob("*.jsonl")):
        try:
            with open(f, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))
        except Exception as e:
            log.warning(f"Gagal membaca weather file {f}: {e}")
    return records


# ── Kalkulasi Sinyal ──────────────────────────────────────────────────────────

def compute_price_signal(
    feature_store: list[dict],
    komoditas: str,
    lookback_days: int = 30,
) -> float:
    """
    Sinyal harga 0–100 berdasarkan:
      - Perubahan harga 7 hari terakhir (momentum)
      - Deviasi harga hari ini dari rata-rata 30 hari (z-score dinormalisasi)
    """
    # Filter data per komoditas
    rows = [r for r in feature_store if r.get("commodity") == komoditas and r.get("avg_price")]
    if not rows:
        return 50.0  # data tidak tersedia, nilai tengah

    # Urutkan berdasarkan tanggal
    rows.sort(key=lambda r: r.get("date_parsed") or r.get("date", ""))

    prices = [float(r["avg_price"]) for r in rows]
    today_price = prices[-1]

    # Momentum 7 hari
    if len(prices) >= 7:
        price_7d_ago = prices[-7]
        pct_change_7d = (today_price - price_7d_ago) / price_7d_ago * 100.0
    else:
        pct_change_7d = 0.0

    # Z-score terhadap 30 hari terakhir
    recent = prices[-lookback_days:]
    mean_30d = np.mean(recent)
    std_30d  = np.std(recent)
    z_score  = (today_price - mean_30d) / std_30d if std_30d > 0 else 0.0

    # Konversi ke skor 0-100
    # Momentum: +20% → skor 100, -20% → skor 0
    momentum_score = max(0.0, min(100.0, (pct_change_7d + 20.0) / 40.0 * 100.0))
    # Z-score: > +2 → skor 100, < -2 → skor 0
    zscore_score   = max(0.0, min(100.0, (z_score + 2.0) / 4.0 * 100.0))

    # Gabung dengan bobot sama
    return round(0.6 * momentum_score + 0.4 * zscore_score, 2)


def compute_weather_signal(
    weather_records: list[dict],
    komoditas: str,
    sentra_bobot: dict[str, float],
) -> float:
    """
    Sinyal cuaca 0–100 berdasarkan anomali curah hujan dan suhu
    di sentra-sentra produksi komoditas, dibobot sesuai bobot sentra.
    """
    if not weather_records:
        return 0.0

    # Ambil data cuaca terbaru per sentra
    latest: dict[str, dict] = {}
    for rec in weather_records:
        sentra = rec.get("sentra", "")
        if sentra in sentra_bobot:
            existing = latest.get(sentra)
            if existing is None or rec.get("ingestion_ts", "") > existing.get("ingestion_ts", ""):
                latest[sentra] = rec

    if not latest:
        return 0.0

    total_weight  = 0.0
    weighted_score = 0.0

    for sentra, bobot in sentra_bobot.items():
        rec = latest.get(sentra)
        if rec is None:
            continue

        current = rec.get("current") or {}
        daily   = rec.get("daily")   or {}

        score = 0.0

        # Periksa curah hujan
        precip = current.get("precipitation") or 0.0
        if precip > PRECIP_THRESHOLD_MM:
            score += min(50.0, (precip / PRECIP_THRESHOLD_MM - 1.0) * 30.0 + 50.0)

        # Periksa kode cuaca WMO (kode ≥ 60 = hujan lebat / badai)
        wmo_code = current.get("weather_code") or 0
        if wmo_code >= 80:   # hujan lebat / badai petir
            score += 40.0
        elif wmo_code >= 60: # hujan sedang
            score += 20.0

        # Periksa suhu ekstrem (>= 35°C atau <= 15°C)
        temp = current.get("temperature_2m")
        if temp is not None:
            if temp >= 35.0:
                score += 15.0
            elif temp <= 15.0:
                score += 10.0

        score = min(100.0, score)
        weighted_score += bobot * score
        total_weight   += bobot

    return round(weighted_score / total_weight if total_weight > 0 else 0.0, 2)


def compute_news_signal(nlp_data: dict, komoditas: str) -> float:
    """
    Sinyal berita 0–100 berdasarkan:
      - Velocity (percepatan munculnya berita risiko)
      - Skor rata-rata sinyal artikel
    """
    velocity_data = nlp_data.get("velocity_per_komoditas", {})
    kom_data = velocity_data.get(komoditas, {})

    if not kom_data:
        return 0.0

    velocity  = float(kom_data.get("velocity", 0))
    avg_score = float(kom_data.get("avg_score", 0))

    # Velocity > 3x dari rata-rata → skor 100
    velocity_score = min(100.0, (velocity / 3.0) * 100.0)

    # avg_score (biasanya 0–15) → normalisasi ke 0-100
    score_normalized = min(100.0, avg_score / 15.0 * 100.0)

    return round(0.6 * velocity_score + 0.4 * score_normalized, 2)


# ── Agregasi ──────────────────────────────────────────────────────────────────

def classify_risk(index: float) -> str:
    """Klasifikasi level risiko dari nilai indeks."""
    if index >= 80:
        return "KRITIS"
    if index >= 60:
        return "SIAGA"
    if index >= 40:
        return "WASPADA"
    return "AMAN"


def compute_all_risk_indices() -> dict[str, Any]:
    """
    Hitung Indeks Risiko untuk semua komoditas dan simpan ke JSON.
    """
    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).resolve().parent))
    from sentra_mapper import SENTRA_MAP

    # Load data
    feature_store_raw = _load_json(EXPORT_DIR / "feature_store.json")
    nlp_data          = _load_json(EXPORT_DIR / "nlp_signals.json") or {}
    weather_records   = _load_weather_jsonl()

    # Flatten feature_store (JSON lines atau list)
    if isinstance(feature_store_raw, dict):
        # Delta Lake export → biasanya list of records dalam field tertentu
        feature_store: list[dict] = []
    elif isinstance(feature_store_raw, list):
        feature_store = feature_store_raw
    else:
        feature_store = []

    log.info(
        f"Data dimuat: {len(feature_store)} price rows, "
        f"{len(weather_records)} weather records, "
        f"NLP: {'ada' if nlp_data else 'tidak ada'}"
    )

    results: dict[str, dict] = {}

    for komoditas in KOMODITAS_LIST:
        # Bobot sentra untuk cuaca
        sentra_bobot: dict[str, float] = {
            s["nama"]: s["bobot"]
            for s in SENTRA_MAP.get(komoditas, [])
        }

        price_sig   = compute_price_signal(feature_store, komoditas)
        weather_sig = compute_weather_signal(weather_records, komoditas, sentra_bobot)
        news_sig    = compute_news_signal(nlp_data, komoditas)

        # Fusi tertimbang
        risk_index = round(
            W_PRICE   * price_sig
            + W_WEATHER * weather_sig
            + W_NEWS    * news_sig,
            2,
        )

        level = classify_risk(risk_index)

        results[komoditas] = {
            "komoditas":     komoditas,
            "risk_index":    risk_index,
            "level":         level,
            "components": {
                "price_signal":   price_sig,
                "weather_signal": weather_sig,
                "news_signal":    news_sig,
            },
            "weights": {
                "price":   W_PRICE,
                "weather": W_WEATHER,
                "news":    W_NEWS,
            },
        }

        log.info(
            f"  {komoditas:25s} | risk={risk_index:5.1f} [{level:7s}] "
            f"P={price_sig:.1f} W={weather_sig:.1f} N={news_sig:.1f}"
        )

    output: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "risk_indices": results,
        "metadata": {
            "weights": {"price": W_PRICE, "weather": W_WEATHER, "news": W_NEWS},
            "thresholds": {"aman": 40, "waspada": 60, "siaga": 80, "kritis": 100},
        },
    }

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = EXPORT_DIR / "risk_index.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info(f"Indeks risiko disimpan ke {out_path}")
    return output


if __name__ == "__main__":
    result = compute_all_risk_indices()
    print("\n=== INDEKS RISIKO LONJAKAN HARGA ===")
    for kom, data in result["risk_indices"].items():
        comp = data["components"]
        print(
            f"  {data['komoditas']:25s} | "
            f"INDEX={data['risk_index']:5.1f}  [{data['level']:7s}]  "
            f"harga={comp['price_signal']:.1f}  "
            f"cuaca={comp['weather_signal']:.1f}  "
            f"berita={comp['news_signal']:.1f}"
        )
    sys.exit(0)
