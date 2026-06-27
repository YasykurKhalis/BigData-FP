"""
LUMBUNG — Big Data Synthetic Snapshot Generator
Owner: tim

Menghasilkan dataset streaming sintetis berskala besar yang mensimulasikan
beberapa tahun data pangan Indonesia. Dataset ini menggantikan kebutuhan
API key untuk demo/development dan memberikan cukup volume untuk melatih
model ML secara bermakna.

Big Data characteristics:
  • Volume   : puluhan ribu record multi-tahun
  • Variety  : harga (tabular), cuaca (semi-structured), berita (unstructured text)
  • Velocity : granularity harian, date-partitioned seperti stream Kafka
  • Veracity : distribusi realistis (seasonality, volatility cluster, shocks)

Output:
  temp_buffer/streaming/
  ├── prices/bapanas/YYYY-MM-DD/batch_HHMMSS.jsonl
  ├── prices/pihps/YYYY-MM-DD/batch_HHMMSS.jsonl
  ├── prices/siskaperbapo/YYYY-MM-DD/batch_HHMMSS.jsonl
  ├── weather/YYYY-MM-DD/batch_HHMMSS.jsonl
  ├── news/YYYY-MM-DD/batch_HHMMSS.jsonl
  └── kurs/YYYY-MM-DD/batch_HHMMSS.jsonl

USAGE:
  python scripts/generate_snapshot.py
  python scripts/generate_snapshot.py --years 5 --seed 42
  python scripts/generate_snapshot.py --push-to-hdfs
"""

from __future__ import annotations
import argparse
import hashlib
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("snapshot_generator")

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_ROOT = BASE_DIR / "temp_buffer" / "streaming"

KOMODITAS_LIST = [
    "beras",
    "cabai_rawit_merah",
    "cabai_keriting",
    "bawang_merah",
    "bawang_putih",
]

# Koordinat dan komoditas utama per sentra (sama dengan producer_weather.py)
SENTRA_PRODUKSI: dict[str, dict[str, Any]] = {
    "brebes":      {"lat": -6.872, "lon": 109.046, "komoditas": ["cabai", "bawang"]},
    "karawang":    {"lat": -6.305, "lon": 107.305, "komoditas": ["beras"]},
    "magelang":    {"lat": -7.475, "lon": 110.218, "komoditas": ["bawang"]},
    "cianjur":     {"lat": -6.817, "lon": 107.142, "komoditas": ["beras"]},
    "probolinggo": {"lat": -7.754, "lon": 113.215, "komoditas": ["bawang"]},
}

# Harga acuan nasional Juni 2026 (Rp/kg)
BASE_PRICE = {
    "beras":             13500.0,
    "cabai_rawit_merah": 83500.0,
    "cabai_keriting":    52000.0,
    "bawang_merah":      37000.0,
    "bawang_putih":      41000.0,
}

# Volatilitas harian (std dev fraksi)
VOLATILITY = {
    "beras":             0.003,
    "cabai_rawit_merah": 0.045,
    "cabai_keriting":    0.038,
    "bawang_merah":      0.022,
    "bawang_putih":      0.018,
}

# Seasonal amplitude & phase (dalam fraksi harga).
# Bulan basis: Jan=1 ... Des=12. Harvest biasanya menekan harga.
SEASON_PARAMS = {
    "beras":             {"amp": 0.04,  "peak_month": 3,  "harvest_months": [3, 4, 9, 10]},
    "cabai_rawit_merah": {"amp": 0.18,  "peak_month": 8,  "harvest_months": [2, 3, 7, 8]},
    "cabai_keriting":    {"amp": 0.15,  "peak_month": 8,  "harvest_months": [2, 3, 7, 8]},
    "bawang_merah":      {"amp": 0.12,  "peak_month": 9,  "harvest_months": [4, 5, 9, 10]},
    "bawang_putih":      {"amp": 0.10,  "peak_month": 10, "harvest_months": [5, 6, 10, 11]},
}

# Spread harga antar sumber (fraksi dari harga acuan)
SOURCE_BIAS = {
    "bapanas":      1.000,   # acuan nasional retail
    "pihps":        0.978,   # pasar tradisional sedikit lebih murah
    "siskaperbapo": 0.950,   # Jawa Timur, dekat sentra produksi
}

KURS_BASE = 16350.0
KURS_VOL = 0.004

# Template berita dalam bahasa Indonesia
NEWS_TEMPLATES = [
    "Harga {komoditas} di pasar tradisional {lokasi} mengalami tekanan akibat cuaca ekstrem.",
    "Stok {komoditas} menipis di {lokasi}, pedagang waspada lonjakan harga.",
    "Panen {komoditas} di {lokasi} diprediksi melimpah, harga diproyeksikan stabil.",
    "Distribusi {komoditas} ke {lokasi} terganggu hujan deras, harga naik tipis.",
    "Operasi pasar {komoditas} di {lokasi} diperluas untuk menekan inflasi.",
    "Permintaan {komoditas} meningkat jelang Ramadhan, harga di {lokasi} naik.",
    "El Nino diprediksi pengaruhi produksi {komoditas} di {lokasi} tahun ini.",
    "Harga {komoditas} di {lokasi} turun setelah panen raya dari sentra produksi.",
]

SHOCK_TEMPLATES = [
    "Gagal panen {komoditas} di {lokasi} akibat banjir, harga melonjak signifikan.",
    "Kekeringan parah di {lokasi} memicu kenaikan harga {komoditas}.",
    "Banjir bandang rendam ribuan hektar lahan {komoditas} di {lokasi}.",
    "Wabah hama wereng mengancam produksi {komoditas} di {lokasi}.",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="LUMBUNG synthetic Big Data snapshot generator")
    parser.add_argument("--years", type=int, default=5, help="Jumlah tahun data historis (default: 5)")
    parser.add_argument("--seed", type=int, default=2026, help="Random seed untuk reproducibility")
    parser.add_argument("--news-per-day", type=int, default=8, help="Rata-rata artikel per hari")
    parser.add_argument("--push-to-hdfs", action="store_true", help="Push snapshot ke HDFS setelah generate")
    parser.add_argument("--output-root", type=Path, default=OUTPUT_ROOT, help="Output directory")
    return parser.parse_args()


def iso_ts(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat()


def month_angle(month: int) -> float:
    """Konversi bulan (1-12) ke sudut dalam radian untuk seasonality sinusoidal."""
    return 2 * np.pi * (month - 1) / 12


def seasonal_factor(komoditas: str, date: datetime.date) -> float:
    """Faktor musiman: peak_month = harga tertinggi, harvest = penurunan."""
    params = SEASON_PARAMS[komoditas]
    amp = params["amp"]
    peak = params["peak_month"]
    harvest = set(params["harvest_months"])

    # Sinusoidal dengan puncak di peak_month
    angle = month_angle(date.month) - month_angle(peak)
    factor = 1.0 + amp * np.cos(angle)

    # Penurunan tambahan saat panen
    if date.month in harvest:
        factor -= amp * 0.35

    return factor


def generate_price_series(
    rng: np.random.Generator,
    dates: list[datetime.date],
    komoditas: str,
    source_key: str,
) -> list[dict[str, Any]]:
    """Generate daily price records dengan random walk + seasonality + shocks."""
    base = BASE_PRICE[komoditas]
    vol = VOLATILITY[komoditas]
    bias = SOURCE_BIAS[source_key]
    price = base * bias

    records = []
    shock_dates: set[datetime.date] = set()

    for i, date in enumerate(dates):
        # Trend inflasi tahunan ~3%
        years_since_start = i / 365.25
        trend = 1.0 + 0.03 * years_since_start

        # Seasonality
        season = seasonal_factor(komoditas, date)

        # Random walk dengan mean reversion ke baseline
        deviation = (price - base * bias * trend * season) / (base * bias)
        mean_revert = -0.08 * deviation
        random_drift = rng.normal(0, vol)
        shock = 0.0

        # Shock event: 2% probability per day, lebih besar untuk cabai
        if rng.random() < 0.02:
            shock_size = rng.uniform(0.05, 0.25) if komoditas == "beras" else rng.uniform(0.10, 0.45)
            shock = rng.choice([-1, 1]) * shock_size
            if shock > 0:
                shock_dates.add(date)

        drift = mean_revert + random_drift + shock
        price = max(price * (1.0 + drift), base * bias * 0.45)
        price = min(price, base * bias * 2.2)

        ts = iso_ts(datetime.combine(date, datetime.min.time()))
        records.append({
            "source": source_key,
            "data_source": f"{source_key}-snapshot",
            "komoditas": komoditas,
            "level_harga": "konsumen" if source_key != "pihps" else "pasar_tradisional",
            "price_idr_per_kg": round(price, 0),
            "currency": "IDR",
            "unit": "kg",
            "country": "ID",
            "fetched_at_utc": ts,
            "ingestion_ts": ts,
        })

    return records


def generate_weather_series(
    rng: np.random.Generator,
    dates: list[datetime.date],
    sentra: str,
    meta: dict[str, Any],
    shock_dates: set[datetime.date],
) -> list[dict[str, Any]]:
    """Generate daily weather records per sentra."""
    base_temp = 27.0  # °C
    base_precip = 6.0  # mm
    lat, lon = meta["lat"], meta["lon"]
    komoditas_list = meta["komoditas"]

    records = []
    for date in dates:
        # Musim hujan: Nov-Mar meningkatkan curah hujan
        wet_season = 1.0 + 0.6 * np.cos(month_angle(date.month) - month_angle(1))

        # Temperature
        temp_max = base_temp + rng.normal(2.5, 1.2) + 1.5 * np.sin(month_angle(date.month))
        temp_min = temp_max - rng.uniform(4.0, 8.0)
        temp_current = temp_min + rng.uniform(0.4, 0.7) * (temp_max - temp_min)

        # Precipitation: exponential-like, dengan spike pada shock_dates
        precip = rng.exponential(base_precip * wet_season)
        if date in shock_dates and rng.random() < 0.6:
            precip += rng.uniform(40.0, 120.0)
        precip = min(precip, 250.0)

        # Weather code WMO sederhana
        if precip > 50:
            wmo = 95  # hujan lebat/petir
        elif precip > 20:
            wmo = 81  # hujan sedang-lebat
        elif precip > 5:
            wmo = 61  # hujan ringan
        elif precip > 0:
            wmo = 51  # gerimis
        else:
            wmo = 0 if rng.random() < 0.7 else 1  # cerah / berawan

        ts = iso_ts(datetime.combine(date, datetime.min.time()))
        records.append({
            "source": "open-meteo",
            "sentra": sentra,
            "lat": lat,
            "lon": lon,
            "komoditas": komoditas_list,
            "fetched_at_utc": ts,
            "current": {
                "temperature_2m": round(temp_current, 1),
                "relative_humidity_2m": int(rng.uniform(60, 95)),
                "precipitation": round(precip, 1),
                "rain": round(precip * rng.uniform(0.85, 1.0), 1),
                "wind_speed_10m": round(rng.uniform(2.0, 12.0), 1),
                "weather_code": wmo,
            },
            "daily": {
                "temperature_2m_max": round(temp_max, 1),
                "temperature_2m_min": round(temp_min, 1),
                "precipitation_sum": round(precip, 1),
                "rain_sum": round(precip * rng.uniform(0.85, 1.0), 1),
                "wind_speed_10m_max": round(rng.uniform(5.0, 20.0), 1),
            },
            "ingestion_ts": ts,
        })

    return records


def generate_kurs_series(
    rng: np.random.Generator,
    dates: list[datetime.date],
) -> list[dict[str, Any]]:
    """Generate daily USD/IDR kurs dengan random walk."""
    kurs = KURS_BASE
    records = []
    for date in dates:
        deviation = (kurs - KURS_BASE) / KURS_BASE
        mean_revert = -0.05 * deviation
        drift = rng.normal(0, KURS_VOL) + mean_revert
        kurs = max(kurs * (1.0 + drift), KURS_BASE * 0.82)
        kurs = min(kurs, KURS_BASE * 1.18)

        ts = iso_ts(datetime.combine(date, datetime.min.time()))
        records.append({
            "source": "jisdor_bi",
            "data_source": "jisdor-snapshot",
            "pair": "USD/IDR",
            "kurs_jual": round(kurs, 2),
            "kurs_beli": round(kurs * 0.997, 2),
            "kurs_tengah": round(kurs * 0.9985, 2),
            "currency": "IDR",
            "country": "ID",
            "fetched_at_utc": ts,
            "ingestion_ts": ts,
        })
    return records


def generate_news_records(
    rng: np.random.Generator,
    dates: list[datetime.date],
    komoditas_list: list[str],
    sentra_list: list[str],
    shock_dates: set[datetime.date],
    news_per_day: int,
) -> list[dict[str, Any]]:
    """Generate synthetic news articles correlated with shocks."""
    records = []
    article_counter = 0

    for date in dates:
        n_today = rng.poisson(news_per_day)
        is_shock_day = date in shock_dates
        if is_shock_day:
            n_today += rng.poisson(8)  # spike berita saat shock

        for _ in range(n_today):
            komoditas = rng.choice(komoditas_list)
            sentra = rng.choice(sentra_list)
            lokasi = sentra.replace("_", " ").title()

            if is_shock_day and rng.random() < 0.5:
                template = rng.choice(SHOCK_TEMPLATES)
                supply_kw = ["gagal panen", "banjir", "kekeringan", "hama", "lonjakan harga"]
                score_base = 15
            else:
                template = rng.choice(NEWS_TEMPLATES)
                supply_kw = sorted(rng.choice(
                    ["harga", "stok", "panen", "produksi", "distribusi", "subsidi"],
                    size=rng.integers(1, 3),
                    replace=False,
                ).tolist())
                score_base = 6

            title = template.format(komoditas=komoditas.replace("_", " "), lokasi=lokasi)
            summary = f"{title} Analis memperkirakan dinamika pasar {komoditas.replace('_', ' ')} akan terus dipantau."

            relevance = score_base + len(supply_kw) * 2 + rng.integers(0, 5)
            article_counter += 1
            art_id = hashlib.sha1(f"{date}-{article_counter}-{title}".encode()).hexdigest()[:16]

            ts = iso_ts(datetime.combine(date, datetime.min.time()))
            records.append({
                "source": f"snapshot-{rng.choice(['detik', 'antara', 'cnn'])}",
                "article_id": art_id,
                "url": f"https://news.example.com/{date}/{art_id}",
                "title": title,
                "summary": summary[:500],
                "published": ts,
                "komoditas_matched": [komoditas.replace("_", " ")],
                "supply_keywords": supply_kw,
                "sentra_matched": [sentra],
                "relevance_score": int(relevance),
                "fetched_at_utc": ts,
                "ingestion_ts": ts,
            })

    return records


def write_partitioned_jsonl(
    records: list[dict[str, Any]],
    output_root: Path,
    subpath: str,
    date_field: str = "fetched_at_utc",
) -> int:
    """Tulis list record ke file JSONL partition by date."""
    if not records:
        return 0

    # Kelompokkan per date
    by_date: dict[str, list[dict]] = {}
    for rec in records:
        ts = rec.get(date_field, "")
        date_str = ts[:10] if isinstance(ts, str) and len(ts) >= 10 else "1970-01-01"
        by_date.setdefault(date_str, []).append(rec)

    count = 0
    for date_str, day_records in sorted(by_date.items()):
        out_dir = output_root / subpath / date_str
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / "batch_000000.jsonl"
        with open(out_file, "w", encoding="utf-8") as f:
            for rec in day_records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
        count += len(day_records)

    return count


def push_to_hdfs_local() -> int:
    """Panggil hdfs/push_to_hdfs.py --only-raw jika tersedia."""
    push_script = BASE_DIR / "hdfs" / "push_to_hdfs.py"
    if not push_script.exists():
        log.warning("hdfs/push_to_hdfs.py tidak ditemukan, skip push ke HDFS.")
        return 1

    import subprocess
    log.info("Meng-push snapshot ke HDFS...")
    try:
        subprocess.run([sys.executable, str(push_script), "--only-raw"], check=True, cwd=str(BASE_DIR))
        return 0
    except subprocess.CalledProcessError as e:
        log.error(f"Push ke HDFS gagal: {e}")
        return e.returncode


def main() -> int:
    args = parse_args()
    rng = np.random.default_rng(args.seed)

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=args.years * 365)
    dates = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

    log.info("=" * 60)
    log.info("LUMBUNG — Big Data Synthetic Snapshot Generator")
    log.info(f"Periode : {start_date} s/d {end_date} ({len(dates)} hari)")
    log.info(f"Seed    : {args.seed}")
    log.info(f"Output  : {args.output_root}")
    log.info("=" * 60)

    stats: dict[str, int] = {}
    all_shock_dates: set[datetime.date] = set()

    # ── Prices ───────────────────────────────────────────────────────────────
    log.info("Generating price records...")
    for source_key in ["bapanas", "pihps", "siskaperbapo"]:
        topic_path = f"prices/{source_key}"
        source_records: list[dict] = []
        for komoditas in KOMODITAS_LIST:
            records = generate_price_series(rng, dates, komoditas, source_key)
            source_records.extend(records)

            # Kumpulkan shock dates dari pergerakan ekstrem (>15% dalam sehari)
            for i in range(1, len(records)):
                prev = records[i - 1]["price_idr_per_kg"]
                curr = records[i]["price_idr_per_kg"]
                if abs(curr - prev) / prev > 0.15:
                    date_str = records[i]["fetched_at_utc"][:10]
                    all_shock_dates.add(datetime.strptime(date_str, "%Y-%m-%d").date())

            stats[f"price_{source_key}_{komoditas}"] = len(records)
            log.info(f"  {source_key:15s} {komoditas:22s}: {len(records):,} records")

        n = write_partitioned_jsonl(source_records, args.output_root, topic_path)
        log.info(f"  {source_key:15s} {'TOTAL':22s}: {n:,} records")

    log.info(f"Total shock dates detected: {len(all_shock_dates)}")

    # ── Weather ──────────────────────────────────────────────────────────────
    log.info("Generating weather records...")
    all_weather_records: list[dict] = []
    for sentra, meta in SENTRA_PRODUKSI.items():
        records = generate_weather_series(rng, dates, sentra, meta, all_shock_dates)
        all_weather_records.extend(records)
        log.info(f"  {sentra:15s}: {len(records):,} records")
    n = write_partitioned_jsonl(all_weather_records, args.output_root, "weather")
    stats["weather"] = n
    log.info(f"  {'TOTAL':15s}: {n:,} records")

    # ── Kurs ─────────────────────────────────────────────────────────────────
    log.info("Generating kurs records...")
    kurs_records = generate_kurs_series(rng, dates)
    n = write_partitioned_jsonl(kurs_records, args.output_root, "kurs")
    stats["kurs"] = n
    log.info(f"  USD/IDR: {n:,} records")

    # ── News ─────────────────────────────────────────────────────────────────
    log.info("Generating news records...")
    news_records = generate_news_records(
        rng, dates, KOMODITAS_LIST, list(SENTRA_PRODUKSI.keys()), all_shock_dates, args.news_per_day
    )
    n = write_partitioned_jsonl(news_records, args.output_root, "news")
    stats["news"] = n
    log.info(f"  artikel: {n:,} records")

    # ── Summary ──────────────────────────────────────────────────────────────
    total_records = sum(stats.values())
    log.info("=" * 60)
    log.info("SNAPSHOT SUMMARY")
    log.info("=" * 60)
    for key, val in stats.items():
        log.info(f"  {key:30s}: {val:>10,}")
    log.info(f"  {'TOTAL':30s}: {total_records:>10,}")
    log.info(f"  Est. size on disk: ~{total_records * 0.4:.0f} KB")

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "start_date": str(start_date),
        "end_date": str(end_date),
        "days": len(dates),
        "seed": args.seed,
        "news_per_day": args.news_per_day,
        "record_counts": stats,
        "total_records": total_records,
        "output_root": str(args.output_root),
    }
    manifest_path = args.output_root / "snapshot_manifest.json"
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)
    log.info(f"Manifest: {manifest_path}")

    if args.push_to_hdfs:
        return push_to_hdfs_local()

    return 0


if __name__ == "__main__":
    sys.exit(main())
