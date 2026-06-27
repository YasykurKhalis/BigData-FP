"""LUMBUNG - Producer Stok Bulog / Cadangan Pangan Pemerintah (BATCH)
Owner: Ryan (5027231046)

Pull data stok pangan pemerintah (gudang Bulog) dari
Panel Harga Bapanas - stok cadangan beras pemerintah dan
komoditas strategis lainnya.

URL primary  : https://panelharga.badanpangan.go.id/
               (endpoint publik Bapanas)
Fallback     : baseline stok nasional realistis + siklus bulanan
               depletion/replenishment agar pipeline tetap berjalan.

Setiap event mencatat field `data_source` eksplisit:
  - "bapanas-panel"       = real-time dari Panel Harga Bapanas
  - "synthetic-fallback"  = fallback ke baseline + walk

Komoditas (10 fokus LUMBUNG):
  beras, cabai_rawit_merah, cabai_keriting, bawang_merah, bawang_putih,
  gula_pasir, minyak_goreng, daging_ayam, telur_ayam, daging_sapi

USAGE:
    python kafka/producer_stok_bulog.py --dry-run --once
    python kafka/producer_stok_bulog.py --once
    python kafka/producer_stok_bulog.py --batch
    python kafka/producer_stok_bulog.py
"""

from __future__ import annotations
import argparse
import json
import logging
import math
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "stok-bulog"
FETCH_INTERVAL_SEC = 24 * 60 * 60  # harian

BAPANAS_BASE = "https://panelharga.badanpangan.go.id"
BAPANAS_STOCK_ENDPOINT = "/api/stock-data"

STATE_FILE = Path(__file__).resolve().parent.parent / "logs" / "stok_bulog_walk_state.json"

KOMODITAS_LIST = [
    "beras", "cabai_rawit_merah", "cabai_keriting", "bawang_merah",
    "bawang_putih", "gula_pasir", "minyak_goreng", "daging_ayam",
    "telur_ayam", "daging_sapi",
]

# Baseline stok nasional (ton) dan kapasitas gudang (ton)
BASELINE_STOK = {
    "beras":             {"stok_ton": 1_500_000, "kapasitas_gudang_ton": 2_500_000},
    "cabai_rawit_merah": {"stok_ton":    15_000, "kapasitas_gudang_ton":    30_000},
    "cabai_keriting":    {"stok_ton":    12_000, "kapasitas_gudang_ton":    25_000},
    "bawang_merah":      {"stok_ton":    80_000, "kapasitas_gudang_ton":   150_000},
    "bawang_putih":      {"stok_ton":    50_000, "kapasitas_gudang_ton":   100_000},
    "gula_pasir":        {"stok_ton":   350_000, "kapasitas_gudang_ton":   500_000},
    "minyak_goreng":     {"stok_ton":   200_000, "kapasitas_gudang_ton":   400_000},
    "daging_ayam":       {"stok_ton":    25_000, "kapasitas_gudang_ton":    60_000},
    "telur_ayam":        {"stok_ton":    10_000, "kapasitas_gudang_ton":    30_000},
    "daging_sapi":       {"stok_ton":    35_000, "kapasitas_gudang_ton":    80_000},
}

PROVINSI_SAMPLE = [
    "Jawa Barat", "Jawa Tengah", "Jawa Timur", "Sumatera Utara",
    "Sulawesi Selatan", "Lampung", "Nusa Tenggara Barat",
    "Kalimantan Selatan", "Bali", "DKI Jakarta",
]

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
FALLBACK_FILE = LOG_DIR / "stok_bulog_fallback.jsonl"

USER_AGENT = "Mozilla/5.0 (LUMBUNG-bot/0.1; ITS Surabaya BigData FP)"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("producer_stok_bulog")


def make_kafka_producer():
    try:
        from kafka import KafkaProducer
    except ImportError:
        log.warning("kafka-python belum terinstall - fallback file.")
        return None
    try:
        p = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda v: v.encode("utf-8") if v else None,
            acks="all", retries=3, linger_ms=200,
            request_timeout_ms=5000, api_version_auto_timeout_ms=3000,
        )
        log.info(f"Kafka connected: {KAFKA_BOOTSTRAP}")
        return p
    except Exception as e:
        log.warning(f"Kafka unavailable ({type(e).__name__}: {e}) - fallback file.")
        return None


def fetch_stok_bapanas(komoditas: str) -> dict | None:
    """
    Ambil data stok dari Panel Harga Bapanas.
    Return dict {stok_ton, kapasitas_gudang_ton, utilisasi_pct} atau None.
    """
    try:
        url = f"{BAPANAS_BASE}{BAPANAS_STOCK_ENDPOINT}"
        headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        params = {"commodity": komoditas}
        r = requests.get(url, params=params, timeout=15, headers=headers)
        r.raise_for_status()
        body = r.json()
        if body.get("data"):
            return body["data"]
    except Exception as e:
        log.debug(f"Bapanas stock API fail {komoditas}: {type(e).__name__}: {e}")
    return None


# ---------------- Random walk state (persistent) ----------------
def load_walk_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_walk_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def synthetic_stok(komoditas: str, state: dict) -> dict:
    """
    Generate stok sintetis dengan siklus bulanan.
    Stok deplesi selama bulan, replenishment di awal bulan.
    """
    baseline = BASELINE_STOK.get(komoditas, {
        "stok_ton": 10_000, "kapasitas_gudang_ton": 20_000
    })
    base_stok = baseline["stok_ton"]
    kapasitas = baseline["kapasitas_gudang_ton"]

    key = f"{komoditas}_stok"
    last_stok = state.get(key, base_stok)

    # Siklus bulanan: hari ke berapa dalam bulan menentukan depletion
    day_of_month = datetime.now(timezone.utc).day
    # Sinusoidal cycle: stok tinggi awal bulan, rendah akhir bulan
    cycle_factor = math.cos(math.pi * day_of_month / 30) * 0.15
    random_drift = random.gauss(0, 0.05)
    mean_revert = -0.08 * ((last_stok - base_stok) / base_stok)

    drift = cycle_factor + random_drift + mean_revert
    new_stok = max(last_stok * (1 + drift), base_stok * 0.2)
    new_stok = min(new_stok, kapasitas)

    state[key] = new_stok

    utilisasi = round((new_stok / kapasitas) * 100, 1) if kapasitas > 0 else 0.0

    return {
        "stok_ton": round(new_stok, 1),
        "kapasitas_gudang_ton": kapasitas,
        "utilisasi_pct": utilisasi,
    }


def build_message(komoditas: str, data: dict, source: str, provinsi: str) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    today = datetime.now(timezone.utc)
    return {
        "source": "stok-bulog",
        "data_source": source,
        "komoditas": komoditas,
        "stok_ton": data["stok_ton"],
        "kapasitas_gudang_ton": data["kapasitas_gudang_ton"],
        "utilisasi_pct": data["utilisasi_pct"],
        "provinsi": provinsi,
        "tanggal": today.strftime("%Y-%m-%d"),
        "country": "ID",
        "fetched_at_utc": now,
        "ingestion_ts": now,
    }


def send_or_fallback(producer, msg: dict, dry_run: bool = False) -> None:
    key = msg["komoditas"]
    if dry_run:
        log.info(f"[DRY-RUN] {key:22s} | stok={msg['stok_ton']:>12,.1f} ton | "
                 f"util={msg['utilisasi_pct']:>5.1f}% | src={msg['data_source']}")
        return
    if producer is not None:
        try:
            producer.send(TOPIC, key=key, value=msg)
            log.info(f"-> Kafka [{TOPIC}] {key:22s} | stok={msg['stok_ton']:>12,.1f} ton | "
                     f"src={msg['data_source']}")
            return
        except Exception as e:
            log.error(f"Kafka send gagal ({e}) - fallback file.")
    with FALLBACK_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(msg, ensure_ascii=False) + "\n")
    log.info(f"-> FILE  {FALLBACK_FILE.name} key={key}")


def run_once(producer, dry_run: bool = False, batch: bool = False) -> int:
    success = 0
    api_hits = 0
    synthetic_hits = 0
    walk_state = load_walk_state()

    provinsi_list = PROVINSI_SAMPLE if batch else [random.choice(PROVINSI_SAMPLE)]

    for komoditas in KOMODITAS_LIST:
        for provinsi in provinsi_list:
            api_data = fetch_stok_bapanas(komoditas)
            source = None

            if api_data is not None:
                source = "bapanas-panel"
                api_hits += 1
                data = api_data
            else:
                data = synthetic_stok(komoditas, walk_state)
                source = "synthetic-fallback"
                synthetic_hits += 1

            msg = build_message(komoditas, data, source, provinsi)
            send_or_fallback(producer, msg, dry_run=dry_run)
            success += 1

    if not dry_run:
        save_walk_state(walk_state)

    if producer is not None and not dry_run:
        try:
            producer.flush(timeout=5)
        except Exception:
            pass

    log.info(f"Selesai: {success} events "
             f"(API={api_hits} | synthetic={synthetic_hits})")
    return success


def main() -> int:
    parser = argparse.ArgumentParser(description="LUMBUNG Stok Bulog batch producer")
    parser.add_argument("--once", action="store_true", help="Single fetch lalu exit")
    parser.add_argument("--dry-run", action="store_true", help="Print ke stdout, tanpa Kafka")
    parser.add_argument("--batch", action="store_true", help="Batch mode: semua provinsi sekaligus")
    parser.add_argument("--interval", type=int, default=FETCH_INTERVAL_SEC)
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("LUMBUNG producer_stok_bulog - Stok Bulog -> Kafka")
    log.info(f"Komoditas: {KOMODITAS_LIST}")
    log.info(f"Topic    : {TOPIC}")
    log.info(f"Mode     : once={args.once} dry_run={args.dry_run} batch={args.batch}")
    log.info("=" * 60)

    producer = None if args.dry_run else make_kafka_producer()

    if args.once:
        n = run_once(producer, dry_run=args.dry_run, batch=args.batch)
        return 0 if n > 0 else 1

    while True:
        run_once(producer, dry_run=args.dry_run, batch=args.batch)
        log.info(f"Sleep {args.interval}s...")
        time.sleep(args.interval)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log.info("Interrupted. Bye.")
        sys.exit(0)
