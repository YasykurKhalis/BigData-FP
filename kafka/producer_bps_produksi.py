"""LUMBUNG - Producer Produksi Pertanian BPS (BATCH)
Owner: Ryan (5027231046)

Pull data produksi pertanian bulanan (ATAP/ARAM Kementan) dari
BPS (Badan Pusat Statistik) - statistik produksi tanaman pangan
dan hortikultura nasional per provinsi.

URL primary  : https://webapi.bps.go.id/v1/
               (butuh API key dari env var BPS_API_KEY)
Fallback     : baseline produksi nasional realistis + random walk
               agar pipeline tetap menghasilkan event untuk demo.

Setiap event mencatat field `data_source` eksplisit:
  - "bps-api"             = real-time dari BPS Web API
  - "synthetic-fallback"  = fallback ke baseline + walk

Komoditas (10 fokus LUMBUNG):
  beras, cabai_rawit_merah, cabai_keriting, bawang_merah, bawang_putih,
  gula_pasir, minyak_goreng, daging_ayam, telur_ayam, daging_sapi

USAGE:
    python kafka/producer_bps_produksi.py --dry-run --once
    python kafka/producer_bps_produksi.py --once
    python kafka/producer_bps_produksi.py --batch
    python kafka/producer_bps_produksi.py
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "bps-produksi"
FETCH_INTERVAL_SEC = 30 * 24 * 60 * 60  # bulanan

BPS_API_BASE = "https://webapi.bps.go.id/v1"
BPS_API_KEY = os.environ.get("BPS_API_KEY", "")

STATE_FILE = Path(__file__).resolve().parent.parent / "logs" / "bps_produksi_walk_state.json"

KOMODITAS_LIST = [
    "beras", "cabai_rawit_merah", "cabai_keriting", "bawang_merah",
    "bawang_putih", "gula_pasir", "minyak_goreng", "daging_ayam",
    "telur_ayam", "daging_sapi",
]

# Baseline produksi nasional tahunan (ton) dan luas panen (ha)
# Dibagi 12 untuk estimasi bulanan
BASELINE_PRODUKSI = {
    "beras":             {"produksi_ton_year": 31_000_000, "luas_panen_ha_year": 10_000_000},
    "cabai_rawit_merah": {"produksi_ton_year":    800_000, "luas_panen_ha_year":      75_000},
    "cabai_keriting":    {"produksi_ton_year":    700_000, "luas_panen_ha_year":      65_000},
    "bawang_merah":      {"produksi_ton_year":  1_800_000, "luas_panen_ha_year":     170_000},
    "bawang_putih":      {"produksi_ton_year":     90_000, "luas_panen_ha_year":      15_000},
    "gula_pasir":        {"produksi_ton_year":  2_300_000, "luas_panen_ha_year":     420_000},
    "minyak_goreng":     {"produksi_ton_year": 48_000_000, "luas_panen_ha_year":  14_000_000},
    "daging_ayam":       {"produksi_ton_year":  3_800_000, "luas_panen_ha_year":           0},
    "telur_ayam":        {"produksi_ton_year":  5_400_000, "luas_panen_ha_year":           0},
    "daging_sapi":       {"produksi_ton_year":    550_000, "luas_panen_ha_year":           0},
}

VOLATILITY = 0.08  # standar deviasi bulanan

PROVINSI_SAMPLE = [
    "Jawa Barat", "Jawa Tengah", "Jawa Timur", "Sumatera Utara",
    "Sulawesi Selatan", "Lampung", "Nusa Tenggara Barat",
    "Kalimantan Selatan", "Bali", "DI Yogyakarta",
]

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
FALLBACK_FILE = LOG_DIR / "bps_produksi_fallback.jsonl"

USER_AGENT = "Mozilla/5.0 (LUMBUNG-bot/0.1; ITS Surabaya BigData FP)"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("producer_bps_produksi")


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


def fetch_bps_produksi(komoditas: str) -> dict | None:
    """
    Ambil data produksi dari BPS Web API.
    Return dict {produksi_ton, luas_panen_ha} atau None jika gagal.
    """
    if not BPS_API_KEY:
        log.debug("BPS_API_KEY tidak di-set, skip API call.")
        return None
    try:
        url = f"{BPS_API_BASE}/api/list/model/data/domain/0000/var/0/key/{BPS_API_KEY}/"
        headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        r = requests.get(url, timeout=15, headers=headers)
        r.raise_for_status()
        body = r.json()
        if body.get("status") == "OK" and body.get("data"):
            return body["data"]
    except Exception as e:
        log.debug(f"BPS API fail {komoditas}: {type(e).__name__}: {e}")
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


def synthetic_produksi(komoditas: str, state: dict) -> dict:
    """Generate data produksi sintetis dengan random walk."""
    baseline = BASELINE_PRODUKSI.get(komoditas, {
        "produksi_ton_year": 100_000, "luas_panen_ha_year": 10_000
    })
    monthly_prod = baseline["produksi_ton_year"] / 12
    monthly_area = baseline["luas_panen_ha_year"] / 12

    key_prod = f"{komoditas}_prod"
    key_area = f"{komoditas}_area"

    last_prod = state.get(key_prod, monthly_prod)
    last_area = state.get(key_area, monthly_area)

    # Random walk dengan mean-reversion
    drift_prod = random.gauss(0, VOLATILITY) - 0.1 * ((last_prod - monthly_prod) / monthly_prod)
    drift_area = random.gauss(0, VOLATILITY * 0.5) - 0.1 * ((last_area - monthly_area) / monthly_area)

    new_prod = max(last_prod * (1 + drift_prod), monthly_prod * 0.3)
    new_prod = min(new_prod, monthly_prod * 2.0)

    new_area = max(last_area * (1 + drift_area), monthly_area * 0.5)
    new_area = min(new_area, monthly_area * 1.5)

    state[key_prod] = new_prod
    state[key_area] = new_area

    produktivitas = round(new_prod / new_area, 2) if new_area > 0 else 0.0

    return {
        "produksi_ton": round(new_prod, 1),
        "luas_panen_ha": round(new_area, 1),
        "produktivitas_ton_per_ha": produktivitas,
    }


def build_message(komoditas: str, data: dict, source: str, provinsi: str) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    today = datetime.now(timezone.utc)
    return {
        "source": "bps-produksi",
        "data_source": source,
        "komoditas": komoditas,
        "tahun": today.year,
        "bulan": today.month,
        "produksi_ton": data["produksi_ton"],
        "luas_panen_ha": data["luas_panen_ha"],
        "produktivitas_ton_per_ha": data["produktivitas_ton_per_ha"],
        "provinsi": provinsi,
        "country": "ID",
        "fetched_at_utc": now,
        "ingestion_ts": now,
    }


def send_or_fallback(producer, msg: dict, dry_run: bool = False) -> None:
    key = msg["komoditas"]
    if dry_run:
        log.info(f"[DRY-RUN] {key:22s} | {msg['produksi_ton']:>12,.1f} ton | "
                 f"{msg['luas_panen_ha']:>10,.1f} ha | src={msg['data_source']}")
        return
    if producer is not None:
        try:
            producer.send(TOPIC, key=key, value=msg)
            log.info(f"-> Kafka [{TOPIC}] {key:22s} | {msg['produksi_ton']:>12,.1f} ton | "
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
            api_data = fetch_bps_produksi(komoditas)
            source = None

            if api_data is not None:
                source = "bps-api"
                api_hits += 1
                data = api_data
            else:
                data = synthetic_produksi(komoditas, walk_state)
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
    parser = argparse.ArgumentParser(description="LUMBUNG BPS Produksi batch producer")
    parser.add_argument("--once", action="store_true", help="Single fetch lalu exit")
    parser.add_argument("--dry-run", action="store_true", help="Print ke stdout, tanpa Kafka")
    parser.add_argument("--batch", action="store_true", help="Batch mode: semua provinsi sekaligus")
    parser.add_argument("--interval", type=int, default=FETCH_INTERVAL_SEC)
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("LUMBUNG producer_bps_produksi - BPS Produksi -> Kafka")
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
