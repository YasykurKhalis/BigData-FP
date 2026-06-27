"""LUMBUNG - Producer Impor-Ekspor BPS (BATCH)
Owner: Ryan (5027231046)

Pull data statistik perdagangan luar negeri (impor/ekspor) dari
BPS (Badan Pusat Statistik) - volume dan nilai impor-ekspor
komoditas pangan strategis bulanan.

URL primary  : https://webapi.bps.go.id/v1/
               (butuh API key dari env var BPS_API_KEY)
Fallback     : baseline volume perdagangan realistis + random walk
               agar pipeline tetap menghasilkan event untuk demo.

Catatan penting:
  - Bawang putih: 80%+ impor dari China
  - Beras: impor occasional (kuota khusus Bulog)
  - Gula pasir: impor raw sugar signifikan
  - Minyak goreng: ekspor CPO besar (Indonesia = eksportir #1 dunia)

Setiap event mencatat field `data_source` eksplisit:
  - "bps-trade-api"       = real-time dari BPS Web API
  - "synthetic-fallback"  = fallback ke baseline + walk

USAGE:
    python kafka/producer_impor_ekspor.py --dry-run --once
    python kafka/producer_impor_ekspor.py --once
    python kafka/producer_impor_ekspor.py --batch
    python kafka/producer_impor_ekspor.py
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
TOPIC = "impor-ekspor"
FETCH_INTERVAL_SEC = 30 * 24 * 60 * 60  # bulanan

BPS_API_BASE = "https://webapi.bps.go.id/v1"
BPS_API_KEY = os.environ.get("BPS_API_KEY", "")

STATE_FILE = Path(__file__).resolve().parent.parent / "logs" / "impor_ekspor_walk_state.json"

KOMODITAS_LIST = [
    "beras", "cabai_rawit_merah", "cabai_keriting", "bawang_merah",
    "bawang_putih", "gula_pasir", "minyak_goreng", "daging_ayam",
    "telur_ayam", "daging_sapi",
]

# Baseline impor-ekspor bulanan (ton dan USD)
# Berdasarkan data BPS dan Kemendag realistis
BASELINE_TRADE = {
    "beras": {
        "impor_ton": 25_000, "impor_usd": 12_500_000,
        "ekspor_ton": 500, "ekspor_usd": 350_000,
        "negara_asal_impor": ["Thailand", "Vietnam", "India", "Pakistan"],
    },
    "cabai_rawit_merah": {
        "impor_ton": 2_000, "impor_usd": 1_800_000,
        "ekspor_ton": 300, "ekspor_usd": 420_000,
        "negara_asal_impor": ["India", "China", "Vietnam"],
    },
    "cabai_keriting": {
        "impor_ton": 1_500, "impor_usd": 1_200_000,
        "ekspor_ton": 200, "ekspor_usd": 280_000,
        "negara_asal_impor": ["India", "China"],
    },
    "bawang_merah": {
        "impor_ton": 15_000, "impor_usd": 9_000_000,
        "ekspor_ton": 1_000, "ekspor_usd": 800_000,
        "negara_asal_impor": ["India", "Thailand", "Filipina"],
    },
    "bawang_putih": {
        "impor_ton": 45_000, "impor_usd": 27_000_000,
        "ekspor_ton": 50, "ekspor_usd": 45_000,
        "negara_asal_impor": ["China", "India", "Mesir"],  # 80%+ dari China
    },
    "gula_pasir": {
        "impor_ton": 250_000, "impor_usd": 112_500_000,
        "ekspor_ton": 5_000, "ekspor_usd": 3_500_000,
        "negara_asal_impor": ["Thailand", "Australia", "Brasil"],
    },
    "minyak_goreng": {
        "impor_ton": 5_000, "impor_usd": 5_000_000,
        "ekspor_ton": 2_800_000, "ekspor_usd": 2_520_000_000,  # CPO eksportir terbesar
        "negara_asal_impor": ["Malaysia", "Singapura"],
    },
    "daging_ayam": {
        "impor_ton": 3_000, "impor_usd": 4_500_000,
        "ekspor_ton": 500, "ekspor_usd": 750_000,
        "negara_asal_impor": ["Brasil", "AS", "Jepang"],
    },
    "telur_ayam": {
        "impor_ton": 500, "impor_usd": 600_000,
        "ekspor_ton": 200, "ekspor_usd": 280_000,
        "negara_asal_impor": ["Malaysia", "India"],
    },
    "daging_sapi": {
        "impor_ton": 35_000, "impor_usd": 175_000_000,
        "ekspor_ton": 100, "ekspor_usd": 700_000,
        "negara_asal_impor": ["Australia", "India", "Brasil", "Selandia Baru"],
    },
}

VOLATILITY = 0.12  # standar deviasi bulanan (trade lebih volatile)

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
FALLBACK_FILE = LOG_DIR / "impor_ekspor_fallback.jsonl"

USER_AGENT = "Mozilla/5.0 (LUMBUNG-bot/0.1; ITS Surabaya BigData FP)"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("producer_impor_ekspor")


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


def fetch_bps_trade(komoditas: str) -> dict | None:
    """
    Ambil data impor-ekspor dari BPS Web API.
    Return dict atau None jika gagal.
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
        log.debug(f"BPS trade API fail {komoditas}: {type(e).__name__}: {e}")
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


def synthetic_trade(komoditas: str, state: dict) -> dict:
    """Generate data perdagangan sintetis dengan random walk."""
    baseline = BASELINE_TRADE.get(komoditas, {
        "impor_ton": 1_000, "impor_usd": 500_000,
        "ekspor_ton": 100, "ekspor_usd": 70_000,
        "negara_asal_impor": ["China"],
    })

    fields = ["impor_ton", "impor_usd", "ekspor_ton", "ekspor_usd"]
    result = {}

    for field in fields:
        key = f"{komoditas}_{field}"
        base_val = baseline[field]
        last_val = state.get(key, base_val)

        drift = random.gauss(0, VOLATILITY)
        mean_revert = -0.08 * ((last_val - base_val) / base_val) if base_val > 0 else 0
        new_val = max(last_val * (1 + drift + mean_revert), base_val * 0.1)
        new_val = min(new_val, base_val * 3.0)

        state[key] = new_val
        result[field] = round(new_val, 1) if "ton" in field else round(new_val, 2)

    # Pilih negara asal utama
    negara_list = baseline.get("negara_asal_impor", ["China"])
    result["negara_asal"] = random.choice(negara_list)

    return result


def build_message(komoditas: str, data: dict, source: str) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    today = datetime.now(timezone.utc)
    return {
        "source": "impor-ekspor",
        "data_source": source,
        "komoditas": komoditas,
        "bulan": today.month,
        "tahun": today.year,
        "impor_ton": data["impor_ton"],
        "impor_usd": data["impor_usd"],
        "ekspor_ton": data["ekspor_ton"],
        "ekspor_usd": data["ekspor_usd"],
        "negara_asal": data.get("negara_asal", ""),
        "country": "ID",
        "fetched_at_utc": now,
        "ingestion_ts": now,
    }


def send_or_fallback(producer, msg: dict, dry_run: bool = False) -> None:
    key = msg["komoditas"]
    if dry_run:
        log.info(f"[DRY-RUN] {key:22s} | impor={msg['impor_ton']:>10,.1f} ton "
                 f"${msg['impor_usd']:>14,.2f} | ekspor={msg['ekspor_ton']:>10,.1f} ton | "
                 f"src={msg['data_source']}")
        return
    if producer is not None:
        try:
            producer.send(TOPIC, key=key, value=msg)
            log.info(f"-> Kafka [{TOPIC}] {key:22s} | impor={msg['impor_ton']:>10,.1f} ton | "
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

    for komoditas in KOMODITAS_LIST:
        api_data = fetch_bps_trade(komoditas)
        source = None

        if api_data is not None:
            source = "bps-trade-api"
            api_hits += 1
            data = api_data
        else:
            data = synthetic_trade(komoditas, walk_state)
            source = "synthetic-fallback"
            synthetic_hits += 1

        msg = build_message(komoditas, data, source)
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
    parser = argparse.ArgumentParser(description="LUMBUNG Impor-Ekspor batch producer")
    parser.add_argument("--once", action="store_true", help="Single fetch lalu exit")
    parser.add_argument("--dry-run", action="store_true", help="Print ke stdout, tanpa Kafka")
    parser.add_argument("--batch", action="store_true", help="Batch mode: semua komoditas sekaligus")
    parser.add_argument("--interval", type=int, default=FETCH_INTERVAL_SEC)
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("LUMBUNG producer_impor_ekspor - BPS Trade -> Kafka")
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
