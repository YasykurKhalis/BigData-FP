"""LUMBUNG - Producer Harga Pupuk PIHC (BATCH)
Owner: Ryan (5027231046)

Pull data harga pupuk dari Pupuk Indonesia Holding Company (PIHC) -
harga eceran tertinggi (HET) pupuk bersubsidi dan harga nonsubsidi
pupuk pertanian di tingkat provinsi.

URL primary  : https://www.pupuk-indonesia.com/
               (scraping website PIHC)
Fallback     : baseline HET resmi + harga pasar realistis + random walk
               agar pipeline tetap menghasilkan event untuk demo.

Jenis pupuk yang dipantau:
  Urea, SP-36, ZA, NPK Phonska, Organik

HET (Harga Eceran Tertinggi) pupuk bersubsidi 2024-2026:
  Urea    : Rp 2.250/kg
  SP-36   : Rp 2.400/kg
  ZA      : Rp 1.700/kg
  NPK     : Rp 2.300/kg
  Organik : Rp   800/kg

Setiap event mencatat field `data_source` eksplisit:
  - "pihc-web"            = real-time dari website PIHC
  - "synthetic-fallback"  = fallback ke baseline + walk

USAGE:
    python kafka/producer_harga_pupuk.py --dry-run --once
    python kafka/producer_harga_pupuk.py --once
    python kafka/producer_harga_pupuk.py --batch
    python kafka/producer_harga_pupuk.py
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
TOPIC = "harga-pupuk"
FETCH_INTERVAL_SEC = 7 * 24 * 60 * 60  # mingguan

PIHC_BASE = "https://www.pupuk-indonesia.com"

STATE_FILE = Path(__file__).resolve().parent.parent / "logs" / "harga_pupuk_walk_state.json"

# Jenis pupuk yang dipantau
JENIS_PUPUK = [
    "Urea", "SP-36", "ZA", "NPK Phonska", "Organik",
]

# Baseline HET (Harga Eceran Tertinggi) pupuk bersubsidi (Rp/kg)
# dan harga nonsubsidi pasar (Rp/kg)
BASELINE_HARGA = {
    "Urea": {
        "harga_subsidi_per_kg": 2_250,
        "harga_nonsubsidi_per_kg": 8_500,
        "ketersediaan_pct": 85.0,
    },
    "SP-36": {
        "harga_subsidi_per_kg": 2_400,
        "harga_nonsubsidi_per_kg": 9_200,
        "ketersediaan_pct": 78.0,
    },
    "ZA": {
        "harga_subsidi_per_kg": 1_700,
        "harga_nonsubsidi_per_kg": 5_800,
        "ketersediaan_pct": 82.0,
    },
    "NPK Phonska": {
        "harga_subsidi_per_kg": 2_300,
        "harga_nonsubsidi_per_kg": 11_500,
        "ketersediaan_pct": 75.0,
    },
    "Organik": {
        "harga_subsidi_per_kg": 800,
        "harga_nonsubsidi_per_kg": 2_500,
        "ketersediaan_pct": 90.0,
    },
}

# Volatilitas: HET tetap (subsidi), nonsubsidi fluktuatif
VOLATILITY_NONSUB = 0.03
VOLATILITY_KETERSEDIAAN = 0.05

PROVINSI_SAMPLE = [
    "Jawa Barat", "Jawa Tengah", "Jawa Timur", "Sumatera Utara",
    "Sulawesi Selatan", "Lampung", "Nusa Tenggara Barat",
    "Kalimantan Selatan", "Sumatera Selatan", "Banten",
]

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
FALLBACK_FILE = LOG_DIR / "harga_pupuk_fallback.jsonl"

USER_AGENT = "Mozilla/5.0 (LUMBUNG-bot/0.1; ITS Surabaya BigData FP)"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("producer_harga_pupuk")


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


def fetch_pihc_harga(jenis_pupuk: str) -> dict | None:
    """
    Scrape harga pupuk dari website PIHC.
    Return dict {harga_subsidi_per_kg, harga_nonsubsidi_per_kg, ketersediaan_pct}
    atau None jika gagal.
    """
    try:
        url = f"{PIHC_BASE}/id/pupuk"
        headers = {"User-Agent": USER_AGENT, "Accept": "text/html"}
        r = requests.get(url, timeout=15, headers=headers)
        r.raise_for_status()
        # Parse HTML untuk cari harga (simplified - real implementation perlu BeautifulSoup)
        if jenis_pupuk.lower() in r.text.lower():
            log.debug(f"PIHC page loaded but parsing not implemented for {jenis_pupuk}")
    except Exception as e:
        log.debug(f"PIHC web fail {jenis_pupuk}: {type(e).__name__}: {e}")
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


def synthetic_harga_pupuk(jenis_pupuk: str, state: dict) -> dict:
    """
    Generate harga pupuk sintetis.
    HET subsidi tetap (diatur pemerintah), nonsubsidi dan ketersediaan fluktuatif.
    """
    baseline = BASELINE_HARGA.get(jenis_pupuk, {
        "harga_subsidi_per_kg": 2_000,
        "harga_nonsubsidi_per_kg": 7_000,
        "ketersediaan_pct": 80.0,
    })

    # HET subsidi tetap (pemerintah menetapkan)
    harga_subsidi = baseline["harga_subsidi_per_kg"]

    # Harga nonsubsidi fluktuatif
    key_nonsub = f"{jenis_pupuk}_nonsub"
    base_nonsub = baseline["harga_nonsubsidi_per_kg"]
    last_nonsub = state.get(key_nonsub, base_nonsub)

    drift = random.gauss(0, VOLATILITY_NONSUB)
    mean_revert = -0.1 * ((last_nonsub - base_nonsub) / base_nonsub)
    new_nonsub = max(last_nonsub * (1 + drift + mean_revert), base_nonsub * 0.7)
    new_nonsub = min(new_nonsub, base_nonsub * 1.5)
    state[key_nonsub] = new_nonsub

    # Ketersediaan fluktuatif
    key_avail = f"{jenis_pupuk}_avail"
    base_avail = baseline["ketersediaan_pct"]
    last_avail = state.get(key_avail, base_avail)

    drift_avail = random.gauss(0, VOLATILITY_KETERSEDIAAN)
    mean_revert_avail = -0.15 * ((last_avail - base_avail) / 100)
    new_avail = last_avail + (drift_avail + mean_revert_avail) * 100
    new_avail = max(min(new_avail, 100.0), 30.0)
    state[key_avail] = new_avail

    return {
        "harga_subsidi_per_kg": round(harga_subsidi, 0),
        "harga_nonsubsidi_per_kg": round(new_nonsub, 0),
        "ketersediaan_pct": round(new_avail, 1),
    }


def build_message(jenis_pupuk: str, data: dict, source: str, provinsi: str) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "source": "harga-pupuk",
        "data_source": source,
        "jenis_pupuk": jenis_pupuk,
        "harga_subsidi_per_kg": data["harga_subsidi_per_kg"],
        "harga_nonsubsidi_per_kg": data["harga_nonsubsidi_per_kg"],
        "ketersediaan_pct": data["ketersediaan_pct"],
        "currency": "IDR",
        "unit": "kg",
        "provinsi": provinsi,
        "country": "ID",
        "fetched_at_utc": now,
        "ingestion_ts": now,
    }


def send_or_fallback(producer, msg: dict, dry_run: bool = False) -> None:
    key = msg["jenis_pupuk"]
    if dry_run:
        log.info(f"[DRY-RUN] {key:15s} | subsidi=Rp {int(msg['harga_subsidi_per_kg']):>6,}/kg | "
                 f"nonsub=Rp {int(msg['harga_nonsubsidi_per_kg']):>6,}/kg | "
                 f"avail={msg['ketersediaan_pct']:>5.1f}% | src={msg['data_source']}")
        return
    if producer is not None:
        try:
            producer.send(TOPIC, key=key, value=msg)
            log.info(f"-> Kafka [{TOPIC}] {key:15s} | subsidi=Rp {int(msg['harga_subsidi_per_kg']):>6,}/kg | "
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

    for jenis_pupuk in JENIS_PUPUK:
        for provinsi in provinsi_list:
            api_data = fetch_pihc_harga(jenis_pupuk)
            source = None

            if api_data is not None:
                source = "pihc-web"
                api_hits += 1
                data = api_data
            else:
                data = synthetic_harga_pupuk(jenis_pupuk, walk_state)
                source = "synthetic-fallback"
                synthetic_hits += 1

            msg = build_message(jenis_pupuk, data, source, provinsi)
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
    parser = argparse.ArgumentParser(description="LUMBUNG Harga Pupuk batch producer")
    parser.add_argument("--once", action="store_true", help="Single fetch lalu exit")
    parser.add_argument("--dry-run", action="store_true", help="Print ke stdout, tanpa Kafka")
    parser.add_argument("--batch", action="store_true", help="Batch mode: semua provinsi sekaligus")
    parser.add_argument("--interval", type=int, default=FETCH_INTERVAL_SEC)
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("LUMBUNG producer_harga_pupuk - PIHC Pupuk -> Kafka")
    log.info(f"Jenis Pupuk: {JENIS_PUPUK}")
    log.info(f"Topic      : {TOPIC}")
    log.info(f"Mode       : once={args.once} dry_run={args.dry_run} batch={args.batch}")
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
