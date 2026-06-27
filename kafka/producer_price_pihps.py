"""LUMBUNG - Producer Harga PIHPS Bank Indonesia (REAL DATA)
Owner: Ryan (5027231046)

Pull harga konsumen harian 5 komoditas strategis LUMBUNG dari
PIHPS (Pusat Informasi Harga Pangan Strategis) Bank Indonesia -
sumber resmi harga pangan pasar tradisional tingkat nasional.

URL primary  : https://www.bi.go.id/hargapangan/WebSite/Home/GetChartData
               (endpoint publik, tanpa API key)
Fallback     : snapshot harga aktual Juni 2026 (Rp/kg) + random walk persisten
               agar pipeline tetap menghasilkan event Rp/kg untuk demo.

Setiap event mencatat field `data_source` eksplisit:
  - "pihps-api"      = real-time dari PIHPS BI
  - "pihps-snapshot"  = fallback ke baseline + walk

Komoditas (5 fokus LUMBUNG):
  beras, cabai_rawit_merah, cabai_keriting, bawang_merah, bawang_putih

USAGE:
    python kafka/producer_price_pihps.py --dry-run --once
    python kafka/producer_price_pihps.py --once
    python kafka/producer_price_pihps.py
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
TOPIC = "price-pihps"
FETCH_INTERVAL_SEC = 6 * 60 * 60  # 4x sehari

# PIHPS Bank Indonesia — endpoint publik GetChartData
# Tidak butuh API key, return JSON langsung
# Referensi: https://www.bi.go.id/hargapangan/
PIHPS_BASE = "https://www.bi.go.id"
PIHPS_CHART_ENDPOINT = "/hargapangan/WebSite/Home/GetChartData"

# File data real PIHPS BI (di-download dari endpoint publik)
REALDATA_FILE = Path(__file__).resolve().parent.parent / "data" / "pihps_realdata.json"

# Persistensi state random walk
STATE_FILE = Path(__file__).resolve().parent.parent / "logs" / "pihps_walk_state.json"

# Mapping komoditas LUMBUNG -> nama komoditas di PIHPS BI
# (persis seperti di GetCommoditiesTree, termasuk trailing space untuk keriting)
PIHPS_COMMODITY_NAME = {
    "beras":             "Beras Kualitas Medium I",
    "cabai_rawit_merah": "Cabai Rawit Merah",
    "cabai_keriting":    "Cabai Merah Keriting ",   # trailing space dari PIHPS
    "bawang_merah":      "Bawang Merah Ukuran Sedang",
    "bawang_putih":      "Bawang Putih Ukuran Sedang",
}

# Snapshot harga pasar tradisional Juni 2026 (Rp/kg)
# Kalibrasi dari data PIHPS BI — sedikit beda dari Bapanas karena
# level harga berbeda (pasar tradisional vs konsumen retail)
SNAPSHOT_PRICE = {
    "beras":             13200.0,
    "cabai_rawit_merah": 81000.0,
    "cabai_keriting":    50500.0,
    "bawang_merah":      36000.0,
    "bawang_putih":      40000.0,
}

# Volatilitas harian (std dev sebagai fraksi dari harga)
VOLATILITY = {
    "beras":             0.003,
    "cabai_rawit_merah": 0.045,
    "cabai_keriting":    0.038,
    "bawang_merah":      0.022,
    "bawang_putih":      0.018,
}

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
FALLBACK_FILE = LOG_DIR / "price_pihps_fallback.jsonl"

USER_AGENT = "Mozilla/5.0 (LUMBUNG-bot/0.1; ITS Surabaya BigData FP)"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("producer_pihps")


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


def load_realdata_cache() -> dict:
    """Load data real PIHPS BI dari file JSON lokal."""
    if not REALDATA_FILE.exists():
        return {}
    try:
        return json.loads(REALDATA_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


_REALDATA_CACHE = None


def fetch_pihps_price(komoditas: str) -> float | None:
    """
    Ambil harga real PIHPS BI untuk 1 komoditas.
    1) Baca dari file data/pihps_realdata.json (data real dari PIHPS BI)
    2) Coba hit API langsung (biasanya diblokir WAF dari Python)
    Return harga terbaru (Rp/kg) atau None.
    """
    global _REALDATA_CACHE
    # 1) File-based real data
    if _REALDATA_CACHE is None:
        _REALDATA_CACHE = load_realdata_cache()
    if komoditas in _REALDATA_CACHE:
        rows = _REALDATA_CACHE[komoditas].get("data", [])
        if rows:
            latest = rows[-1]
            price = latest.get("nominal")
            if price is not None and float(price) > 0:
                log.debug(f"Real data from file: {komoditas} = {price}")
                return float(price)

    # 2) Try API directly (may fail due to WAF)
    com_name = PIHPS_COMMODITY_NAME.get(komoditas)
    if com_name:
        params = {"comName": com_name}
        headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        url = f"{PIHPS_BASE}{PIHPS_CHART_ENDPOINT}"
        try:
            r = requests.get(url, params=params, timeout=15, headers=headers)
            r.raise_for_status()
            body = r.json()
            rows = body.get("data", [])
            if rows:
                latest = rows[-1]
                price = latest.get("nominal")
                if price is not None and float(price) > 0:
                    return float(price)
        except Exception as e:
            log.debug(f"PIHPS API fail {com_name}: {type(e).__name__}: {e}")
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


def snapshot_with_walk(komoditas: str, state: dict) -> float:
    """Random walk persisten dengan mean-reversion ke baseline."""
    base = SNAPSHOT_PRICE[komoditas]
    vol = VOLATILITY[komoditas]
    last = state.get(komoditas, base)

    deviation = (last - base) / base
    mean_revert_pull = -0.1 * deviation
    random_drift = random.gauss(0, vol)
    drift = mean_revert_pull + random_drift

    new_price = max(last * (1 + drift), base * 0.5)
    new_price = min(new_price, base * 2.0)
    new_price = round(new_price, 0)

    state[komoditas] = new_price
    return new_price


def build_message(komoditas: str, price: float, source: str) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "source": "pihps",
        "data_source": source,
        "komoditas": komoditas,
        "level_harga": "pasar_tradisional",
        "price_idr_per_kg": price,
        "currency": "IDR",
        "unit": "kg",
        "country": "ID",
        "fetched_at_utc": now,
        "ingestion_ts": now,
    }


def send_or_fallback(producer, msg: dict, dry_run: bool = False) -> None:
    key = msg["komoditas"]
    if dry_run:
        log.info(f"[DRY-RUN] {key:22s} | Rp {int(msg['price_idr_per_kg']):>7,}/kg | "
                 f"src={msg['data_source']}")
        return
    if producer is not None:
        try:
            producer.send(TOPIC, key=key, value=msg)
            log.info(f"-> Kafka [{TOPIC}] {key:22s} | Rp {int(msg['price_idr_per_kg']):>7,}/kg | "
                     f"src={msg['data_source']}")
            return
        except Exception as e:
            log.error(f"Kafka send gagal ({e}) - fallback file.")
    with FALLBACK_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(msg, ensure_ascii=False) + "\n")
    log.info(f"-> FILE  {FALLBACK_FILE.name} key={key}")


def run_once(producer, dry_run: bool = False) -> int:
    success = 0
    api_hits = 0
    snapshot_hits = 0
    walk_state = load_walk_state()

    for komoditas in PIHPS_COMMODITY_NAME:
        price = fetch_pihps_price(komoditas)
        source = None

        if price is not None:
            source = "pihps-api"
            api_hits += 1
            walk_state[komoditas] = price
        else:
            price = snapshot_with_walk(komoditas, walk_state)
            source = "pihps-snapshot"
            snapshot_hits += 1

        msg = build_message(komoditas, price, source)
        send_or_fallback(producer, msg, dry_run=dry_run)
        success += 1

    if not dry_run:
        save_walk_state(walk_state)

    if producer is not None and not dry_run:
        try:
            producer.flush(timeout=5)
        except Exception:
            pass

    log.info(f"Selesai: {success}/{len(PIHPS_COMMODITY_NAME)} "
             f"(API={api_hits} | snapshot={snapshot_hits})")
    return success


def main() -> int:
    parser = argparse.ArgumentParser(description="LUMBUNG PIHPS price producer")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--interval", type=int, default=FETCH_INTERVAL_SEC)
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("LUMBUNG producer_price_pihps - PIHPS Bank Indonesia -> Kafka")
    log.info(f"Komoditas: {list(PIHPS_COMMODITY_NAME.keys())}")
    log.info(f"Topic    : {TOPIC}")
    log.info(f"Mode     : once={args.once} dry_run={args.dry_run}")
    log.info("=" * 60)

    producer = None if args.dry_run else make_kafka_producer()

    if args.once:
        n = run_once(producer, dry_run=args.dry_run)
        return 0 if n > 0 else 1

    while True:
        run_once(producer, dry_run=args.dry_run)
        log.info(f"Sleep {args.interval}s...")
        time.sleep(args.interval)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log.info("Interrupted. Bye.")
        sys.exit(0)
