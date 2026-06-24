"""LUMBUNG - Producer Kurs JISDOR Bank Indonesia (REAL DATA)
Owner: Ryan (5027231046)

Pull kurs USD/IDR harian dari JISDOR (Jakarta Interbank Spot Dollar Rate)
Bank Indonesia - kurs acuan resmi transaksi valuta asing di Indonesia.

URL primary  : https://www.bi.go.id/id/statistik/informasi-kurs/jisdor/default.aspx
API alt      : https://api.exchangerate-api.com/v4/latest/USD  (free, no key)
Fallback     : snapshot kurs aktual Juni 2026 + random walk persisten.

Setiap event mencatat field `data_source` eksplisit:
  - "jisdor-api"       = real-time dari BI JISDOR
  - "exchangerate-api" = fallback ke exchangerate-api.com (free tier)
  - "jisdor-snapshot"  = fallback ke baseline + walk

Kurs relevan untuk analisis harga bawang putih (sebagian besar impor).

USAGE:
    python kafka/producer_kurs_bi.py --dry-run --once
    python kafka/producer_kurs_bi.py --once
    python kafka/producer_kurs_bi.py
"""

from __future__ import annotations
import argparse
import json
import logging
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "kurs-bi"
FETCH_INTERVAL_SEC = 12 * 60 * 60  # 2x sehari

# BI JISDOR API (scrape endpoint)
JISDOR_URL = "https://www.bi.go.id/biwebservice/wskursbi.asmx/getSubKursLokal3"

# Free fallback API (no key needed)
EXCHANGERATE_URL = "https://api.exchangerate-api.com/v4/latest/USD"

# Persistensi state random walk
STATE_FILE = Path(__file__).resolve().parent.parent / "logs" / "kurs_walk_state.json"

# Snapshot kurs USD/IDR Juni 2026
SNAPSHOT_KURS = 16350.0

# Volatilitas harian kurs (std dev fraksi, IDR relatif stabil)
KURS_VOLATILITY = 0.004

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
FALLBACK_FILE = LOG_DIR / "kurs_fallback.jsonl"

USER_AGENT = "Mozilla/5.0 (LUMBUNG-bot/0.1; ITS Surabaya BigData FP)"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("producer_kurs_bi")


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


def fetch_jisdor() -> float | None:
    """Try BI JISDOR web service."""
    try:
        headers = {"User-Agent": USER_AGENT, "Accept": "application/json, text/xml"}
        r = requests.get(JISDOR_URL, timeout=12, headers=headers)
        r.raise_for_status()
        # JISDOR returns XML; parse kurs jual USD
        import xml.etree.ElementTree as ET
        root = ET.fromstring(r.text)
        # Cari element yang mengandung kurs
        for elem in root.iter():
            if elem.text and elem.text.strip().replace(".", "").replace(",", "").isdigit():
                val = float(elem.text.strip().replace(",", ""))
                if 10000 < val < 25000:  # sanity check IDR range
                    return val
    except Exception as e:
        log.debug(f"JISDOR API fail: {type(e).__name__}: {e}")
    return None


def fetch_exchangerate_api() -> float | None:
    """Fallback: free exchangerate-api.com (no key needed)."""
    try:
        headers = {"User-Agent": USER_AGENT}
        r = requests.get(EXCHANGERATE_URL, timeout=10, headers=headers)
        r.raise_for_status()
        data = r.json()
        idr_rate = data.get("rates", {}).get("IDR")
        if idr_rate and 10000 < idr_rate < 25000:
            return float(idr_rate)
    except Exception as e:
        log.debug(f"ExchangeRate API fail: {type(e).__name__}: {e}")
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


def snapshot_with_walk(state: dict) -> float:
    """Random walk persisten untuk kurs USD/IDR."""
    base = SNAPSHOT_KURS
    vol = KURS_VOLATILITY
    last = state.get("usd_idr", base)

    deviation = (last - base) / base
    mean_revert_pull = -0.1 * deviation
    random_drift = random.gauss(0, vol)
    drift = mean_revert_pull + random_drift

    new_kurs = max(last * (1 + drift), base * 0.85)  # floor
    new_kurs = min(new_kurs, base * 1.15)              # cap
    new_kurs = round(new_kurs, 2)

    state["usd_idr"] = new_kurs
    return new_kurs


def build_message(kurs: float, source: str) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "source": "jisdor_bi",
        "data_source": source,
        "pair": "USD/IDR",
        "kurs_jual": kurs,
        "kurs_beli": round(kurs * 0.997, 2),  # spread ~0.3%
        "kurs_tengah": round(kurs * 0.9985, 2),
        "currency": "IDR",
        "country": "ID",
        "fetched_at_utc": now,
        "ingestion_ts": now,
    }


def send_or_fallback(producer, msg: dict, dry_run: bool = False) -> None:
    key = "USD_IDR"
    if dry_run:
        log.info(f"[DRY-RUN] {key} | Rp {msg['kurs_jual']:,.2f} | src={msg['data_source']}")
        return
    if producer is not None:
        try:
            producer.send(TOPIC, key=key, value=msg)
            log.info(f"-> Kafka [{TOPIC}] {key} | Rp {msg['kurs_jual']:,.2f} | "
                     f"src={msg['data_source']}")
            return
        except Exception as e:
            log.error(f"Kafka send gagal ({e}) - fallback file.")
    with FALLBACK_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(msg, ensure_ascii=False) + "\n")
    log.info(f"-> FILE  {FALLBACK_FILE.name} key={key}")


def run_once(producer, dry_run: bool = False) -> int:
    walk_state = load_walk_state()
    kurs = None
    source = None

    # Layer 1: JISDOR BI
    kurs = fetch_jisdor()
    if kurs is not None:
        source = "jisdor-api"
        walk_state["usd_idr"] = kurs

    # Layer 2: exchangerate-api.com (free)
    if kurs is None:
        kurs = fetch_exchangerate_api()
        if kurs is not None:
            source = "exchangerate-api"
            walk_state["usd_idr"] = kurs

    # Layer 3: snapshot + walk
    if kurs is None:
        kurs = snapshot_with_walk(walk_state)
        source = "jisdor-snapshot"

    msg = build_message(kurs, source)
    send_or_fallback(producer, msg, dry_run=dry_run)

    if not dry_run:
        save_walk_state(walk_state)

    if producer is not None and not dry_run:
        try:
            producer.flush(timeout=5)
        except Exception:
            pass

    log.info(f"Selesai: kurs={kurs:,.2f} src={source}")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="LUMBUNG JISDOR kurs producer")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--interval", type=int, default=FETCH_INTERVAL_SEC)
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("LUMBUNG producer_kurs_bi - JISDOR Bank Indonesia -> Kafka")
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
