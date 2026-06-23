"""
LUMBUNG — Producer harga Panel Bapanas
Owner: Ryan

Menggunakan public API (yfinance) untuk mengambil harga futures komoditas
sebagai proxy harga pangan Bapanas.
"""

from __future__ import annotations
import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import yfinance as yf

from utils import KAFKA_BOOTSTRAP

TOPIC = "price-bapanas"
FETCH_INTERVAL_SEC = 24 * 60 * 60

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
FALLBACK_FILE = LOG_DIR / "price_bapanas_fallback.jsonl"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("producer_bapanas")

# Mapping komoditas ke ticker yfinance
TICKER_MAP = {
    "beras": "ZR=F",      # Rough Rice Futures
    "kedelai": "ZS=F",    # Soybean Futures
    "gula": "SB=F",       # Sugar Futures
    "gandum": "KE=F",     # Hard Red Winter Wheat Futures
}

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
            acks="all",
            retries=3,
        )
        log.info(f"Kafka connected: {KAFKA_BOOTSTRAP}")
        return p
    except Exception as e:
        log.warning(f"Kafka unavailable ({type(e).__name__}: {e}) - fallback file.")
        return None


def fetch_commodity(komoditas, ticker_symbol):
    ticker = yf.Ticker(ticker_symbol)
    data = ticker.history(period="1d")
    if data.empty:
        raise ValueError(f"No data returned for {ticker_symbol}")
    
    row = data.iloc[-1]
    now = datetime.now(timezone.utc).isoformat()
    return {
        "source": "bapanas_proxy",
        "commodity": komoditas,
        "ticker": ticker_symbol,
        "date": str(row.name.date()),
        "price": float(row["Close"]),
        "fetched_at_utc": now,
        "ingestion_ts": now,
    }


def send_or_fallback(producer, msg, dry_run=False):
    key = msg["commodity"]
    if dry_run:
        log.info(f"[DRY-RUN] {key} | price={msg['price']}")
        print(json.dumps(msg, ensure_ascii=False))
        return
        
    if producer is not None:
        try:
            producer.send(TOPIC, key=key, value=msg)
            log.info(f"-> Kafka [{TOPIC}] key={key} price={msg['price']}")
            return
        except Exception as e:
            log.error(f"Kafka send gagal ({e}) - fallback file.")
            
    with FALLBACK_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(msg, ensure_ascii=False) + "\n")
    log.info(f"-> FILE  {FALLBACK_FILE.name} key={key} price={msg['price']}")


def run_once(producer, dry_run=False):
    success = 0
    for komoditas, ticker_symbol in TICKER_MAP.items():
        try:
            msg = fetch_commodity(komoditas, ticker_symbol)
            send_or_fallback(producer, msg, dry_run=dry_run)
            success += 1
        except Exception as e:
            log.error(f"Error fetching {komoditas}: {type(e).__name__}: {e}")
            
    if producer is not None and not dry_run:
        producer.flush(timeout=5)
    return success


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Fetch sekali")
    parser.add_argument("--dry-run", action="store_true", help="Print console")
    parser.add_argument("--interval", type=int, default=FETCH_INTERVAL_SEC)
    args = parser.parse_args()

    producer = None if args.dry_run else make_kafka_producer()

    if args.once:
        run_once(producer, dry_run=args.dry_run)
        return 0

    while True:
        run_once(producer, dry_run=args.dry_run)
        log.info(f"Sleep {args.interval}s...")
        time.sleep(args.interval)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(0)
