"""LUMBUNG - Producer Cuaca Open-Meteo (sentra produksi)

Owner: Ryan (5027231046)

Fetch cuaca dari Open-Meteo untuk 5 sentra dan kirim ke Kafka topic
`weather-sentra`. Fallback ke file kalau Kafka tidak available.

USAGE:
    python kafka/producer_weather.py --dry-run --once
    python kafka/producer_weather.py --once
    python kafka/producer_weather.py
"""

from __future__ import annotations
import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

KAFKA_BOOTSTRAP = "localhost:9092"
SENTRA_PRODUKSI = {
    "brebes":      {"lat": -6.872, "lon": 109.046, "komoditas": ["cabai", "bawang"]},
    "karawang":    {"lat": -6.305, "lon": 107.305, "komoditas": ["beras"]},
    "magelang":    {"lat": -7.475, "lon": 110.218, "komoditas": ["bawang"]},
    "cianjur":     {"lat": -6.817, "lon": 107.142, "komoditas": ["beras"]},
    "probolinggo": {"lat": -7.754, "lon": 113.215, "komoditas": ["bawang"]},
}

TOPIC = "weather-sentra"
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
FETCH_INTERVAL_SEC = 30 * 60

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
FALLBACK_FILE = LOG_DIR / "weather_fallback.jsonl"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("producer_weather")


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
            linger_ms=200,
            request_timeout_ms=5000,
            api_version_auto_timeout_ms=3000,
        )
        log.info(f"Kafka connected: {KAFKA_BOOTSTRAP}")
        return p
    except Exception as e:
        log.warning(f"Kafka unavailable ({type(e).__name__}: {e}) - fallback file.")
        return None


def fetch_weather(lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": "temperature_2m,relative_humidity_2m,precipitation,rain,wind_speed_10m,weather_code",
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,rain_sum,wind_speed_10m_max",
        "timezone": "Asia/Jakarta",
        "forecast_days": 7,
    }
    r = requests.get(OPEN_METEO_URL, params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def build_message(sentra, meta, weather):
    now = datetime.now(timezone.utc).isoformat()
    return {
        "source": "open-meteo",
        "sentra": sentra,
        "lat": meta["lat"],
        "lon": meta["lon"],
        "komoditas": meta["komoditas"],
        "fetched_at_utc": now,
        "current": weather.get("current"),
        "daily": weather.get("daily"),
        "ingestion_ts": now,
    }


def send_or_fallback(producer, msg, dry_run=False):
    key = msg["sentra"]
    if dry_run:
        temp = (msg.get("current") or {}).get("temperature_2m")
        log.info(f"[DRY-RUN] {key} | temp={temp}C")
        print(json.dumps(msg, ensure_ascii=False)[:300] + "...")
        return
    if producer is not None:
        try:
            producer.send(TOPIC, key=key, value=msg)
            log.info(f"-> Kafka [{TOPIC}] key={key}")
            return
        except Exception as e:
            log.error(f"Kafka send gagal ({e}) - fallback file.")
    with FALLBACK_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(msg, ensure_ascii=False) + "\n")
    log.info(f"-> FILE  {FALLBACK_FILE.name} key={key}")


def run_once(producer, dry_run=False):
    success = 0
    for sentra, meta in SENTRA_PRODUKSI.items():
        try:
            weather = fetch_weather(meta["lat"], meta["lon"])
            msg = build_message(sentra, meta, weather)
            send_or_fallback(producer, msg, dry_run=dry_run)
            success += 1
        except requests.HTTPError as e:
            log.error(f"HTTP error {sentra}: {e}")
        except Exception as e:
            log.error(f"Error {sentra}: {type(e).__name__}: {e}")
    if producer is not None and not dry_run:
        try:
            producer.flush(timeout=5)
        except Exception:
            pass
    return success


def main():
    parser = argparse.ArgumentParser(description="LUMBUNG weather producer")
    parser.add_argument("--once", action="store_true", help="Fetch sekali, lalu exit")
    parser.add_argument("--dry-run", action="store_true", help="Tidak kirim Kafka")
    parser.add_argument("--interval", type=int, default=FETCH_INTERVAL_SEC)
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("LUMBUNG producer_weather - Open-Meteo -> Kafka")
    log.info(f"Sentra: {list(SENTRA_PRODUKSI.keys())}")
    log.info(f"Topic : {TOPIC}")
    log.info(f"Mode  : once={args.once} dry_run={args.dry_run}")
    log.info("=" * 60)

    producer = None if args.dry_run else make_kafka_producer()

    if args.once:
        n = run_once(producer, dry_run=args.dry_run)
        log.info(f"Selesai: {n}/{len(SENTRA_PRODUKSI)} sentra sukses")
        return 0 if n > 0 else 1

    while True:
        n = run_once(producer, dry_run=args.dry_run)
        log.info(f"Tick selesai: {n}/{len(SENTRA_PRODUKSI)}. Sleep {args.interval}s...")
        time.sleep(args.interval)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log.info("Interrupted. Bye.")
        sys.exit(0)
