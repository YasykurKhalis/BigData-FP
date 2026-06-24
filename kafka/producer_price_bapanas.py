"""LUMBUNG - Producer Harga Komoditas Panel Bapanas (REAL DATA)
Owner: Ryan (5027231046)

Pull harga konsumen harian 5 komoditas strategis LUMBUNG dari
Panel Harga Bapanas (Badan Pangan Nasional) - sumber resmi pemerintah
Indonesia, bukan proxy futures global.

URL primary  : https://api-panelhargav2.badanpangan.go.id/api/front/...
Fallback     : snapshot harga aktual Juni 2026 (Rp/kg) + jitter realistic
               agar pipeline tetap menghasilkan event Rp/kg untuk demo.

Setiap event mencatat field `data_source` eksplisit:
  - "bapanas-api"      = real-time dari Bapanas API
  - "bapanas-snapshot" = fallback ke baseline + jitter

Komoditas (5 fokus LUMBUNG):
  beras, cabai_rawit_merah, cabai_keriting, bawang_merah, bawang_putih

USAGE:
    python kafka/producer_price_bapanas.py --dry-run --once
    python kafka/producer_price_bapanas.py --once
    python kafka/producer_price_bapanas.py
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
TOPIC = "price-bapanas"
FETCH_INTERVAL_SEC = 6 * 60 * 60  # 4x sehari

# Bapanas Panel Harga API endpoints
# Catatan: per 2026 Bapanas mewajibkan API key (daftar gratis di portal).
# Set via env: BAPANAS_API_KEY=xxx
# Referensi: https://panelharga.badanpangan.go.id/
BAPANAS_BASE = "https://api-panelhargav2.badanpangan.go.id"
BAPANAS_ENDPOINT = "/api/front/harga-pangan-informasi"
BAPANAS_API_KEY = os.getenv("BAPANAS_API_KEY", "").strip()

# Persistensi state random walk supaya harga snapshot punya momentum
# (run berikutnya melanjutkan dari harga terakhir, bukan reset ke baseline)
STATE_FILE = Path(__file__).resolve().parent.parent / "logs" / "bapanas_walk_state.json"

# Mapping komoditas LUMBUNG -> commodity_id di Bapanas
# (ID per Bapanas Panel Harga, level_harga_id=3 = konsumen retail)
BAPANAS_COMMODITY_ID = {
    "beras":             28,   # Beras Medium
    "cabai_rawit_merah":  7,   # Cabai Rawit Merah
    "cabai_keriting":     6,   # Cabai Merah Keriting
    "bawang_merah":      23,
    "bawang_putih":      24,
}

# Snapshot harga aktual Juni 2026 (Rp/kg) - kalibrasi dari PIHPS BI
SNAPSHOT_PRICE = {
    "beras":             13500.0,
    "cabai_rawit_merah": 83500.0,
    "cabai_keriting":    52000.0,
    "bawang_merah":      37000.0,
    "bawang_putih":      41000.0,
}

# Volatilitas harian (std dev sebagai fraksi dari harga, kalibrasi historis)
VOLATILITY = {
    "beras":             0.003,
    "cabai_rawit_merah": 0.045,
    "cabai_keriting":    0.038,
    "bawang_merah":      0.022,
    "bawang_putih":      0.018,
}

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
FALLBACK_FILE = LOG_DIR / "price_bapanas_fallback.jsonl"

USER_AGENT = "Mozilla/5.0 (LUMBUNG-bot/0.1; ITS Surabaya BigData FP)"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("producer_bapanas")


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


def fetch_bapanas_api(commodity_id: int, period_date: str | None = None) -> dict | None:
    """Hit Bapanas Panel Harga API untuk 1 commodity. Return dict harga atau None."""
    if period_date is None:
        period_date = datetime.now().strftime("%d/%m/%Y")
    params = {
        "province_id": "",      # kosong = nasional
        "city_id": "",
        "level_harga_id": 3,    # konsumen retail
        "commodity_id": commodity_id,
        "period_date": period_date,
    }
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if BAPANAS_API_KEY:
        # Coba beberapa header umum yang Bapanas pakai
        headers["X-API-KEY"] = BAPANAS_API_KEY
        headers["Authorization"] = f"Bearer {BAPANAS_API_KEY}"
        params["api_key"] = BAPANAS_API_KEY
    url = f"{BAPANAS_BASE}{BAPANAS_ENDPOINT}"
    try:
        r = requests.get(url, params=params, timeout=12, headers=headers)
        if r.status_code == 401:
            log.debug(f"401 Unauthorized commodity={commodity_id} - "
                      f"set BAPANAS_API_KEY env var untuk akses real-time")
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.debug(f"Bapanas API fail commodity_id={commodity_id}: {type(e).__name__}: {e}")
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


def extract_price_from_api(raw: dict) -> float | None:
    """Parser fleksibel - Bapanas kadang ubah skema. Coba beberapa shape."""
    if raw is None:
        return None
    # Coba beberapa shape umum
    candidates = []
    if isinstance(raw, dict):
        # Shape A: {data: [{average: 13500, ...}]}
        if isinstance(raw.get("data"), list) and raw["data"]:
            row = raw["data"][0]
            for key in ("average", "today", "harga", "price"):
                v = row.get(key) if isinstance(row, dict) else None
                if v is not None:
                    candidates.append(v)
        # Shape B: {data: {average: 13500}}
        if isinstance(raw.get("data"), dict):
            for key in ("average", "today", "harga", "price"):
                v = raw["data"].get(key)
                if v is not None:
                    candidates.append(v)
        # Shape C: langsung di root
        for key in ("average", "today", "harga", "price"):
            v = raw.get(key)
            if v is not None:
                candidates.append(v)

    for c in candidates:
        try:
            f = float(c)
            if f > 0:
                return f
        except (TypeError, ValueError):
            continue
    return None


def snapshot_with_walk(komoditas: str, state: dict) -> float:
    """Random walk persisten dengan mean-reversion ke baseline.

    Setiap call: harga = harga_kemarin * (1 + drift) dengan drift gaussian
    yang sedikit di-pull balik ke baseline (mean reversion). Hasil time series
    realistic untuk training ML, bukan jitter independen.
    """
    base = SNAPSHOT_PRICE[komoditas]
    vol = VOLATILITY[komoditas]
    last = state.get(komoditas, base)

    # Mean reversion: dorong balik ke baseline kalau menyimpang
    deviation = (last - base) / base
    mean_revert_pull = -0.1 * deviation   # 10% per step balik ke base
    random_drift = random.gauss(0, vol)
    drift = mean_revert_pull + random_drift

    new_price = max(last * (1 + drift), base * 0.5)  # floor 50% baseline
    new_price = min(new_price, base * 2.0)            # cap 200% baseline
    new_price = round(new_price, 0)

    state[komoditas] = new_price
    return new_price


def build_message(komoditas: str, price: float, source: str) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "source": "bapanas",
        "data_source": source,                  # "bapanas-api" | "bapanas-snapshot"
        "komoditas": komoditas,
        "level_harga": "konsumen",
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

    for komoditas, commodity_id in BAPANAS_COMMODITY_ID.items():
        price = None
        source = None

        raw = fetch_bapanas_api(commodity_id)
        if raw is not None:
            price = extract_price_from_api(raw)
            if price is not None:
                source = "bapanas-api"
                api_hits += 1
                walk_state[komoditas] = price

        if price is None:
            price = snapshot_with_walk(komoditas, walk_state)
            source = "bapanas-snapshot"
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

    api_status = "ON" if BAPANAS_API_KEY else "OFF (no API key)"
    log.info(f"Selesai: {success}/{len(BAPANAS_COMMODITY_ID)} "
             f"(API={api_hits} | snapshot={snapshot_hits}) | key={api_status}")
    return success


def main() -> int:
    parser = argparse.ArgumentParser(description="LUMBUNG Bapanas price producer")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--interval", type=int, default=FETCH_INTERVAL_SEC)
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("LUMBUNG producer_price_bapanas - Panel Harga Bapanas -> Kafka")
    log.info(f"Komoditas: {list(BAPANAS_COMMODITY_ID.keys())}")
    log.info(f"Topic    : {TOPIC}")
    log.info(f"API key  : {'SET' if BAPANAS_API_KEY else 'NOT SET (fallback ke snapshot)'}")
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
