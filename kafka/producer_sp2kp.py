"""LUMBUNG - Producer SP2KP Kemendag (BATCH)
Owner: Ryan (5027231046)

Pull data harga kebutuhan pokok dari SP2KP (Sistem Pemantauan Pasar
Kebutuhan Pokok) Kementerian Perdagangan - sumber harga konsumen
dan grosir di pasar modern & tradisional tingkat kota/kabupaten.

URL primary  : https://sp2kp.kemendag.go.id/
               (endpoint publik Kemendag)
Fallback     : baseline harga pasar realistis + random walk
               agar pipeline tetap menghasilkan event untuk demo.

Berbeda dari PIHPS (Bank Indonesia):
  - SP2KP mengukur di pasar modern DAN tradisional
  - Mencakup harga grosir dan konsumen
  - Level harga biasanya sedikit berbeda dari PIHPS

Setiap event mencatat field `data_source` eksplisit:
  - "sp2kp-api"           = real-time dari SP2KP Kemendag
  - "synthetic-fallback"  = fallback ke baseline + walk

Komoditas (10 fokus LUMBUNG):
  beras, cabai_rawit_merah, cabai_keriting, bawang_merah, bawang_putih,
  gula_pasir, minyak_goreng, daging_ayam, telur_ayam, daging_sapi

USAGE:
    python kafka/producer_sp2kp.py --dry-run --once
    python kafka/producer_sp2kp.py --once
    python kafka/producer_sp2kp.py --batch
    python kafka/producer_sp2kp.py
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
TOPIC = "sp2kp-kemendag"
FETCH_INTERVAL_SEC = 24 * 60 * 60  # harian

SP2KP_BASE = "https://sp2kp.kemendag.go.id"
SP2KP_API_ENDPOINT = "/api/harga-kebutuhan-pokok"

STATE_FILE = Path(__file__).resolve().parent.parent / "logs" / "sp2kp_walk_state.json"

KOMODITAS_LIST = [
    "beras", "cabai_rawit_merah", "cabai_keriting", "bawang_merah",
    "bawang_putih", "gula_pasir", "minyak_goreng", "daging_ayam",
    "telur_ayam", "daging_sapi",
]

# Baseline harga SP2KP (Rp/kg) - sedikit berbeda dari PIHPS
# SP2KP mengukur di pasar modern & tradisional, harga rata-rata nasional
BASELINE_HARGA = {
    "beras": {
        "harga_konsumen": 13_800,   # sedikit lebih tinggi dari PIHPS
        "harga_grosir":   12_500,
    },
    "cabai_rawit_merah": {
        "harga_konsumen": 85_000,
        "harga_grosir":   72_000,
    },
    "cabai_keriting": {
        "harga_konsumen": 53_000,
        "harga_grosir":   44_000,
    },
    "bawang_merah": {
        "harga_konsumen": 38_000,
        "harga_grosir":   31_000,
    },
    "bawang_putih": {
        "harga_konsumen": 42_000,
        "harga_grosir":   35_000,
    },
    "gula_pasir": {
        "harga_konsumen": 21_000,
        "harga_grosir":   18_500,
    },
    "minyak_goreng": {
        "harga_konsumen": 21_500,
        "harga_grosir":   19_000,
    },
    "daging_ayam": {
        "harga_konsumen": 38_500,
        "harga_grosir":   33_000,
    },
    "telur_ayam": {
        "harga_konsumen": 30_500,
        "harga_grosir":   27_000,
    },
    "daging_sapi": {
        "harga_konsumen": 152_000,
        "harga_grosir":   135_000,
    },
}

# Volatilitas harian (std dev sebagai fraksi dari harga)
VOLATILITY = {
    "beras":             0.004,
    "cabai_rawit_merah": 0.050,
    "cabai_keriting":    0.042,
    "bawang_merah":      0.025,
    "bawang_putih":      0.020,
    "gula_pasir":        0.006,
    "minyak_goreng":     0.009,
    "daging_ayam":       0.016,
    "telur_ayam":        0.013,
    "daging_sapi":       0.006,
}

PASAR_SAMPLE = [
    ("Pasar Tanah Abang", "Jakarta Pusat", "DKI Jakarta"),
    ("Pasar Kramat Jati", "Jakarta Timur", "DKI Jakarta"),
    ("Pasar Caringin", "Bandung", "Jawa Barat"),
    ("Pasar Johar", "Semarang", "Jawa Tengah"),
    ("Pasar Pabean", "Surabaya", "Jawa Timur"),
    ("Pasar Beringharjo", "Yogyakarta", "DI Yogyakarta"),
    ("Pasar Raya Padang", "Padang", "Sumatera Barat"),
    ("Pasar Petisah", "Medan", "Sumatera Utara"),
    ("Pasar Daya", "Makassar", "Sulawesi Selatan"),
    ("Pasar Badung", "Denpasar", "Bali"),
]

LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
FALLBACK_FILE = LOG_DIR / "sp2kp_fallback.jsonl"

USER_AGENT = "Mozilla/5.0 (LUMBUNG-bot/0.1; ITS Surabaya BigData FP)"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("producer_sp2kp")


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


def fetch_sp2kp_harga(komoditas: str) -> dict | None:
    """
    Ambil harga dari SP2KP Kemendag API.
    Return dict {harga_konsumen, harga_grosir} atau None.
    """
    try:
        url = f"{SP2KP_BASE}{SP2KP_API_ENDPOINT}"
        headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
        params = {"commodity": komoditas}
        r = requests.get(url, params=params, timeout=15, headers=headers)
        r.raise_for_status()
        body = r.json()
        if body.get("data"):
            return body["data"]
    except Exception as e:
        log.debug(f"SP2KP API fail {komoditas}: {type(e).__name__}: {e}")
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


def synthetic_harga_sp2kp(komoditas: str, state: dict) -> dict:
    """
    Generate harga sintetis SP2KP dengan random walk.
    Harga konsumen dan grosir bergerak korrelasi tinggi.
    """
    baseline = BASELINE_HARGA.get(komoditas, {
        "harga_konsumen": 15_000, "harga_grosir": 12_000,
    })
    vol = VOLATILITY.get(komoditas, 0.01)

    # Harga konsumen
    key_kons = f"{komoditas}_konsumen"
    base_kons = baseline["harga_konsumen"]
    last_kons = state.get(key_kons, base_kons)

    deviation = (last_kons - base_kons) / base_kons
    mean_revert = -0.1 * deviation
    drift = random.gauss(0, vol) + mean_revert

    new_kons = max(last_kons * (1 + drift), base_kons * 0.5)
    new_kons = min(new_kons, base_kons * 2.0)
    new_kons = round(new_kons, 0)
    state[key_kons] = new_kons

    # Harga grosir: korrelasi dengan konsumen, selalu lebih rendah
    key_gros = f"{komoditas}_grosir"
    base_gros = baseline["harga_grosir"]
    margin_ratio = base_gros / base_kons  # rasio grosir/konsumen tetap
    new_gros = new_kons * margin_ratio
    # Tambah sedikit noise independen
    new_gros *= (1 + random.gauss(0, vol * 0.3))
    new_gros = max(new_gros, base_gros * 0.5)
    new_gros = min(new_gros, new_kons * 0.98)  # grosir selalu < konsumen
    new_gros = round(new_gros, 0)
    state[key_gros] = new_gros

    return {
        "harga_konsumen": new_kons,
        "harga_grosir": new_gros,
    }


def build_message(komoditas: str, data: dict, source: str,
                  pasar: str, kota: str, provinsi: str) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "source": "sp2kp-kemendag",
        "data_source": source,
        "komoditas": komoditas,
        "harga_konsumen": data["harga_konsumen"],
        "harga_grosir": data["harga_grosir"],
        "currency": "IDR",
        "unit": "kg",
        "pasar": pasar,
        "kota": kota,
        "provinsi": provinsi,
        "country": "ID",
        "fetched_at_utc": now,
        "ingestion_ts": now,
    }


def send_or_fallback(producer, msg: dict, dry_run: bool = False) -> None:
    key = msg["komoditas"]
    if dry_run:
        log.info(f"[DRY-RUN] {key:22s} | konsumen=Rp {int(msg['harga_konsumen']):>7,}/kg | "
                 f"grosir=Rp {int(msg['harga_grosir']):>7,}/kg | "
                 f"{msg['pasar']} | src={msg['data_source']}")
        return
    if producer is not None:
        try:
            producer.send(TOPIC, key=key, value=msg)
            log.info(f"-> Kafka [{TOPIC}] {key:22s} | konsumen=Rp {int(msg['harga_konsumen']):>7,}/kg | "
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

    pasar_list = PASAR_SAMPLE if batch else [random.choice(PASAR_SAMPLE)]

    for komoditas in KOMODITAS_LIST:
        for pasar, kota, provinsi in pasar_list:
            api_data = fetch_sp2kp_harga(komoditas)
            source = None

            if api_data is not None:
                source = "sp2kp-api"
                api_hits += 1
                data = api_data
            else:
                data = synthetic_harga_sp2kp(komoditas, walk_state)
                source = "synthetic-fallback"
                synthetic_hits += 1

            msg = build_message(komoditas, data, source, pasar, kota, provinsi)
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
    parser = argparse.ArgumentParser(description="LUMBUNG SP2KP Kemendag batch producer")
    parser.add_argument("--once", action="store_true", help="Single fetch lalu exit")
    parser.add_argument("--dry-run", action="store_true", help="Print ke stdout, tanpa Kafka")
    parser.add_argument("--batch", action="store_true", help="Batch mode: semua pasar sekaligus")
    parser.add_argument("--interval", type=int, default=FETCH_INTERVAL_SEC)
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("LUMBUNG producer_sp2kp - SP2KP Kemendag -> Kafka")
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
