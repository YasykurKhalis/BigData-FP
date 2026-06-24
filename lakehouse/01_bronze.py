"""
LUMBUNG — Bronze layer: raw ingest dari HDFS + metadata
Owner: Yasykur (patched Ryan)

Membaca data raw JSONL dari HDFS (WebHDFS REST), menambah metadata ingest,
dan menyimpan sebagai tabel append-only di layer Bronze (Delta Lake).

Flow: HDFS (WebHDFS) -> pandas -> Delta Lake (lokal)
"""

from __future__ import annotations
import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "hdfs"))
from utils import write_delta, now_utc, BRONZE_DIR
from _dns_patch import patch_dns

import pandas as pd

# Patch DNS untuk resolve container hostname -> localhost
patch_dns()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bronze_layer")

WEBHDFS_URL = os.getenv("WEBHDFS_URL", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
HDFS_ROOT = "/data/lumbung"

# Juga baca dari lokal (untuk historical seed dll)
BASE_DIR = Path(__file__).resolve().parent.parent
LOCAL_BUFFER = BASE_DIR / "temp_buffer"

# Mapping: nama stream -> HDFS path relatif terhadap HDFS_ROOT
HDFS_STREAMS = {
    "price_bapanas":      "streaming/prices/bapanas",
    "price_pihps":        "streaming/prices/pihps",
    "price_siskaperbapo": "streaming/prices/siskaperbapo",
    "weather":            "streaming/weather",
    "news":               "streaming/news",
    "kurs":               "streaming/kurs",
}

# Batch ingest dirs (lokal saja)
BATCH_STREAMS = {
    "batch_produksi":     LOCAL_BUFFER / "batch" / "bps_produksi",
    "batch_imporekspor":  LOCAL_BUFFER / "batch" / "bps_imporekspor",
    "batch_bulog_stok":   LOCAL_BUFFER / "batch" / "bulog_stok",
    "batch_pupuk_harga":  LOCAL_BUFFER / "batch" / "pupuk_harga",
}


def _get_hdfs_client():
    """Return WebHDFS client atau None."""
    try:
        from hdfs import InsecureClient
        client = InsecureClient(WEBHDFS_URL, user=HDFS_USER)
        client.status("/")
        log.info(f"HDFS connected: {WEBHDFS_URL}")
        return client
    except Exception as e:
        log.warning(f"HDFS unavailable: {e}")
        return None


def _read_jsonl_from_hdfs(client, hdfs_dir: str) -> list[dict]:
    """Baca semua .jsonl file dari HDFS directory (rekursif)."""
    records = []
    try:
        # List semua file secara rekursif
        for dirpath, dirnames, filenames in client.walk(hdfs_dir):
            for fname in filenames:
                if not fname.endswith(".jsonl"):
                    continue
                fpath = f"{dirpath}/{fname}"
                try:
                    with client.read(fpath, encoding="utf-8") as reader:
                        content = reader.read()
                        for line in content.strip().split("\n"):
                            line = line.strip()
                            if line:
                                records.append(json.loads(line))
                except Exception as e:
                    log.warning(f"  Gagal baca {fpath}: {e}")
    except Exception as e:
        log.warning(f"  Gagal walk {hdfs_dir}: {e}")
    return records


def _read_jsonl_from_local(directory: Path) -> list[dict]:
    """Baca semua .jsonl file dari directory lokal (rekursif)."""
    records = []
    if not directory.exists():
        return records
    for f in sorted(directory.rglob("*.jsonl")):
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
    return records


def process_bronze():
    client = _get_hdfs_client()

    # ── 1. Streaming data dari HDFS ──────────────────────────────────────
    for name, hdfs_subpath in HDFS_STREAMS.items():
        hdfs_dir = f"{HDFS_ROOT}/{hdfs_subpath}"
        log.info(f"Memproses {name} dari HDFS:{hdfs_dir} ...")

        records = []

        # Baca dari HDFS (satu-satunya sumber untuk streaming)
        if client:
            hdfs_records = _read_jsonl_from_hdfs(client, hdfs_dir)
            log.info(f"  HDFS: {len(hdfs_records)} records")
            records.extend(hdfs_records)
        else:
            log.error("  HDFS tidak tersedia! Data streaming HARUS dari HDFS.")

        if not records:
            log.warning(f"  Tidak ada data untuk {name}, lewati.")
            continue

        df = pd.DataFrame(records)

        # Tambahkan metadata bronze
        df["_ingested_to_bronze_at"] = now_utc()
        df["_source_layer"] = "bronze"
        df["_stream_name"] = name

        # Tulis ke Delta Lake
        bronze_table_path = str(BRONZE_DIR / name)
        write_delta(df, bronze_table_path, mode="overwrite")
        log.info(f"  Berhasil menulis {len(df)} records ke bronze/{name}")

    # ── 2. Batch data dari lokal ─────────────────────────────────────────
    for name, path in BATCH_STREAMS.items():
        if not path.exists():
            log.warning(f"Path tidak ditemukan, lewati: {path}")
            continue

        log.info(f"Memproses {name} dari {path} ...")
        records = _read_jsonl_from_local(path)

        if not records:
            log.warning(f"  Tidak ada data untuk {name}, lewati.")
            continue

        df = pd.DataFrame(records)
        df["_ingested_to_bronze_at"] = now_utc()
        df["_source_layer"] = "bronze"
        df["_stream_name"] = name

        bronze_table_path = str(BRONZE_DIR / name)
        write_delta(df, bronze_table_path, mode="overwrite")
        log.info(f"  Berhasil menulis {len(df)} records ke bronze/{name}")

    log.info("Bronze layer selesai.")


if __name__ == "__main__":
    process_bronze()
