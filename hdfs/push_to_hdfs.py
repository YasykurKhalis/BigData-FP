"""
LUMBUNG — Push semua data lokal ke HDFS
Owner: tim

1. Upload raw JSONL streaming + batch dari temp_buffer → HDFS
2. Upload export JSON (risk_index, forecast, dll) → HDFS export/
3. Upload ML models → HDFS models/

Gunakan WebHDFS REST via library `hdfs` + DNS patch container.

USAGE:
    python hdfs/push_to_hdfs.py
    python hdfs/push_to_hdfs.py --only-export   # hanya upload export JSON
    python hdfs/push_to_hdfs.py --only-raw       # hanya upload raw data
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import sys
from pathlib import Path

# Patch DNS agar hostname container bisa di-resolve dari host
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _dns_patch import patch_dns
patch_dns()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("push_to_hdfs")

WEBHDFS_URL = os.getenv("WEBHDFS_URL", "http://localhost:9870")
HDFS_USER   = os.getenv("HDFS_USER", "root")
HDFS_ROOT   = "/data/lumbung"

BASE_DIR   = Path(__file__).resolve().parent.parent
TEMP_DIR   = BASE_DIR / "temp_buffer"
EXPORT_DIR = TEMP_DIR / "export"
MODEL_DIR  = BASE_DIR / "ml" / "models"


def get_hdfs_client():
    try:
        from hdfs import InsecureClient
    except ImportError:
        log.error("Library `hdfs` tidak terinstall. Jalankan: pip install hdfs")
        sys.exit(1)

    client = InsecureClient(WEBHDFS_URL, user=HDFS_USER)
    try:
        client.status("/")
        log.info(f"Terhubung ke HDFS: {WEBHDFS_URL} sebagai {HDFS_USER}")
    except Exception as e:
        log.error(f"Gagal terhubung ke HDFS: {e}")
        sys.exit(1)
    return client


def hdfs_makedirs(client, path: str) -> None:
    try:
        client.makedirs(path)
    except Exception:
        pass  # Direktori sudah ada


def upload_file(client, local_path: Path, hdfs_path: str, overwrite: bool = True) -> bool:
    try:
        hdfs_makedirs(client, str(Path(hdfs_path).parent))
        with open(local_path, "rb") as f:
            client.write(hdfs_path, f, overwrite=overwrite)
        size_kb = local_path.stat().st_size / 1024
        log.info(f"  ✓ {local_path.name:45s} → {hdfs_path} ({size_kb:.1f} KB)")
        return True
    except Exception as e:
        log.error(f"  ✗ Gagal upload {local_path}: {e}")
        return False


def push_raw_streaming(client) -> int:
    """Upload semua file JSONL streaming ke HDFS /data/lumbung/streaming/"""
    streaming_dir = TEMP_DIR / "streaming"
    if not streaming_dir.exists():
        log.warning("temp_buffer/streaming/ tidak ditemukan, skip.")
        return 0

    count = 0
    for jsonl_file in sorted(streaming_dir.rglob("*.jsonl")):
        # Pertahankan struktur relatif: streaming/prices/bapanas/2026-06-23/batch_xxx.jsonl
        rel = jsonl_file.relative_to(TEMP_DIR)
        hdfs_target = f"{HDFS_ROOT}/{rel}"
        if upload_file(client, jsonl_file, hdfs_target):
            count += 1

    log.info(f"Streaming upload: {count} file")
    return count


def push_raw_batch(client) -> int:
    """Upload semua file JSONL batch ke HDFS /data/lumbung/batch/"""
    batch_dir = TEMP_DIR / "batch"
    if not batch_dir.exists():
        log.warning("temp_buffer/batch/ tidak ditemukan, skip.")
        return 0

    count = 0
    for jsonl_file in sorted(batch_dir.rglob("*.jsonl")):
        rel = jsonl_file.relative_to(TEMP_DIR)
        hdfs_target = f"{HDFS_ROOT}/{rel}"
        if upload_file(client, jsonl_file, hdfs_target):
            count += 1

    log.info(f"Batch upload: {count} file")
    return count


def push_export_json(client) -> int:
    """Upload semua file JSON export ke HDFS /data/lumbung/export/"""
    if not EXPORT_DIR.exists():
        log.warning("temp_buffer/export/ tidak ditemukan, skip.")
        return 0

    count = 0
    for json_file in sorted(EXPORT_DIR.glob("*.json")):
        hdfs_target = f"{HDFS_ROOT}/export/{json_file.name}"
        if upload_file(client, json_file, hdfs_target):
            count += 1

    log.info(f"Export JSON upload: {count} file")
    return count


def push_ml_models(client) -> int:
    """Upload model .joblib ke HDFS /data/lumbung/models/"""
    if not MODEL_DIR.exists():
        log.warning("ml/models/ tidak ditemukan, skip.")
        return 0

    count = 0
    for model_file in sorted(MODEL_DIR.glob("*.joblib")):
        hdfs_target = f"{HDFS_ROOT}/models/{model_file.name}"
        if upload_file(client, model_file, hdfs_target):
            count += 1

    log.info(f"ML Models upload: {count} file")
    return count


def verify_hdfs_structure(client) -> None:
    """Tampilkan ringkasan isi HDFS setelah push."""
    log.info("\n=== Struktur HDFS /data/lumbung/ ===")
    try:
        def list_recursive(path: str, depth: int = 0, max_depth: int = 3):
            if depth > max_depth:
                return
            try:
                statuses = client.list(path, status=True)
                for name, info in statuses:
                    full = f"{path}/{name}"
                    ftype = "DIR" if info["type"] == "DIRECTORY" else f"{info['length']//1024}KB"
                    log.info(f"  {'  ' * depth}{'📁' if info['type']=='DIRECTORY' else '📄'} {name} [{ftype}]")
                    if info["type"] == "DIRECTORY" and depth < max_depth:
                        list_recursive(full, depth + 1, max_depth)
            except Exception:
                pass

        list_recursive(f"{HDFS_ROOT}")
    except Exception as e:
        log.warning(f"Tidak bisa list HDFS: {e}")


def main() -> int:
    parser = argparse.ArgumentParser(description="LUMBUNG push to HDFS")
    parser.add_argument("--only-export", action="store_true", help="Hanya upload export JSON")
    parser.add_argument("--only-raw",    action="store_true", help="Hanya upload raw JSONL")
    args = parser.parse_args()

    client = get_hdfs_client()
    total = 0

    log.info("=" * 60)
    log.info("LUMBUNG — Push data ke HDFS")
    log.info(f"Target: {WEBHDFS_URL}{HDFS_ROOT}")
    log.info("=" * 60)

    if args.only_export:
        total += push_export_json(client)
        total += push_ml_models(client)
    elif args.only_raw:
        total += push_raw_streaming(client)
        total += push_raw_batch(client)
    else:
        # Upload semua
        log.info("\n[1/4] Raw Streaming JSONL →")
        total += push_raw_streaming(client)

        log.info("\n[2/4] Raw Batch JSONL →")
        total += push_raw_batch(client)

        log.info("\n[3/4] Export JSON →")
        total += push_export_json(client)

        log.info("\n[4/4] ML Models →")
        total += push_ml_models(client)

    verify_hdfs_structure(client)

    log.info(f"\n✅ Selesai: {total} file berhasil di-upload ke HDFS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
