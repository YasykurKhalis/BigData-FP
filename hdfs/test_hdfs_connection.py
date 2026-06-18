"""LUMBUNG - HDFS connection smoke test (via WebHDFS, no JNI/libhdfs).

Pakai library `hdfs` (HTTP WebHDFS) supaya tidak butuh libhdfs.so.
Tepat untuk demo: cek namenode hidup, buat folder, tulis file, baca balik.

USAGE:
    python hdfs/test_hdfs_connection.py
    python hdfs/test_hdfs_connection.py --cleanup
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone

# Patch DNS supaya host Windows bisa follow WebHDFS redirect ke `datanode:9864`
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _dns_patch import patch_dns  # noqa: E402
patch_dns()

WEBHDFS_URL = "http://localhost:9870"
HDFS_USER = "root"
TEST_ROOT = "/data/lumbung"
TEST_DIR = f"{TEST_ROOT}/smoke_test"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("hdfs_test")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cleanup", action="store_true",
                        help="Hapus folder smoke_test setelah test")
    args = parser.parse_args()

    try:
        from hdfs import InsecureClient
    except ImportError:
        log.error("Library `hdfs` belum terinstall.")
        log.error("Install: pip install hdfs")
        return 2

    log.info("=" * 60)
    log.info(f"WebHDFS URL : {WEBHDFS_URL}")
    log.info(f"User        : {HDFS_USER}")
    log.info("=" * 60)

    try:
        client = InsecureClient(WEBHDFS_URL, user=HDFS_USER)
    except Exception as e:
        log.error(f"Gagal membuat client: {e}")
        return 1

    # 1. Cek status namenode
    log.info("[1/5] Cek status NameNode...")
    try:
        root_status = client.status("/")
        log.info(f"      OK - root /  type={root_status['type']}")
    except Exception as e:
        log.error(f"      FAIL - NameNode tidak respon: {e}")
        log.error("      Pastikan: docker compose -f docker-compose-hadoop.yml up -d")
        return 1

    # 2. Buat folder /data/lumbung
    log.info(f"[2/5] makedirs {TEST_ROOT}...")
    try:
        client.makedirs(TEST_ROOT)
        log.info("      OK")
    except Exception as e:
        log.error(f"      FAIL: {e}")
        return 1

    # 3. Tulis file test
    test_file = f"{TEST_DIR}/hello_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    payload = {
        "project": "LUMBUNG",
        "test": "hdfs_connectivity",
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "from": "test_hdfs_connection.py",
        "message": "Halo HDFS dari LUMBUNG!",
    }
    log.info(f"[3/5] Tulis {test_file}...")
    try:
        client.makedirs(TEST_DIR)
        with client.write(test_file, encoding="utf-8", overwrite=True) as w:
            json.dump(payload, w, ensure_ascii=False, indent=2)
        log.info(f"      OK - {len(json.dumps(payload))} bytes")
    except Exception as e:
        log.error(f"      FAIL: {e}")
        return 1

    # 4. List folder
    log.info(f"[4/5] List {TEST_DIR}...")
    try:
        files = client.list(TEST_DIR)
        for f in files[-5:]:
            log.info(f"      - {f}")
    except Exception as e:
        log.error(f"      FAIL: {e}")
        return 1

    # 5. Baca balik
    log.info(f"[5/5] Read {test_file}...")
    try:
        with client.read(test_file, encoding="utf-8") as r:
            content = r.read()
        echo = json.loads(content)
        log.info(f"      OK - message='{echo.get('message')}'")
    except Exception as e:
        log.error(f"      FAIL: {e}")
        return 1

    if args.cleanup:
        log.info(f"Cleanup: hapus {TEST_DIR}...")
        try:
            client.delete(TEST_DIR, recursive=True)
            log.info("      OK")
        except Exception as e:
            log.warning(f"      Gagal cleanup: {e}")

    log.info("")
    log.info("=" * 60)
    log.info("SUCCESS - HDFS reachable & writable")
    log.info(f"Buka UI: {WEBHDFS_URL}/explorer.html#{TEST_DIR}")
    log.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
