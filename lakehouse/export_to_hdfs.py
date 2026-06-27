"""
LUMBUNG — Export Gold tables → JSON di HDFS/Lokal
Owner: Yasykur (patched Ryan)

Mengekspor tabel Gold (Feature Store) ke format statis JSON + upload ke HDFS.

Output:
  - temp_buffer/export/feature_store.json   (array of records)
  - temp_buffer/export/price_history.json   (harga historis per komoditas)
  - HDFS:/data/lumbung/export/              (mirror)
"""

from __future__ import annotations
import json
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "hdfs"))
from utils import read_delta, GOLD_DIR
from _dns_patch import patch_dns

patch_dns()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("export_layer")

BASE_DIR   = Path(__file__).resolve().parent.parent
EXPORT_DIR = BASE_DIR / "temp_buffer" / "export"

WEBHDFS_URL = os.getenv("WEBHDFS_URL", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
HDFS_EXPORT = "/data/lumbung/export"


def _upload_to_hdfs(local_path: Path, hdfs_path: str):
    """Upload file ke HDFS via WebHDFS."""
    try:
        from hdfs import InsecureClient
        client = InsecureClient(WEBHDFS_URL, user=HDFS_USER)
        client.makedirs(os.path.dirname(hdfs_path))
        with open(local_path, "rb") as f:
            client.write(hdfs_path, f, overwrite=True)
        log.info(f"  Uploaded ke HDFS:{hdfs_path}")
    except Exception as e:
        log.warning(f"  HDFS upload gagal: {e}")


def export_gold_to_json() -> bool:
    feature_store_path = str(GOLD_DIR / "feature_store")
    df = read_delta(feature_store_path)

    if df.empty:
        log.error("Tabel Gold feature_store kosong. Export dibatalkan.")
        return False

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Export feature_store lengkap
    records = df.to_dict(orient="records")
    out_path = EXPORT_DIR / "feature_store.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2, default=str)
    log.info(f"Berhasil mengekspor {len(records)} record ke {out_path}")
    _upload_to_hdfs(out_path, f"{HDFS_EXPORT}/feature_store.json")

    # 2. Export price_history per komoditas (untuk grafik tren)
    komoditas_col = "komoditas" if "komoditas" in df.columns else "commodity"
    if komoditas_col in df.columns and "date_parsed" in df.columns and "avg_price" in df.columns:
        price_history: dict[str, list] = {}
        for row in records:
            kom = row.get(komoditas_col, "")
            date = str(row.get("date_parsed", ""))
            price = row.get("avg_price")
            if kom and price is not None:
                price_history.setdefault(kom, []).append({"date": date, "price": price})

        for kom in price_history:
            price_history[kom].sort(key=lambda r: r["date"])

        ph_path = EXPORT_DIR / "price_history.json"
        with open(ph_path, "w", encoding="utf-8") as f:
            json.dump(price_history, f, ensure_ascii=False)
        log.info(f"Berhasil mengekspor price_history ke {ph_path}")
        _upload_to_hdfs(ph_path, f"{HDFS_EXPORT}/price_history.json")

    return True


if __name__ == "__main__":
    success = export_gold_to_json()
    sys.exit(0 if success else 1)
