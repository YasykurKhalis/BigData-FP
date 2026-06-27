"""
LUMBUNG — Export Gold tables → JSON lokal
Owner: Yasykur (patched)

Mengekspor tabel Gold (Feature Store) ke format statis JSON
agar dashboard Flask tidak perlu menyalakan PySpark.

Menggunakan deltalake + pandas (tanpa PySpark/JVM).

Output:
  - temp_buffer/export/feature_store.json   (array of records)
  - temp_buffer/export/price_history.json   (harga historis per komoditas)
"""

from __future__ import annotations
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import read_delta

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("export_layer")

BASE_DIR   = Path(__file__).resolve().parent.parent
GOLD_DIR   = BASE_DIR / "temp_buffer" / "lakehouse" / "gold"
EXPORT_DIR = BASE_DIR / "temp_buffer" / "export"


def export_gold_to_json() -> bool:
    feature_store_path = GOLD_DIR / "feature_store"
    if not feature_store_path.exists():
        log.error("Tabel Gold feature_store tidak ditemukan. Export dibatalkan.")
        return False

    df = read_delta(str(feature_store_path))
    if df.empty:
        log.error("Tabel Gold feature_store kosong. Export dibatalkan.")
        return False

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Export feature_store lengkap sebagai JSON array
    records = df.to_dict(orient="records")
    out_path = EXPORT_DIR / "feature_store.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, default=str)
    log.info(f"Berhasil mengekspor {len(records)} record ke {out_path}")

    # 2. Export ringkasan harga historis per komoditas (untuk grafik tren)
    komoditas_col = "komoditas" if "komoditas" in df.columns else "commodity"
    if komoditas_col in df.columns and "date_parsed" in df.columns and "avg_price" in df.columns:
        price_history: dict[str, list] = {}
        for row in records:
            kom  = str(row.get(komoditas_col, ""))
            date = str(row.get("date_parsed", row.get("date", "")))
            price = row.get("avg_price")
            if kom and price is not None:
                if kom not in price_history:
                    price_history[kom] = []
                price_history[kom].append({"date": date, "price": price})

        # Urutkan per tanggal
        for kom in price_history:
            price_history[kom].sort(key=lambda r: r["date"])

        ph_path = EXPORT_DIR / "price_history.json"
        with open(ph_path, "w", encoding="utf-8") as f:
            json.dump(price_history, f, ensure_ascii=False)
        log.info(f"Berhasil mengekspor price_history ke {ph_path}")

    return True


if __name__ == "__main__":
    success = export_gold_to_json()
    sys.exit(0 if success else 1)
