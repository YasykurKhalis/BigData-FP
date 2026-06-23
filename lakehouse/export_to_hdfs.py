"""
LUMBUNG — Export Gold tables → JSON di HDFS/Lokal
Owner: Yasykur

Mengekspor tabel Gold (Feature Store) ke format statis JSON
agar dashboard Flask tidak perlu menyalakan PySpark.

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
from utils import get_spark_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("export_layer")

BASE_DIR   = Path(__file__).resolve().parent.parent
GOLD_DIR   = BASE_DIR / "temp_buffer" / "lakehouse" / "gold"
EXPORT_DIR = BASE_DIR / "temp_buffer" / "export"


def export_gold_to_json() -> bool:
    spark = get_spark_session("Lumbung_Export")

    feature_store_path = GOLD_DIR / "feature_store"
    if not feature_store_path.exists():
        log.error("Tabel Gold feature_store tidak ditemukan. Export dibatalkan.")
        spark.stop()
        return False

    df = spark.read.format("delta").load(str(feature_store_path))
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Export feature_store lengkap sebagai JSON array
    # Repartition(1) agar Spark menulis ke satu file saja
    tmp_path = str(EXPORT_DIR / "_tmp_feature_store")
    df.coalesce(1).write.format("json").mode("overwrite").save(tmp_path)

    # Gabungkan part files menjadi satu JSON array
    tmp_dir = Path(tmp_path)
    records = []
    for part in sorted(tmp_dir.glob("part-*.json")):
        with open(part, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass

    out_path = EXPORT_DIR / "feature_store.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, default=str)
    log.info(f"Berhasil mengekspor {len(records)} record ke {out_path}")

    # Cleanup tmp
    import shutil
    shutil.rmtree(tmp_path, ignore_errors=True)

    # 2. Export ringkasan harga historis per komoditas (untuk grafik tren)
    if "commodity" in df.columns and "date_parsed" in df.columns and "avg_price" in df.columns:
        price_history: dict[str, list] = {}
        for row in records:
            kom  = row.get("commodity", "")
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

    spark.stop()
    return True


if __name__ == "__main__":
    success = export_gold_to_json()
    sys.exit(0 if success else 1)
