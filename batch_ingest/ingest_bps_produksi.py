"""
LUMBUNG — Batch ingest Produksi Pertanian
Owner: Yasykur

Mengambil data Crop Production Index Indonesia dari API Publik World Bank
sebagai proxy indikator pasokan pertanian riil.
"""

import requests
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

# Setup logging
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ingest_produksi")

# World Bank API for Crop Production Index (Indonesia)
# AG.PRD.CROP.XD is the indicator code for Crop production index (2014-2016 = 100)
WB_API_URL = "https://api.worldbank.org/v2/country/ID/indicator/AG.PRD.CROP.XD?format=json&per_page=50"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "temp_buffer" / "batch" / "bps_produksi"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def fetch_and_save():
    log.info(f"Fetching crop production data from World Bank: {WB_API_URL}")
    try:
        r = requests.get(WB_API_URL, timeout=15)
        r.raise_for_status()
        data = r.json()
        
        if len(data) < 2 or not isinstance(data[1], list):
            log.error("Unexpected response format from World Bank API")
            return False
            
        records = data[1]
        now = datetime.now(timezone.utc).isoformat()
        
        # Save as JSONL
        date_str = datetime.now().strftime("%Y-%m-%d")
        batch_ts = datetime.now().strftime("%H%M%S")
        filename = OUTPUT_DIR / f"crop_production_{date_str}_{batch_ts}.jsonl"
        
        with open(filename, "w", encoding="utf-8") as f:
            for rec in records:
                if rec["value"] is not None:
                    msg = {
                        "source": "world_bank",
                        "indicator": "crop_production_index",
                        "country": "ID",
                        "year": rec["date"],
                        "value": rec["value"],
                        "ingestion_ts": now,
                    }
                    f.write(json.dumps(msg, ensure_ascii=False) + "\n")
                    
        log.info(f"Berhasil menyimpan {len(records)} record ke {filename}")
        return True
        
    except Exception as e:
        log.error(f"Gagal mengambil data: {e}")
        return False

if __name__ == "__main__":
    success = fetch_and_save()
    sys.exit(0 if success else 1)
