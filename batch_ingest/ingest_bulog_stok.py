"""
LUMBUNG — Batch ingest Stok Bulog
Owner: Yasykur

Mengambil data Cereal Yield (kg per hectare) Indonesia dari API Publik World Bank
sebagai proxy indikator ketersediaan stok pangan domestik riil.
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
log = logging.getLogger("ingest_stok_bulog")

# AG.YLD.CREL.KG = Cereal yield (kg per hectare)
WB_API_URL = "https://api.worldbank.org/v2/country/ID/indicator/AG.YLD.CREL.KG?format=json&per_page=50"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "temp_buffer" / "batch" / "bulog_stok"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def fetch_and_save():
    log.info(f"Fetching cereal yield data from World Bank: {WB_API_URL}")
    try:
        r = requests.get(WB_API_URL, timeout=15)
        r.raise_for_status()
        data = r.json()
        
        if len(data) < 2 or not isinstance(data[1], list):
            log.error("Unexpected response format from World Bank API")
            return False
            
        records = data[1]
        now = datetime.now(timezone.utc).isoformat()
        
        date_str = datetime.now().strftime("%Y-%m-%d")
        batch_ts = datetime.now().strftime("%H%M%S")
        filename = OUTPUT_DIR / f"cereal_yield_{date_str}_{batch_ts}.jsonl"
        
        with open(filename, "w", encoding="utf-8") as f:
            for rec in records:
                if rec["value"] is not None:
                    msg = {
                        "source": "world_bank",
                        "indicator": "cereal_yield_kg_per_ha",
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
