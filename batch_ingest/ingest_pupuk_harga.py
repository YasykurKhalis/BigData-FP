"""
LUMBUNG — Batch ingest Harga Pupuk PIHC
Owner: Yasykur

Menggunakan public API (yfinance) untuk mengambil harga saham/komoditas
perusahaan pupuk global (Nutrien Ltd - NTR) sebagai proxy indeks harga pupuk.
"""

from __future__ import annotations
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import yfinance as yf

# Setup logging
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ingest_pupuk_harga")

OUTPUT_DIR = Path(__file__).resolve().parent.parent / "temp_buffer" / "batch" / "pupuk_harga"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def fetch_and_save():
    ticker_symbol = "NTR"
    log.info(f"Fetching fertilizer proxy data from yfinance: {ticker_symbol}")
    try:
        ticker = yf.Ticker(ticker_symbol)
        data = ticker.history(period="1mo")
        if data.empty:
            log.error(f"No data returned for {ticker_symbol}")
            return False
            
        now = datetime.now(timezone.utc).isoformat()
        date_str = datetime.now().strftime("%Y-%m-%d")
        batch_ts = datetime.now().strftime("%H%M%S")
        filename = OUTPUT_DIR / f"fertilizer_proxy_{date_str}_{batch_ts}.jsonl"
        
        with open(filename, "w", encoding="utf-8") as f:
            for index, row in data.iterrows():
                msg = {
                    "source": "yfinance_proxy",
                    "indicator": "fertilizer_price_index",
                    "ticker": ticker_symbol,
                    "date": str(index.date()),
                    "close_price": float(row["Close"]),
                    "ingestion_ts": now,
                }
                f.write(json.dumps(msg, ensure_ascii=False) + "\n")
                    
        log.info(f"Berhasil menyimpan {len(data)} record ke {filename}")
        return True
        
    except Exception as e:
        log.error(f"Gagal mengambil data: {e}")
        return False

if __name__ == "__main__":
    success = fetch_and_save()
    sys.exit(0 if success else 1)
