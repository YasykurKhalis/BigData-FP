"""
LUMBUNG — Lakehouse Utilities
Owner: Yasykur (patched Ryan)

Helper untuk Delta Lake operations pakai deltalake + pandas.
Tidak membutuhkan PySpark/JVM/winutils — pure Python + Rust.
"""

import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from deltalake import DeltaTable, write_deltalake
import logging


BASE_DIR = Path(__file__).resolve().parent.parent
LAKEHOUSE_DIR = BASE_DIR / "temp_buffer" / "lakehouse"
BRONZE_DIR = LAKEHOUSE_DIR / "bronze"
SILVER_DIR = LAKEHOUSE_DIR / "silver"
GOLD_DIR   = LAKEHOUSE_DIR / "gold"

# HDFS configuration used across pipeline
WEBHDFS_URL = os.getenv("WEBHDFS_URL", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")
log = logging.getLogger("utils")


def read_jsonl_files(directory: Path, recursive: bool = True) -> pd.DataFrame:
    """Baca semua .jsonl file dari directory ke pandas DataFrame."""
    pattern = "**/*.jsonl" if recursive else "*.jsonl"
    files = sorted(directory.glob(pattern))

    if not files:
        return pd.DataFrame()

    records = []
    for f in files:
        with open(f, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

    if not records:
        return pd.DataFrame()

    return pd.DataFrame(records)


def write_delta(df: pd.DataFrame, table_path: str, mode: str = "append"):
    """Tulis pandas DataFrame ke Delta Lake table."""
    Path(table_path).mkdir(parents=True, exist_ok=True)

    if df.empty:
        return

    # Convert semua kolom object ke string untuk kompatibilitas Arrow
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str)

    if Path(table_path, "_delta_log").exists() and mode == "append":
        write_deltalake(table_path, df, mode="append", schema_mode="merge")
    else:
        # overwrite dengan schema_mode overwrite agar bisa menangani
        # perubahan jumlah kolom antar run
        write_deltalake(table_path, df, mode="overwrite", schema_mode="overwrite")


def read_delta(table_path: str) -> pd.DataFrame:
    """Baca Delta Lake table ke pandas DataFrame."""
    if not Path(table_path, "_delta_log").exists():
        return pd.DataFrame()

    dt = DeltaTable(table_path)
    df = dt.to_pandas()

    # Convert Arrow-backed string columns ke object dtype
    # supaya pd.to_datetime dan operasi lain bisa handle dengan benar
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]) and df[col].dtype != object:
            df[col] = df[col].astype(object)

    return df




def get_hdfs_client():
    """Return an InsecureClient or None (fallback)."""
    try:
        from hdfs import InsecureClient
        client = InsecureClient(WEBHDFS_URL, user=HDFS_USER)
        client.status("/")  # probe connection
        log.info(f"HDFS client ready: {WEBHDFS_URL}")
        return client
    except Exception as e:
        log.warning(f"HDFS client unavailable: {e}")
        return None


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()
