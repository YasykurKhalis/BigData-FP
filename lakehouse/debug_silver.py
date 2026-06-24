"""Debug: kenapa 900 seed dates jadi NaT?"""
import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils import read_delta, BRONZE_DIR

df = read_delta(str(BRONZE_DIR / "price_bapanas"))
print(f"Total rows: {len(df)}")
print(f"fetched_at_utc dtype: {df['fetched_at_utc'].dtype}")
print(f"type of first value: {type(df['fetched_at_utc'].iloc[0])}")
print(f"type of last value: {type(df['fetched_at_utc'].iloc[-1])}")
print()

# Test parsing individual values
producer_val = df["fetched_at_utc"].iloc[0]  # producer record
seed_val = df["fetched_at_utc"].iloc[-1]     # seed record

print(f"Producer value: {repr(producer_val)}")
print(f"Seed value:     {repr(seed_val)}")
print()

print(f"pd.to_datetime(producer): {pd.to_datetime(producer_val, errors='coerce')}")
print(f"pd.to_datetime(seed):     {pd.to_datetime(seed_val, errors='coerce')}")
print()

# Test on whole column
parsed = pd.to_datetime(df["fetched_at_utc"], errors="coerce")
print(f"NaT count: {parsed.isna().sum()} / {len(df)}")

# Check if it's a format issue - try with explicit format
parsed2 = pd.to_datetime(df["fetched_at_utc"], format="ISO8601", errors="coerce")
print(f"With ISO8601 format - NaT count: {parsed2.isna().sum()} / {len(df)}")

# Try utc=True
parsed3 = pd.to_datetime(df["fetched_at_utc"], utc=True, errors="coerce")
print(f"With utc=True - NaT count: {parsed3.isna().sum()} / {len(df)}")
