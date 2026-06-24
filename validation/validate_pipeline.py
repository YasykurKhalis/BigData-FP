import pandas as pd, json, pathlib, sys

BASE = pathlib.Path(__file__).resolve().parent.parent

# Paths
parquet_path = BASE / "temp_buffer" / "lakehouse" / "gold" / "feature_store" / "part-00000-66464c01-c06a-4055-b870-eb53a2e72bcf-c000.snappy.parquet"
json_path = BASE / "temp_buffer" / "export" / "feature_store.json"

print("=== Validation Report ===")
# Parquet
if parquet_path.exists():
    df = pd.read_parquet(parquet_path)
    print(f"Parquet rows: {len(df)}")
    print(df.head())
else:
    print("Parquet file not found:", parquet_path)

# JSON
if json_path.exists():
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        print(f"JSON items: {len(data)}")
        print(data[:3])
    else:
        print("JSON content is not a list, showing raw:")
        print(data)
else:
    print("JSON file not found:", json_path)

print("=== End of Report ===")
