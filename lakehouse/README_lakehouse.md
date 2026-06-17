# Lakehouse Medallion — LUMBUNG

Owner: **Yasykur Khalis** (5027241112)

## Bronze
Raw ingest dari streaming + batch, append-only, metadata lengkap.

## Silver
Cleaning, deduplication, schema enforcement, join lintas sumber.

## Gold
Feature store + tabel analitik:

- `price_daily_trend`
- `price_volatility_index`
- `weather_anomaly_sentra`
- `news_keyword_velocity`
- `supply_disruption_signal`
- `feature_store` ⭐
- `risk_index` ⭐
- `forecast_results` ⭐

## Export
`export_to_hdfs.py` menulis JSON ke `/data/lumbung/export/` untuk dibaca dashboard.
