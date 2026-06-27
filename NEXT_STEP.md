# NEXT_STEP.md — Status & Roadmap

## Ringkasan Progress

### Yang Sudah Selesai

#### 1. Workspace Cleanup & Stabilization
- [x] Removed stale `BigData-FP/` git submodule (duplicate directory)
- [x] Fixed `lakehouse/utils.py`: enabled Delta Lake `schema_mode="overwrite"` for schema evolution
- [x] Rewrote `lakehouse/export_gold.py`: replaced obsolete PySpark with `deltalake` + pandas
- [x] Fixed `lakehouse/01_bronze.py`: HDFS `upload()` now passes file path instead of file object
- [x] Fixed `lakehouse/02_silver.py`: added missing imports, DNS patch, and `write_delta()` for silver_prices
- [x] Fixed column-name mismatch `commodity` → `komoditas` in ML modules

#### 2. Synthetic Big Data Snapshot
- [x] Created [`scripts/generate_snapshot.py`](scripts/generate_snapshot.py) — generates ~56,000 records spanning 5 years
- [x] Integrated into orchestrator via `--generate-snapshot` flag
- [x] Big Data characteristics: Volume (56K records), Variety (prices/weather/news/kurs), Velocity (date-partitioned JSONL), Veracity (seasonality, volatility, shocks)
- [x] Bronze layer merges local snapshot with HDFS data and deduplicates by business keys

#### 3. Lakehouse Medallion Pipeline (Bronze → Silver → Gold)
- [x] **Bronze** (`01_bronze.py`): reads HDFS streaming data + local snapshot, deduplicates
- [x] **Silver** (`02_silver.py`): cleans, deduplicates, normalizes schema, writes Delta tables
- [x] **Gold** (`03_gold.py`): aggregates prices + weather + news + kurs into unified `feature_store` (9,130 rows)
- [x] **Export** (`export_gold.py`): exports Gold tables to JSON for dashboard and ML

#### 4. ML Pipeline
- [x] **NLP Keyword Extractor** (`nlp_keyword_extractor.py`): extracts risk signals from 17,698 news articles
- [x] **Feature Importance** (`feature_importance.py`): XGBoost training with time-series cross-validation
  - MAPE: beras ~2.7%, cabai rawit merah ~9.1%, cabai keriting ~7.1%, bawang merah ~5.5%, bawang putih ~6.2%
- [x] **Risk Index** (`compute_risk_index.py`): fuses price (50%), weather (25%), news (25%) signals → 0–100 index
- [x] **Evaluation** (`evaluation.py`): backtesting with baseline MAPE ~4.98% average
- [x] **Recommendation** (`recommendation_llm.py`): rule-based fallback (ASCII-safe) when no Gemini API key
- [x] **Price Forecast** (`train_price_forecast.py`): 7-day ahead prediction per commodity

#### 5. Real Data Producers (by Yasykur)
- [x] `producer_price_pihps.py`: reads `data/pihps_realdata.json`, tries PIHPS BI API, falls back to random walk
- [x] `producer_price_bapanas.py`: tries Bapanas scraping, falls back to random walk
- [x] `producer_price_siskaperbapo.py`: tries SISKAPERBAPO API, falls back to random walk
- [x] `producer_weather.py`: uses Open-Meteo API (real data)
- [x] `producer_news_pangan.py`: uses real RSS feeds (Kompas, Tempo, Antara)
- [x] `producer_kurs_bi.py`: tries JISDOR BI + exchangerate-api.com, falls back to random walk

#### 6. Dashboard & Alerts
- [x] Flask dashboard (`dashboard/app.py`): 7 HTML routes + 8 JSON API endpoints
- [x] Reads all 8 export JSON files from `temp_buffer/export/`
- [x] Alert engine (`alerts/alert_engine.py`): monitors risk indices and generates alerts

#### 7. Orchestration
- [x] `orchestrate.py` supports multiple modes: full pipeline, `--skip-kafka`, `--skip-ingest`, `--generate-snapshot`, `--only-lake`, `--only-ml`, `--only-dash`
- [x] End-to-end pipeline completes with exit code 0 in offline mode

---

## Storage Strategy: HDFS + Local `temp_buffer` Cache

The README states a strict **HDFS-only** constraint. In practice the current implementation uses a pragmatic hybrid:

- **Authoritative raw & processed data → HDFS**
  - Streaming JSONL batches: `/data/lumbung/streaming/...`
  - Bronze & Silver Delta tables: pushed to `/data/lumbung/lakehouse/...`
- **Working cache & dashboard inputs → local `temp_buffer/`**
  - Local Bronze/Silver/Gold Delta tables for fast `deltalake` + pandas reads/writes
  - JSON exports in `temp_buffer/export/` for the Flask dashboard and downstream ML modules

**Rationale:** `deltalake` writes to the local filesystem first; pushing finished tables to HDFS keeps the lakehouse durable while avoiding PySpark/JVM complexity. JSON exports stay local for low-latency dashboard reads.

---

## Data Flow Strategy

The pipeline uses a unified data flow:

```
Data Source (snapshot or real API)
    → Bronze (raw ingest + dedup)
    → Silver (clean + normalize schema)
    → Gold (feature_store: unified features)
    → ML (train + predict in same run)
    → JSON exports
    → Dashboard / Alerts
```

- **Offline mode**: synthetic snapshot → entire pipeline trains and predicts on snapshot data
- **Production mode**: real Kafka streams → accumulated history trains models, new data predicts
- **Hybrid**: snapshot bootstraps the pipeline; as real data accumulates, it replaces snapshot data

---

## Yang Belum Selesai (Next Steps)

### Priority 1 — Dashboard Validation
- [ ] Launch dashboard (`python dashboard/app.py`) and verify all pages render correctly
- [ ] Verify chart.js visualizations display price forecasts and risk indices
- [ ] Test peta_sentra Leaflet map with sentra_mapper data

### Priority 2 — Real Data Integration
- [ ] Obtain `data/pihps_realdata.json` with actual PIHPS price data
- [ ] Validate real-data producers end-to-end: `python kafka/producer_price_pihps.py --once`
- [ ] Wire real producer output through the lakehouse pipeline
- [ ] Replace synthetic snapshot with accumulated real data for training

### Priority 3 — LLM Recommendation
- [ ] Obtain Gemini API key and set `GEMINI_API_KEY` environment variable
- [ ] Test `ml/recommendation_llm.py` with real Gemini API calls
- [ ] Verify generated recommendations are contextually relevant

### Priority 4 — Kafka + HDFS Full Streaming
- [ ] Spin up Docker containers: `docker compose -f docker-compose-hadoop.yml up -d`
- [ ] Spin up Kafka: `docker compose -f docker-compose-kafka.yml up -d`
- [ ] Run `python setup_kafka_topics.py` to create topics
- [ ] Validate `hdfs/consumer_to_hdfs.py` ingests streaming data into Bronze

### Priority 5 — Testing & Validation
- [ ] Run `pytest tests/` and fix any failures
- [ ] Run `python validation/validate_pipeline.py` for schema validation
- [ ] End-to-end validation with real data (not just snapshot)

### Priority 6 — Advanced ML Models
- [ ] Train `train_magnitude_model.py` (spike magnitude prediction)
- [ ] Train `train_timing_classifier.py` (spike timing classification)
- [ ] Tune XGBoost hyperparameters with real data
- [ ] Refine feature importance weights in risk index

### Priority 7 — Production Hardening
- [ ] Finalize Airflow DAG (`dags/lumbung_batch_dag.py`) for scheduled runs
- [ ] Set up alert delivery (email/Telegram) from `alerts/alert_engine.py`
- [ ] HDFS replication and fault tolerance validation
- [ ] Performance benchmarking with larger datasets

---

## Verified Pipeline Output (Latest Run)

```
Command: python orchestrate.py --skip-kafka --skip-ingest --generate-snapshot

Results:
  Snapshot:       56,466 records generated
  Gold:           9,130 rows in feature_store (5 komoditas x 1,826 days)
  NLP:            17,698 articles processed
  MAPE (avg):     4.98% baseline, 2.7-9.1% per commodity (XGBoost)
  Risk Index:     beras=25.1 [AMAN], cabai_rawit=57.8 [WASPADA]
  Alerts:         0 active (no threshold breach)
  Recommendations: rule-based fallback (no Gemini API key)
  Exit code:      0 (success)
```
