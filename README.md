# LUMBUNG

**Lakehouse Untuk Monitoring & prediksi Bahan pangan UNGgulan**

Sistem _Early Warning_ lonjakan harga pangan strategis berbasis _real-time streaming_ dan _Data Lakehouse_. Project ini mengintegrasikan tiga lapisan sinyal — sisi harga/permintaan (streaming), sisi pasokan struktural (batch), dan sinyal eksternal (cuaca + NLP berita) — untuk menghasilkan prediksi harga 7 hari ke depan dan **Indeks Risiko Lonjakan Harga** per komoditas pangan strategis.

> Final Project — Mata Kuliah **Big Data dan Data Lakehouse**, Institut Teknologi Sepuluh Nopember (ITS) Surabaya, 2026.

---

## Tim — Kelompok 7

| No | Nama | NRP | Tanggung Jawab |
|----|------|-----|----------------|
| 1 | Ryan Adya Purwanto | 5027231046 | Streaming Ingestion + ML Forecasting |
| 2 | Yasykur Khalis | 5027241112 | Batch Ingestion + Lakehouse + Risk Engine |
| 3 | Hanif Mawla Faizi | 5027241064 | NLP + AI + Dashboard + Alert + Geospatial |

---

## Komoditas & Wilayah Sentra

**Komoditas yang dipantau:** Beras, Cabai Rawit Merah, Cabai Keriting, Bawang Merah, Bawang Putih.

**Sentra produksi:** Brebes (cabai/bawang), Karawang (beras), Magelang (bawang), Cianjur (beras), Probolinggo (bawang).

---

## Arsitektur Sistem

```
STREAMING SOURCES                    BATCH SOURCES
─────────────────                    ─────────────
PIHPS Bank Indonesia                 BPS Produksi & Luas Panen
Panel Harga Bapanas                  BPS Impor-Ekspor Pangan
SISKAPERBAPO Jatim                   Stok Bulog
Open-Meteo API (cuaca)               Harga Pupuk PIHC
RSS Kompas/Tempo/Antara
Kurs JISDOR Bank Indonesia
        │                                   │
        ▼                                   ▼
  Apache Kafka                      batch_ingest/*.py
  (6 topics)                        (scheduler cron)
        │                                   │
        └──────────────┬────────────────────┘
                       ▼
                 Hadoop HDFS
              /data/lumbung/...
                       │
        ┌──────────────┴──────────────┐
        ▼                             ▼
  BRONZE LAYER                  BATCH TABLES
  (raw ingest + dedup)
        │
        ▼
  SILVER LAYER  (clean, dedup, schema, join)
        │
        ▼
  GOLD LAYER   (feature store + analitik)
  ├── feature_store        ← gabungan price + weather + news + kurs
  ├── risk_index           ← indeks risiko 0-100
  └── forecast_results     ← prediksi 7 hari
        │
        ▼
  export_gold.py → temp_buffer/export/*.json
        │
        ▼
  Flask Dashboard   + ML Models + Alert Engine + Gemini API
```

### Medallion Architecture (Delta Lake)

| Layer | Fungsi | Output |
|-------|--------|--------|
| **Bronze** | Raw ingest dari HDFS/streaming + snapshot, dedup by business key | Delta tables per topic |
| **Silver** | Clean, dedup, normalisasi schema (`komoditas`, `date_parsed`, `avg_price`) | `silver_prices`, `silver_weather`, `silver_news`, `silver_kurs` |
| **Gold** | Agregasi harian + join semua sumber → unified feature store | `feature_store` (9,130 rows) |

---

## Tech Stack

| Layer | Teknologi |
|-------|-----------|
| Ingestion Streaming | Apache Kafka (`kafka-python`, `confluent-kafka`) |
| Ingestion Batch | Python `requests` + `BeautifulSoup` + `feedparser` |
| Storage | Hadoop HDFS (WebHDFS via `hdfs` library) |
| Lakehouse | Delta Lake (`deltalake` + pandas) — Medallion Bronze/Silver/Gold |
| ML Forecasting | XGBoost, scikit-learn (time-series cross-validation) |
| NLP | Regex keyword extraction + keyword velocity analysis |
| AI Generatif | Gemini API (`google-genai`) — opsional, fallback ke rule-based |
| Geospatial | Leaflet.js + `sentra_mapper.py` |
| Dashboard | Flask + Chart.js + Jinja2 |
| Container | Docker + Docker Compose |
| Runtime | Windows 11 + WSL2, Java 17, Python 3.11.9 |

---

## Struktur Direktori

```
FP Big Data/
├── orchestrate.py              # Pipeline orchestrator (entry point utama)
├── requirements.txt            # Python dependencies
├── setup_kafka_topics.py       # Kafka topic initialization
├── run_producers.sh            # Shell script untuk jalankan semua producer
│
├── scripts/
│   └── generate_snapshot.py    # Synthetic Big Data snapshot generator (~56K records)
│
├── kafka/                      # Streaming producers (6 sumber data)
│   ├── producer_price_pihps.py       ← PIHPS Bank Indonesia
│   ├── producer_price_bapanas.py     ← Panel Harga Bapanas
│   ├── producer_price_siskaperbapo.py ← SISKAPERBAPO Jatim
│   ├── producer_weather.py           ← Open-Meteo API
│   ├── producer_news_pangan.py       ← RSS Kompas/Tempo/Antara
│   ├── producer_kurs_bi.py           ← Kurs JISDOR BI
│   └── utils.py                      ← Kafka utility functions
│
├── hdfs/                       # HDFS integration
│   ├── consumer_to_hdfs.py     ← Kafka consumer → HDFS ingest
│   ├── push_to_hdfs.py         ← Push local files to HDFS
│   ├── test_hdfs_connection.py ← HDFS connectivity test
│   └── _dns_patch.py           ← DNS patch (Windows → Docker hostname)
│
├── lakehouse/                  # Medallion pipeline (Delta Lake)
│   ├── 01_bronze.py            ← Raw ingest + dedup
│   ├── 02_silver.py            ← Clean + normalize
│   ├── 03_gold.py              ← Feature store aggregation
│   ├── export_gold.py          ← Export Gold → JSON untuk dashboard/ML
│   ├── utils.py                ← Delta Lake read/write utilities
│   ├── seed_historical_prices.py ← Seed historical price data
│   └── time_travel_demo.py     ← Delta Lake time travel demo
│
├── ml/                         # Machine Learning pipeline
│   ├── nlp_keyword_extractor.py      ← NLP dari berita pangan
│   ├── feature_importance.py         ← XGBoost training + feature importance
│   ├── compute_risk_index.py         ← Risk Index 0-100 (price+weather+news)
│   ├── evaluation.py                 ← Backtesting MAPE/RMSE/DA
│   ├── recommendation_llm.py         ← Gemini API / rule-based recommendations
│   ├── train_price_forecast.py       ← Price forecasting model
│   ├── train_magnitude_model.py      ← Spike magnitude prediction
│   ├── train_timing_classifier.py    ← Spike timing classification
│   ├── sentra_mapper.py              ← Geospatial sentra production mapping
│   ├── models/                       ← Trained model artifacts (.joblib)
│   └── notebooks/                    ← EDA & experiment notebooks
│
├── dashboard/                  # Flask web dashboard
│   ├── app.py                  ← Flask application (7 routes + 8 API endpoints)
│   ├── templates/              ← Jinja2 HTML templates
│   │   ├── base.html           ← Base layout
│   │   ├── index.html          ← Dashboard utama (ringkasan + alert)
│   │   ├── komoditas.html      ← Detail per komoditas
│   │   ├── peta_sentra.html    ← Peta sentra produksi (Leaflet)
│   │   ├── rekomendasi.html    ← Rekomendasi tindakan
│   │   ├── evaluasi.html       ← Evaluasi model
│   │   └── lakehouse.html      ← Delta Lake info
│   └── static/                 ← CSS + JS (Chart.js, Leaflet)
│
├── alerts/                     # Early warning system
│   ├── alert_engine.py         ← Alert generation engine
│   └── alert_config.yml        ← Alert thresholds & config
│
├── temp_buffer/                ← Runtime output (auto-generated)
│   ├── streaming/              ← Snapshot JSONL files (date-partitioned)
│   ├── lakehouse/              ← Local Delta tables (bronze/silver/gold)
│   └── export/                 ← JSON exports untuk dashboard & ML
│       ├── feature_store.json
│       ├── risk_index.json
│       ├── price_forecast.json
│       ├── feature_importance.json
│       ├── evaluation.json
│       ├── recommendations.json
│       ├── nlp_signals.json
│       └── alerts.json
│
├── tests/                      # Unit & integration tests
├── validation/                 # Pipeline validation scripts
├── dags/                       # Airflow DAG definitions
├── docs/                       # Roadmap documentation
│
├── docker-compose-hadoop.yml   # Hadoop cluster (NameNode + DataNode)
├── docker-compose-kafka.yml    # Kafka cluster (broker + Zookeeper)
├── docker-compose-airflow.yml  # Airflow scheduler
└── hadoop.env                  # Hadoop environment variables
```

---

## Cara Menjalankan

### Prasyarat

- **Python 3.11.9** (wajib)
- **Java 17** (untuk Hadoop/Spark, opsional jika hanya mode offline)
- **Docker Desktop** (untuk HDFS + Kafka, opsional jika hanya mode offline)

### 1. Install Dependencies

```bash
# Buat virtual environment (recommended)
python -m venv venv
venv\Scripts\activate

# Install semua dependency
pip install -r requirements.txt
```

### 2. Mode Offline (Tanpa Docker/Kafka) — Recommended untuk Development

Mode ini menggunakan synthetic Big Data snapshot (~56,000 records) sehingga tidak memerlukan API key, Kafka, atau HDFS.

```bash
# Generate snapshot + jalankan seluruh pipeline (lakehouse → ML → alerts)
python orchestrate.py --skip-kafka --skip-ingest --generate-snapshot
```

**Output yang dihasilkan:**
- `temp_buffer/export/feature_store.json` — 9,130 baris fitur terpadu
- `temp_buffer/export/risk_index.json` — indeks risiko per komoditas
- `temp_buffer/export/price_forecast.json` — prediksi harga 7 hari
- `temp_buffer/export/feature_importance.json` — feature importance XGBoost
- `temp_buffer/export/evaluation.json` — metrik evaluasi (MAPE, RMSE, DA)
- `temp_buffer/export/recommendations.json` — rekomendasi tindakan
- `temp_buffer/export/nlp_signals.json` — sinyal NLP dari berita
- `temp_buffer/export/alerts.json` — alert aktif

### 3. Mode Only Lakehouse (Tanpa ML)

```bash
# Hanya jalankan Bronze → Silver → Gold
python orchestrate.py --skip-kafka --skip-ingest --generate-snapshot --only-lake
```

### 4. Mode Only ML (Tanpa Lakehouse)

```bash
# Hanya jalankan ML pipeline + alerts (butuh feature_store.json dari run sebelumnya)
python orchestrate.py --only-ml
```

### 5. Mode Full Streaming (Dengan Docker)

```bash
# Step 1: Spin up Hadoop
docker compose -f docker-compose-hadoop.yml up -d

# Step 2: Spin up Kafka
docker compose -f docker-compose-kafka.yml up -d

# Step 3: Setup Kafka topics
python setup_kafka_topics.py

# Step 4: Jalankan full pipeline
python orchestrate.py
```

### 6. Jalankan Dashboard

```bash
# Setelah pipeline selesai, jalankan dashboard
python dashboard/app.py

# Akses di browser: http://localhost:5000
```

Dashboard menyediakan:
- **`/`** — Halaman utama: ringkasan risiko + alert terbaru
- **`/komoditas`** — Detail per komoditas: harga, forecast, risk index
- **`/peta_sentra`** — Peta sentra produksi dengan risk level
- **`/rekomendasi`** — Rekomendasi untuk pemerintah & UMKM
- **`/evaluasi`** — Metrik evaluasi model (MAPE, RMSE, DA)
- **`/lakehouse`** — Info Delta Lake & sample data
- **`/api/*`** — JSON API untuk semua data (risk_index, alerts, forecast, dll)

### 7. Orchestrator Flags

| Flag | Fungsi |
|------|--------|
| `--skip-kafka` | Skip Kafka producers + consumer |
| `--skip-ingest` | Skip batch ingest (BPS, Bulog, Pupuk) |
| `--generate-snapshot` | Generate synthetic Big Data snapshot sebelum pipeline |
| `--only-lake` | Hanya jalankan Lakehouse (Bronze → Silver → Gold → Export) |
| `--only-ml` | Hanya jalankan ML + Alerts |
| `--only-dash` | Hanya tampilkan info dashboard |

---

## Kafka Topics

| Topic | Sumber | Isi |
|-------|--------|-----|
| `price-pihps` | PIHPS Bank Indonesia | Harga pangan pasar induk |
| `price-bapanas` | Panel Harga Bapanas | Harga pangan daerah |
| `price-siskaperbapo` | SISKAPERBAPO Jatim | Harga pasar Jawa Timur |
| `weather-sentra` | Open-Meteo API | Curah hujan, suhu, cuaca sentra |
| `news-pangan` | RSS Kompas/Tempo/Antara | Berita pangan nasional |
| `kurs-bi` | JISDOR Bank Indonesia | Kurs USD/IDR |

---

## HDFS Path Structure

```
/data/lumbung/
├── streaming/{prices|weather|news|kurs}/YYYY-MM-DD/
├── batch/{bps_produksi|bps_imporekspor|bulog_stok|pupuk_harga}/...
├── lakehouse/{bronze|silver|gold}/
├── export/*.json
└── models/*.joblib
```

> **Catatan:** Implementasi saat ini menggunakan hybrid storage — Delta Lake ditulis ke lokal (`temp_buffer/`) lalu di-push ke HDFS. Dashboard membaca JSON dari `temp_buffer/export/` untuk performa optimal.

---

## Output Sistem

- **Prediksi harga 7 hari** per komoditas (XGBoost, MAPE 2.7–9.1%)
- **Indeks Risiko Lonjakan Harga** (0–100): AMAN / WASPADA / SIAGA / KRITIS
- **Feature importance**: faktor dominan (price lag, weather, news velocity)
- **Rekomendasi tindakan** untuk pemerintah (operasi pasar) dan UMKM (stok/HPP)
- **Peta sentra produksi** terancam (heatmap Leaflet)
- **Alert early warning** berdasarkan threshold risiko
- **Evaluasi model**: MAPE, RMSE, Directional Accuracy, Lead Time

---

## Evaluasi Model (Latest Run on Snapshot)

| Komoditas | MAPE Baseline | MAPE XGBoost | Directional Accuracy |
|-----------|---------------|--------------|----------------------|
| Beras | 1.97% | 2.66% | 49.2% |
| Cabai Rawit Merah | 7.60% | 9.09% | 50.6% |
| Cabai Keriting | 5.93% | 7.14% | 54.2% |
| Bawang Merah | 4.72% | 5.55% | 52.1% |
| Bawang Putih | 4.69% | 6.21% | 50.4% |
| **Rata-rata** | **4.98%** | — | **~51%** |

---

## Data Sources & Producers

Setiap producer menggunakan pola **real-data-first / fallback-last**:

1. Coba ambil dari API/file data real
2. Jika gagal, generate fallback data (random walk) agar pipeline tetap berjalan

| Producer | Sumber Real | Fallback |
|----------|-------------|----------|
| `producer_price_pihps.py` | PIHPS BI API + `data/pihps_realdata.json` | Random walk |
| `producer_price_bapanas.py` | Bapanas scraping | Random walk |
| `producer_price_siskaperbapo.py` | SISKAPERBAPO API | Random walk |
| `producer_weather.py` | Open-Meteo API (gratis, no key) | Fallback JSONL |
| `producer_news_pangan.py` | RSS feeds (Kompas/Tempo/Antara) | Fallback JSONL |
| `producer_kurs_bi.py` | JISDOR BI + exchangerate-api.com | Random walk |

---

## Referensi

Project ini merupakan evolusi dari **ETS NewsPulse** (pipeline Kafka → HDFS → Spark → Flask).
Repo NewsPulse: <https://github.com/YasykurKhalis/BigData-NewsPulse>

---

## Lisensi

Untuk keperluan akademik — Final Project Big Data ITS 2026.
# LUMBUNG

**Lakehouse Untuk Monitoring & prediksi Bahan pangan UNGgulan**

Sistem _Early Warning_ lonjakan harga pangan strategis berbasis _real-time streaming_ dan _Data Lakehouse_. Project ini mengintegrasikan tiga lapisan sinyal — sisi harga/permintaan (streaming), sisi pasokan struktural (batch), dan sinyal eksternal (cuaca + NLP berita) — untuk menghasilkan prediksi harga dan **Indeks Risiko Lonjakan Harga** per komoditas pangan strategis.

> Final Project — Mata Kuliah **Big Data dan Data Lakehouse**, Institut Teknologi Sepuluh Nopember (ITS) Surabaya, 2026.

---

## Tim — Kelompok 7

| No | Nama | NRP | Tanggung Jawab |
|----|------|-----|----------------|
| 1 | Ryan Adya Purwanto | 5027231046 | Streaming Ingestion + ML Forecasting |
| 2 | Yasykur Khalis | 5027241112 | Batch Ingestion + Lakehouse + Risk Engine |
| 3 | Hanif Mawla Faizi | 5027241064 | NLP + AI + Dashboard + Alert + Geospatial |

---

## Komoditas & Wilayah Sentra

**Komoditas yang dipantau:** Beras, Cabai Rawit Merah, Cabai Keriting, Bawang Merah, Bawang Putih.

**Sentra produksi:** Brebes (cabai/bawang), Karawang (beras), Magelang (bawang), Cianjur (beras), Probolinggo (bawang).

---

## Arsitektur

```
STREAMING SOURCES                    BATCH SOURCES
─────────────────                    ─────────────
PIHPS Bank Indonesia                 BPS Produksi & Luas Panen
Panel Harga Bapanas                  BPS Impor-Ekspor Pangan
SISKAPERBAPO Jatim                   Stok Bulog
Open-Meteo API (cuaca)               Harga Pupuk PIHC
RSS Kompas/Tempo/Antara
Kurs JISDOR Bank Indonesia
        │                                   │
        ▼                                   ▼
  Apache Kafka                      batch_ingest/*.py
  (6 topics)                        (scheduler cron)
        │                                   │
        └──────────────┬────────────────────┘
                       ▼
                 Hadoop HDFS
              /data/lumbung/...
                       │
        ┌──────────────┴──────────────┐
        ▼                             ▼
  BRONZE LAYER                  BATCH TABLES
        │
        ▼
  SILVER LAYER  (clean, dedup, schema, join)
        │
        ▼
  GOLD LAYER   (feature store + analitik)
  ├── price_daily_trend
  ├── price_volatility_index
  ├── weather_anomaly_sentra
  ├── news_keyword_velocity
  ├── supply_disruption_signal
  ├── feature_store        ⭐
  ├── risk_index           ⭐
  └── forecast_results     ⭐
        │
        ▼
  export_to_hdfs.py → /data/lumbung/export/*.json
        │
        ▼
  Flask Dashboard (pyarrow → HDFS)
  + ML Models (HDFS)
  + Alert Engine
  + Gemini API (rekomendasi naratif)
```

---

## Tech Stack

| Layer | Teknologi |
|-------|-----------|
| Ingestion Streaming | Apache Kafka |
| Ingestion Batch | Python `requests` + `BeautifulSoup` + scheduler |
| Storage | Hadoop HDFS |
| Lakehouse | Delta Lake (PySpark) — Medallion Bronze/Silver/Gold |
| ML | XGBoost, scikit-learn |
| NLP | Regex keyword extraction + keyword velocity |
| AI Generatif | Gemini API (`google-genai`) |
| Geospatial | Leaflet.js + `pyarrow` |
| Dashboard | Flask + Chart.js |
| Container | Docker + Docker Compose |
| Runtime | Windows 11 + WSL2, Java 17, Python 3.11.9 |

---

## Kafka Topics

1. `price-pihps`
2. `price-bapanas`
3. `price-siskaperbapo`
4. `weather-sentra`
5. `news-pangan`
6. `kurs-bi`

---

## HDFS Path Structure

```
/data/lumbung/
├── streaming/{prices|weather|news|kurs}/YYYY-MM-DD/
├── batch/{bps_produksi|bps_imporekspor|bulog_stok|pupuk_harga}/...
├── lakehouse/{bronze|silver|gold}/
├── export/*.json
└── models/*.pkl
```

> **Constraint:** seluruh data (JSON export, model `.pkl`) disimpan di HDFS — tidak ada penyimpanan lokal. Dashboard membaca via `pyarrow.fs.HadoopFileSystem`.

---

## Quick Start

```bash
# 1. Spin up infra
docker compose -f docker-compose-hadoop.yml up -d
docker compose -f docker-compose-kafka.yml up -d

# 2. Setup Kafka topics
python setup_kafka_topics.py

# 3. Jalankan pipeline ingestion
python kafka/producer_weather.py &
python hdfs/consumer_to_hdfs.py &

# 4. Jalankan dashboard
python dashboard/app.py
```

---

## Folder Structure

```
lumbung-final-project/
├── kafka/              # Streaming producers (Ryan)
├── hdfs/               # HDFS consumer (Ryan)
├── batch_ingest/       # Batch ingestion (Yasykur)
├── lakehouse/          # Medallion Bronze/Silver/Gold (Yasykur)
├── ml/                 # Training + inference + notebooks
├── alerts/             # Early warning engine (Hanif)
├── dashboard/          # Flask serving layer (Hanif)
├── tests/              # Unit & integration tests
└── docs/               # Dokumentasi tambahan
```

---

## Output Sistem

- Prediksi harga 7–30 hari per komoditas
- Indeks Risiko Lonjakan Harga (0–100)
- Estimasi besaran kenaikan (Rp/kg dan %)
- Estimasi waktu lonjakan (window tanggal)
- Feature importance: faktor dominan
- Peta sentra produksi terancam (heatmap Leaflet)
- Rekomendasi tindakan (Gemini API)

---

## Evaluasi Model

| Metrik | Target |
|--------|--------|
| MAPE (beras) | < 10% |
| MAPE (cabai) | < 20% |
| RMSE | Rp/kg |
| Directional Accuracy | arah naik/turun benar |
| Precision/Recall alert | lonjakan terdeteksi |
| Lead Time | hari sebelum lonjakan |

Backtesting: train 2020–2024, validasi 2025, test Juni 2026.

---

## Referensi

Project ini merupakan evolusi dari **ETS NewsPulse** (pipeline Kafka → HDFS → Spark → Flask).
Repo NewsPulse: <https://github.com/YasykurKhalis/BigData-NewsPulse>

---

## Lisensi

Untuk keperluan akademik — Final Project Big Data ITS 2026.
