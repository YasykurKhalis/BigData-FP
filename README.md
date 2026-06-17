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
