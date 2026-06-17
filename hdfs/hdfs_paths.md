# HDFS Path Structure — LUMBUNG

Root: `/data/lumbung/`

## Streaming (Kafka -> HDFS)

```
/data/lumbung/streaming/
├── prices/pihps/YYYY-MM-DD/
├── prices/bapanas/YYYY-MM-DD/
├── prices/siskaperbapo/YYYY-MM-DD/
├── weather/YYYY-MM-DD/
├── news/YYYY-MM-DD/
└── kurs/YYYY-MM-DD/
```

## Batch

```
/data/lumbung/batch/
├── bps_produksi/YYYY-MM/
├── bps_imporekspor/YYYY-MM/
├── bulog_stok/YYYY-WW/
└── pupuk_harga/YYYY-MM/
```

## Lakehouse Medallion

```
/data/lumbung/lakehouse/
├── bronze/
├── silver/
└── gold/
```

## Export (dibaca dashboard via pyarrow)

```
/data/lumbung/export/
├── prices_latest.json
├── forecast.json
├── risk_index.json
├── feature_importance.json
├── sentra_map.json
├── recommendations.json
├── alerts.json
└── evaluation_metrics.json
```

## ML Models

```
/data/lumbung/models/
├── beras_forecast.pkl
├── cabai_rawit_forecast.pkl
├── cabai_keriting_forecast.pkl
├── bawang_merah_forecast.pkl
├── bawang_putih_forecast.pkl
├── magnitude_model.pkl
└── timing_classifier.pkl
```
