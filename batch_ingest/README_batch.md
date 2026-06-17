# Batch Ingestion — LUMBUNG

Owner: **Yasykur Khalis** (5027241112)

Modul ini melakukan ingestion dari sumber struktural yang tidak streaming:

| Sumber | Frekuensi | Target HDFS |
|--------|-----------|-------------|
| BPS Produksi & Luas Panen | Bulanan | `/data/lumbung/batch/bps_produksi/YYYY-MM/` |
| BPS Impor-Ekspor Pangan | Bulanan | `/data/lumbung/batch/bps_imporekspor/YYYY-MM/` |
| Stok Bulog | Mingguan | `/data/lumbung/batch/bulog_stok/YYYY-WW/` |
| Harga Pupuk PIHC | Bulanan | `/data/lumbung/batch/pupuk_harga/YYYY-MM/` |

Dijalankan via `scheduler.py` (APScheduler) atau cron eksternal.
