# Roadmap 01 - Fondasi Ingest

Tujuan fase ini adalah memastikan semua sumber data masuk dengan format yang konsisten dan bisa ditulis ke HDFS secara stabil.

## File yang dikerjakan

- [setup_kafka_topics.py](../BigData-FP/setup_kafka_topics.py)
- [kafka/producer_weather.py](../BigData-FP/kafka/producer_weather.py)
- [kafka/producer_news_pangan.py](../BigData-FP/kafka/producer_news_pangan.py)
- [kafka/producer_price_pihps.py](../BigData-FP/kafka/producer_price_pihps.py)
- [kafka/producer_price_bapanas.py](../BigData-FP/kafka/producer_price_bapanas.py)
- [kafka/producer_price_siskaperbapo.py](../BigData-FP/kafka/producer_price_siskaperbapo.py)
- [kafka/producer_kurs_bi.py](../BigData-FP/kafka/producer_kurs_bi.py)
- [hdfs/consumer_to_hdfs.py](../BigData-FP/hdfs/consumer_to_hdfs.py)
- [hdfs/test_hdfs_connection.py](../BigData-FP/hdfs/test_hdfs_connection.py)
- [batch_ingest/scheduler.py](../BigData-FP/batch_ingest/scheduler.py)
- [batch_ingest/ingest_bps_produksi.py](../BigData-FP/batch_ingest/ingest_bps_produksi.py)
- [batch_ingest/ingest_bps_imporekspor.py](../BigData-FP/batch_ingest/ingest_bps_imporekspor.py)
- [batch_ingest/ingest_bulog_stok.py](../BigData-FP/batch_ingest/ingest_bulog_stok.py)
- [batch_ingest/ingest_pupuk_harga.py](../BigData-FP/batch_ingest/ingest_pupuk_harga.py)
- [tests/test_producers.py](../BigData-FP/tests/test_producers.py)
## Yang harus dilakukan

1. Finalisasi skema pesan untuk tiap topic Kafka.
2. Samakan field wajib seperti `source`, `commodity`, `location`, `timestamp`, dan `ingestion_ts`.
3. Implementasikan producer yang masih stub agar semua topic aktif.
4. Pastikan fallback saat Kafka tidak tersedia tetap menghasilkan file lokal yang valid.
5. Pastikan consumer menulis ke HDFS dengan struktur path yang seragam per tanggal dan per domain.
6. Implementasikan batch source (BPS, Bulog, Pupuk) untuk data pasokan/supply.
7. Tambahkan scheduler untuk eksekusi batch ingestion secara berkala.
8. Tambahkan smoke test yang memverifikasi koneksi HDFS, create folder, write, read, dan list.
9. Tambahkan test minimal untuk payload producer dan fallback behavior.

## Deliverable

- 6 topic Kafka aktif.
- Producer utama menghasilkan event valid.
- Batch ingestion script selesai dan bisa dijalankan via scheduler.
- Consumer berhasil menulis batch ke HDFS.
- Smoke test HDFS lolos.

## Validasi fase

- `python setup_kafka_topics.py --list`
- `python hdfs/test_hdfs_connection.py`
- Jalankan producer `--once` dan pastikan consumer menulis file ke HDFS.

## Dependency

- Bergantung pada Docker Kafka dan Hadoop yang berjalan.
