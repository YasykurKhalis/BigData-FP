# Roadmap 02 - Lakehouse

Tujuan fase ini adalah mengubah raw ingest menjadi data analitik yang siap dipakai ML, dashboard, dan audit historis.

## File yang dikerjakan

- [lakehouse/01_bronze.py](../BigData-FP/lakehouse/01_bronze.py)
- [lakehouse/02_silver.py](../BigData-FP/lakehouse/02_silver.py)
- [lakehouse/03_gold.py](../BigData-FP/lakehouse/03_gold.py)
- [lakehouse/export_gold.py](../BigData-FP/lakehouse/export_gold.py)
- [lakehouse/time_travel_demo.py](../BigData-FP/lakehouse/time_travel_demo.py)
- [tests/test_silver_transformations.py](../BigData-FP/tests/test_silver_transformations.py)

## Yang harus dilakukan

1. Definisikan layout Bronze sebagai append-only raw data dengan metadata lengkap.
2. Definisikan transformasi Silver untuk cleaning, deduplication, schema enforcement, dan normalisasi waktu.
3. Gabungkan sumber yang relevan di Silver, misalnya harga, cuaca, berita, dan batch supply.
4. Bentuk tabel Gold sebagai feature store dan tabel analitik.
5. Ekspor tabel Gold ke JSON di HDFS untuk dibaca dashboard.
6. Siapkan demo time travel untuk menunjukkan versi data historis dan audit perubahan.
7. Tambahkan test transformasi Silver agar hasil cleaning dan join konsisten.

## Deliverable

- Folder Bronze, Silver, dan Gold terisi data yang jelas strukturnya.
- File export untuk dashboard tersedia di HDFS.
- Feature store minimal tersedia untuk modeling.

## Validasi fase

- Data Bronze bisa dibaca ulang dari HDFS.
- Silver tidak menghasilkan duplikasi event yang sama.
- Gold menghasilkan dataset agregat yang stabil.
- Export JSON terbaca oleh dashboard.

## Dependency

- Bergantung pada output ingest fase 01.
