# Roadmap 06 - Orchestration dan Testing

Tujuan fase ini adalah menyatukan semua komponen menjadi pipeline yang bisa dijalankan end-to-end dan diverifikasi dengan test.

## File yang dikerjakan

- [orchestrate.py](../BigData-FP/orchestrate.py)
- [tests/__init__.py](../BigData-FP/tests/__init__.py)

## Yang harus dilakukan

1. Jadikan orchestrator sebagai entry point untuk pipeline lengkap.
2. Tentukan urutan eksekusi yang benar: ingest (batch/streaming) -> HDFS -> lakehouse -> ML -> alert -> dashboard.
3. Tambahkan test integrasi untuk memastikan setiap tahap bisa dipanggil tanpa crash.
4. Buat checklist validasi sebelum demo.

## Deliverable

- Satu entry point untuk menjalankan pipeline secara lengkap.
- Test suite minimal bisa dijalankan per fase.

## Validasi fase

- Orchestrator tidak gagal pada urutan standar.
- Test suite memberi sinyal jika ada regresi.

## Dependency

- Bergantung pada semua fase sebelumnya.