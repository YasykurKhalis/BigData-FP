# Roadmap 04 - Alerting dan Rekomendasi

Tujuan fase ini adalah mengubah hasil model menjadi aksi yang bisa ditindaklanjuti.

## File yang dikerjakan

- [alerts/alert_config.yml](../BigData-FP/alerts/alert_config.yml)
- [alerts/alert_engine.py](../BigData-FP/alerts/alert_engine.py)
- [ml/recommendation_llm.py](../BigData-FP/ml/recommendation_llm.py)

## Yang harus dilakukan

1. Tetapkan threshold risk index dan aturan kapan alert harus aktif.
2. Tentukan channel notifikasi yang dipakai, misalnya log, file, atau integrasi eksternal jika tersedia.
3. Implementasikan alert engine yang membaca output risk index terbaru.
4. Tambahkan logika eskalasi jika risiko tinggi bertahan beberapa periode.
5. Buat rekomendasi naratif yang menjelaskan apa yang sebaiknya dilakukan pemerintah atau pelaku usaha.
6. Pastikan rekomendasi bisa mengacu ke faktor dominan dari model.

## Input dan output teknis

- Input utama: file `risk_index` dan `feature_importance` dari fase 03.
- Input tambahan: komoditas, sentra, level risiko, dan timestamp snapshot.
- Output alert: record notifikasi yang bisa disimpan ke file/log atau diteruskan ke channel lain.
- Output rekomendasi: teks naratif singkat untuk dashboard dan dokumen laporan.

## Aturan operasional

- Alert dijalankan secara periodik setelah output fase 03 tersedia.
- Threshold harus bisa diubah lewat `alert_config.yml` tanpa ubah kode.
- Jika risiko melewati threshold berturut-turut, status harus naik dari warning ke critical.
- Rekomendasi harus mengikuti konteks komoditas dan faktor dominan, bukan generik.

## Deliverable

- Konfigurasi threshold alert.
- Alert engine yang aktif.
- Teks rekomendasi tindakan yang bisa dipakai dashboard.

## Validasi fase

- Alert muncul ketika risk index melewati threshold yang dikonfigurasi.
- Escalation bekerja saat risiko tinggi bertahan beberapa siklus.
- Rekomendasi sesuai dengan komoditas, sentra, dan faktor dominan.
- Output alert dan rekomendasi bisa dibaca dashboard.

## Dependency

- Bergantung pada output fase 03.
