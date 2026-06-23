# Roadmap 03 - Feature Engineering dan ML

Tujuan fase ini adalah menghasilkan prediksi harga, estimasi besaran lonjakan, estimasi waktu lonjakan, dan skor risiko yang bisa dipakai downstream.

## File yang dikerjakan

- [ml/nlp_keyword_extractor.py](../BigData-FP/ml/nlp_keyword_extractor.py)
- [ml/sentra_mapper.py](../BigData-FP/ml/sentra_mapper.py)
- [ml/train_price_forecast.py](../BigData-FP/ml/train_price_forecast.py)
- [ml/train_magnitude_model.py](../BigData-FP/ml/train_magnitude_model.py)
- [ml/train_timing_classifier.py](../BigData-FP/ml/train_timing_classifier.py)
- [ml/compute_risk_index.py](../BigData-FP/ml/compute_risk_index.py)
- [ml/feature_importance.py](../BigData-FP/ml/feature_importance.py)
- [ml/evaluation.py](../BigData-FP/ml/evaluation.py)
- [tests/test_risk_index.py](../BigData-FP/tests/test_risk_index.py)

## Yang harus dilakukan

1. Ekstrak keyword dari berita dan hitung velocity atau lonjakan frekuensinya.
2. Bentuk mapping komoditas ke sentra produksi yang relevan.
3. Siapkan fitur berbasis harga historis, cuaca, berita, dan batch supply.
4. Latih model forecast harga per komoditas.
5. Latih model tambahan untuk besaran kenaikan harga.
6. Latih classifier atau model window waktu lonjakan.
7. Gabungkan output model menjadi risk index 0-100.
8. Hitung feature importance agar faktor dominan penyebab kenaikan bisa dijelaskan.
9. Buat evaluasi dengan metrik forecast, klasifikasi, dan lead time.

## Output teknis yang harus ada

- `feature_store` dari Gold layer sebagai input training.
- Model forecast harga per komoditas.
- Model magnitude dan timing sebagai artefak terpisah.
- File risk index per komoditas yang bisa dibaca alert dan dashboard.
- Ringkasan feature importance per model.
- Laporan evaluasi model dengan metrik utama dan hasil backtesting.

## Aturan operasional

- Forecast menjadi source of truth utama untuk sinyal harga.
- Risk index dihitung dari gabungan forecast, magnitude, timing, news velocity, dan weather anomaly.
- Semua artefak model disimpan di HDFS pada path model yang konsisten.
- Output fase ini harus bisa dipanggil ulang tanpa training ulang jika artefak sudah ada.

## Deliverable

- Prediksi harga per komoditas.
- Estimasi besaran lonjakan.
- Estimasi window waktu lonjakan.
- Risk index per komoditas.
- Laporan evaluasi model.

## Validasi fase

- Metrik forecast, magnitude, dan timing dihitung dan tersimpan.
- Backtesting menghasilkan hasil yang bisa dibandingkan antar komoditas.
- Risk index menghasilkan skala yang konsisten dan deterministik.
- Output model bisa dibaca oleh alert dan dashboard.

## Dependency

- Bergantung pada feature store dan Gold layer dari fase 02.
