# Roadmap 05 - Dashboard

Tujuan fase ini adalah menyajikan hasil lakehouse, model, alert, dan rekomendasi ke pengguna dalam bentuk dashboard yang siap demo.

## File yang dikerjakan

- [dashboard/app.py](../BigData-FP/dashboard/app.py)
- [dashboard/templates/base.html](../BigData-FP/dashboard/templates/base.html)
- [dashboard/templates/index.html](../BigData-FP/dashboard/templates/index.html)
- [dashboard/templates/komoditas.html](../BigData-FP/dashboard/templates/komoditas.html)
- [dashboard/templates/lakehouse.html](../BigData-FP/dashboard/templates/lakehouse.html)
- [dashboard/templates/peta_sentra.html](../BigData-FP/dashboard/templates/peta_sentra.html)
- [dashboard/templates/rekomendasi.html](../BigData-FP/dashboard/templates/rekomendasi.html)
- [dashboard/templates/evaluasi.html](../BigData-FP/dashboard/templates/evaluasi.html)
- [dashboard/static/js/chart.js](../BigData-FP/dashboard/static/js/chart.js)
- [dashboard/static/js/map.js](../BigData-FP/dashboard/static/js/map.js)
- [dashboard/static/js/alert.js](../BigData-FP/dashboard/static/js/alert.js)
- [dashboard/static/css/style.css](../BigData-FP/dashboard/static/css/style.css)

## Yang harus dilakukan

1. Hubungkan dashboard ke HDFS untuk membaca export JSON dan model artifacts.
2. Buat halaman ringkasan utama yang menampilkan status pipeline, risk index, dan alert.
3. Buat halaman detail per komoditas untuk tren harga, forecast, dan sinyal risiko.
4. Buat halaman lakehouse untuk memperlihatkan struktur Bronze, Silver, dan Gold.
5. Buat peta sentra yang menampilkan wilayah berisiko terganggu.
6. Tampilkan rekomendasi naratif dan hasil evaluasi model.
7. Rapikan CSS dan JavaScript agar demo terlihat jelas dan informatif.

## Deliverable

- Dashboard dapat dibuka dari browser lokal.
- Data tampil dari HDFS, bukan hardcode.
- Visualisasi tren, peta, alert, dan rekomendasi tersedia.

## Validasi fase

- Halaman utama tidak error saat membaca data.
- Grafik dan peta menerima data asli dari export.
- Tampilan tetap terbaca di desktop dan mobile.

## Dependency

- Bergantung pada export Gold dan output model dari fase 02 dan 03.
