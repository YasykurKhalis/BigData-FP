# LUMBUNG - Quick Start (connect Kafka + HDFS)

Target: dari laptop Ryan ke sistem yang bisa nge-write ke HDFS dalam < 5 menit.

## 0. Prasyarat

- Docker Desktop running (WSL2 backend)
- Python 3.11.9 (`venv311` aktif)
- Port `9000`, `9092`, `9870`, `8088`, `2181` free

## 1. Install dependency Python

```bash
pip install -r requirements.txt
```

Yang penting buat connect HDFS: `kafka-python`, `hdfs`, `requests`.

## 2. Start Hadoop cluster

```bash
docker compose -f docker-compose-hadoop.yml up -d
```

Bikin network `lumbung-net`, lalu spin: namenode + datanode + resourcemanager + nodemanager + historyserver.

Tunggu ~30-60 detik. Cek status:

```bash
docker ps --filter name=lumbung-
docker logs lumbung-namenode --tail 20
```

Buka di browser:

- HDFS NameNode UI: <http://localhost:9870>
- YARN RM UI: <http://localhost:8088>

## 3. Smoke test HDFS

```bash
python hdfs/test_hdfs_connection.py
```

Expected output:

```
[1/5] Cek status NameNode...     OK
[2/5] makedirs /data/lumbung...  OK
[3/5] Tulis ...hello_...json...  OK
[4/5] List ...                   OK
[5/5] Read ...                   OK - message='Halo HDFS dari LUMBUNG!'
SUCCESS - HDFS reachable & writable
```

Verifikasi di browser: <http://localhost:9870/explorer.html#/data/lumbung/smoke_test>

## 4. Start Kafka

```bash
docker compose -f docker-compose-kafka.yml up -d
```

Tunggu ~20 detik. Cek:

```bash
docker logs lumbung-kafka --tail 20
```

## 5. Buat 6 topic LUMBUNG

```bash
python setup_kafka_topics.py
python setup_kafka_topics.py --list      # verifikasi
```

## 6. Jalankan producer cuaca

Terminal A:

```bash
python kafka/producer_weather.py --once
```

Akan fetch 5 sentra dari Open-Meteo dan kirim ke topic `weather-sentra`.

## 7. Jalankan consumer (Kafka -> HDFS)

Terminal B (sambil producer jalan / setelah):

```bash
python hdfs/consumer_to_hdfs.py --once
```

Setiap batch akan ditulis ke:

```
/data/lumbung/streaming/weather/YYYY-MM-DD/batch_HHMMSS.jsonl
```

Cek di UI: <http://localhost:9870/explorer.html#/data/lumbung/streaming/weather>

## 8. Demo loop (opsional)

Jalankan producer terus-menerus + consumer terus-menerus:

```bash
# Terminal A
python kafka/producer_weather.py --interval 60

# Terminal B
python hdfs/consumer_to_hdfs.py
```

## Troubleshooting

| Masalah | Solusi |
|---|---|
| `docker compose up` error `network lumbung-net not found` | Start hadoop dulu (yang bikin network), baru kafka |
| Producer `Kafka unavailable` | Kafka belum jalan / port 9092 ke-block - cek `docker logs lumbung-kafka` |
| `hdfs_test FAIL - NameNode tidak respon` | NameNode butuh ~30s warm up, retry |
| WebHDFS 403 | Pastikan `HDFS_CONF_dfs_permissions_enabled=false` di `hadoop.env` |
| Producer fetch error | Cek koneksi internet (Open-Meteo) |

## Shutdown

```bash
docker compose -f docker-compose-kafka.yml down
docker compose -f docker-compose-hadoop.yml down
# Hapus volume juga (data hilang):
docker compose -f docker-compose-hadoop.yml down -v
```
