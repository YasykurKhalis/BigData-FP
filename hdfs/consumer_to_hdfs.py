"""LUMBUNG - Consumer Kafka multi-topic -> HDFS (via WebHDFS REST).

Owner: Ryan (5027231046)

Subscribe ke 6 topic LUMBUNG, buffer pesan, flush ke HDFS dengan
struktur date-partitioned:

  /data/lumbung/streaming/prices/pihps/YYYY-MM-DD/<batch>.jsonl
  /data/lumbung/streaming/prices/bapanas/YYYY-MM-DD/<batch>.jsonl
  /data/lumbung/streaming/prices/siskaperbapo/YYYY-MM-DD/<batch>.jsonl
  /data/lumbung/streaming/weather/YYYY-MM-DD/<batch>.jsonl
  /data/lumbung/streaming/news/YYYY-MM-DD/<batch>.jsonl
  /data/lumbung/streaming/kurs/YYYY-MM-DD/<batch>.jsonl

Pakai WebHDFS REST (lib `hdfs`) supaya tidak perlu libhdfs native.
Fallback ke temp_buffer/ kalau HDFS down (dev mode).

USAGE:
    python hdfs/consumer_to_hdfs.py
    python hdfs/consumer_to_hdfs.py --once
    python hdfs/consumer_to_hdfs.py --local           # paksa lokal
"""

from __future__ import annotations
import argparse
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# Patch DNS supaya host Windows bisa follow WebHDFS redirect ke `datanode:9864`
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _dns_patch import patch_dns  # noqa: E402
patch_dns()

BOOTSTRAP = "localhost:9092"
CONSUMER_GROUP = "lumbung-hdfs-consumer"

TOPICS = [
    "price-pihps",
    "price-bapanas",
    "price-siskaperbapo",
    "weather-sentra",
    "news-pangan",
    "kurs-bi",
]

TOPIC_PATH = {
    "price-pihps":        "streaming/prices/pihps",
    "price-bapanas":      "streaming/prices/bapanas",
    "price-siskaperbapo": "streaming/prices/siskaperbapo",
    "weather-sentra":     "streaming/weather",
    "news-pangan":        "streaming/news",
    "kurs-bi":            "streaming/kurs",
}

HDFS_ROOT = "/data/lumbung"
WEBHDFS_URL = os.getenv("WEBHDFS_URL", "http://localhost:9870")
HDFS_USER = os.getenv("HDFS_USER", "root")

FLUSH_EVERY_N_MSG = 5    # demo-responsive (sebelumnya 50)
FLUSH_EVERY_SEC = 10     # demo-responsive (sebelumnya 60)

LOCAL_FALLBACK_ROOT = Path(__file__).resolve().parent.parent / "temp_buffer"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("consumer_to_hdfs")


def make_hdfs():
    """Return WebHDFS client (lib `hdfs`) atau None."""
    try:
        from hdfs import InsecureClient
    except ImportError:
        log.warning("Library `hdfs` belum terinstall - fallback lokal.")
        log.warning("Install: pip install hdfs")
        return None
    try:
        client = InsecureClient(WEBHDFS_URL, user=HDFS_USER)
        client.status("/")  # probe
        log.info(f"HDFS connected: {WEBHDFS_URL} as {HDFS_USER}")
        return client
    except Exception as e:
        log.warning(f"HDFS unavailable ({type(e).__name__}: {e}) - fallback lokal.")
        return None


def make_consumer():
    from kafka import KafkaConsumer
    return KafkaConsumer(
        *TOPICS,
        bootstrap_servers=BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
        key_deserializer=lambda v: v.decode("utf-8") if v else None,
        consumer_timeout_ms=5000,
    )


def hdfs_path(topic, date_str, batch_ts):
    return f"{HDFS_ROOT}/{TOPIC_PATH[topic]}/{date_str}/batch_{batch_ts}.jsonl"


def local_path(topic, date_str, batch_ts):
    p = LOCAL_FALLBACK_ROOT / TOPIC_PATH[topic] / date_str
    p.mkdir(parents=True, exist_ok=True)
    return p / f"batch_{batch_ts}.jsonl"


def flush_buffer(buffer, hdfs):
    if not any(buffer.values()):
        return 0

    total = 0
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    batch_ts = datetime.utcnow().strftime("%H%M%S")

    for topic, msgs in buffer.items():
        if not msgs:
            continue
        payload = "\n".join(json.dumps(m, ensure_ascii=False) for m in msgs)

        wrote = False
        if hdfs is not None:
            try:
                target = hdfs_path(topic, date_str, batch_ts)
                folder = os.path.dirname(target)
                hdfs.makedirs(folder)
                with hdfs.write(target, encoding="utf-8", overwrite=True) as w:
                    w.write(payload)
                log.info(f"-> HDFS  {target} ({len(msgs)} msg)")
                wrote = True
            except Exception as e:
                log.error(f"HDFS write gagal ({topic}): {e} - fallback lokal.")

        if not wrote:
            target = local_path(topic, date_str, batch_ts)
            target.write_text(payload, encoding="utf-8")
            log.info(f"-> LOCAL {target} ({len(msgs)} msg)")

        total += len(msgs)

    for k in list(buffer.keys()):
        buffer[k].clear()
    return total


def run(once, force_local):
    hdfs = None if force_local else make_hdfs()

    try:
        consumer = make_consumer()
    except Exception as e:
        log.error(f"Tidak bisa connect Kafka {BOOTSTRAP}: {e}")
        return 1

    buffer = defaultdict(list)
    last_flush = time.time()
    received = 0

    log.info("=" * 60)
    log.info("LUMBUNG consumer_to_hdfs - listening on:")
    for t in TOPICS:
        log.info(f"  * {t}")
    log.info(f"Sink   : HDFS={'YES' if hdfs else 'NO (local fallback)'}")
    log.info(f"Flush  : every {FLUSH_EVERY_N_MSG} msg OR {FLUSH_EVERY_SEC}s")
    log.info("=" * 60)

    poll_errors = 0
    MAX_POLL_ERRORS = 5
    try:
        while True:
            # Workaround bug kafka-python-ng + Python 3.13:
            # ValueError "Invalid file descriptor: -1" akibat race condition
            # selector saat coordinator reconnect. Catch + retry.
            try:
                polled = consumer.poll(timeout_ms=2000)
                poll_errors = 0  # reset setelah success
            except ValueError as e:
                if "Invalid file descriptor" in str(e):
                    poll_errors += 1
                    log.warning(f"Selector race ({poll_errors}/{MAX_POLL_ERRORS}): {e} - retry")
                    if poll_errors >= MAX_POLL_ERRORS:
                        log.error("Terlalu banyak selector error - abort")
                        break
                    time.sleep(1)
                    continue
                raise

            for tp, msgs in polled.items():
                for m in msgs:
                    buffer[tp.topic].append(m.value)
                    received += 1

            now = time.time()
            count = sum(len(v) for v in buffer.values())

            if count >= FLUSH_EVERY_N_MSG or (count > 0 and now - last_flush >= FLUSH_EVERY_SEC):
                flushed = flush_buffer(buffer, hdfs)
                log.info(f"FLUSH  : {flushed} msg  | total received={received}")
                last_flush = now

            if once and received > 0 and count == 0:
                log.info("Once mode: exit setelah buffer kosong.")
                break

    except KeyboardInterrupt:
        log.info("Interrupted - flush final buffer...")
    finally:
        flush_buffer(buffer, hdfs)
        try:
            consumer.close()
        except Exception:
            pass

    return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Exit setelah buffer kosong sekali")
    parser.add_argument("--local", action="store_true", help="Paksa tulis lokal (skip HDFS)")
    args = parser.parse_args()
    return run(once=args.once, force_local=args.local)


if __name__ == "__main__":
    sys.exit(main())
