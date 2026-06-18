"""
LUMBUNG — Setup Kafka Topics
Owner: Ryan (5027231046)

Buat 6 Kafka topics yang dipakai pipeline LUMBUNG.

USAGE:
    python setup_kafka_topics.py
    python setup_kafka_topics.py --list           # tampilkan topic existing
    python setup_kafka_topics.py --delete         # hapus semua topic LUMBUNG
"""

from __future__ import annotations

import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("setup_kafka_topics")

BOOTSTRAP = "localhost:9092"

TOPICS = [
    "price-pihps",
    "price-bapanas",
    "price-siskaperbapo",
    "weather-sentra",
    "news-pangan",
    "kurs-bi",
]

NUM_PARTITIONS = 3
REPLICATION_FACTOR = 1


def get_admin():
    from kafka.admin import KafkaAdminClient  # type: ignore
    return KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="lumbung-admin")


def list_topics() -> list[str]:
    admin = get_admin()
    return sorted(admin.list_topics())


def create_topics() -> None:
    from kafka.admin import NewTopic  # type: ignore
    from kafka.errors import TopicAlreadyExistsError  # type: ignore

    admin = get_admin()
    existing = set(admin.list_topics())

    to_create = [
        NewTopic(name=t, num_partitions=NUM_PARTITIONS, replication_factor=REPLICATION_FACTOR)
        for t in TOPICS if t not in existing
    ]
    for t in TOPICS:
        if t in existing:
            log.info(f"SKIP  (sudah ada): {t}")

    if not to_create:
        log.info("Semua topik sudah ada. Tidak ada yang dibuat.")
        return

    try:
        admin.create_topics(new_topics=to_create, validate_only=False)
        for t in to_create:
            log.info(f"OK    (dibuat)   : {t.name}")
    except TopicAlreadyExistsError as e:
        log.warning(f"Topic already exists: {e}")


def delete_topics() -> None:
    admin = get_admin()
    existing = set(admin.list_topics())
    to_delete = [t for t in TOPICS if t in existing]
    if not to_delete:
        log.info("Tidak ada topik LUMBUNG untuk dihapus.")
        return
    admin.delete_topics(to_delete)
    for t in to_delete:
        log.info(f"DEL   : {t}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--list", action="store_true", help="List topik yang ada")
    parser.add_argument("--delete", action="store_true", help="Hapus semua topik LUMBUNG")
    args = parser.parse_args()

    log.info(f"Bootstrap: {BOOTSTRAP}")

    try:
        if args.list:
            topics = list_topics()
            log.info(f"Total {len(topics)} topik di cluster:")
            for t in topics:
                marker = "*" if t in TOPICS else " "
                print(f"  {marker} {t}")
            return 0

        if args.delete:
            delete_topics()
            return 0

        create_topics()
        log.info("Selesai. Topik LUMBUNG final di cluster:")
        for t in list_topics():
            if t in TOPICS:
                print(f"  * {t}")
        return 0

    except Exception as e:
        log.error(f"Gagal: {type(e).__name__}: {e}")
        log.error(f"Pastikan Kafka broker jalan di {BOOTSTRAP}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
