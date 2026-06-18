"""LUMBUNG - Producer RSS Berita Pangan
Owner: Ryan (5027231046)

Pull RSS feed dari 3 portal ekonomi besar (Kompas, Tempo, Antara),
filter judul/deskripsi pakai keyword komoditas + sentra + kata kunci
gangguan pasokan, lalu kirim ke Kafka topic `news-pangan`.

USAGE:
    python kafka/producer_news_pangan.py --dry-run --once
    python kafka/producer_news_pangan.py --once
    python kafka/producer_news_pangan.py
"""

from __future__ import annotations
import argparse
import hashlib
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "news-pangan"

RSS_FEEDS = [
    # Detik Finance = sumber utama (terbukti delivering relevant news)
    ("detik-finance",   "https://finance.detik.com/rss"),
    ("detik-news",      "https://news.detik.com/rss"),
    # Antara - kadang ada artikel ekonomi pangan
    ("antara-ekonomi",  "https://www.antaranews.com/rss/ekonomi.xml"),
    ("antara-terkini",  "https://www.antaranews.com/rss/terkini.xml"),
    # CNN Indonesia
    ("cnn-ekonomi",     "https://www.cnnindonesia.com/ekonomi/rss"),
]

KOMODITAS_KEYWORDS = [
    "beras", "padi", "gabah",
    "cabai", "cabe", "rawit", "keriting",
    "bawang merah", "bawang putih", "bawang",
]

SUPPLY_KEYWORDS = [
    "harga", "lonjakan", "kenaikan", "naik", "turun",
    "panen", "gagal panen", "produksi", "stok", "pasokan",
    "impor", "ekspor", "subsidi",
    "bulog", "bapanas", "pihps", "kementan",
    "el nino", "la nina", "hujan", "kekeringan", "banjir",
    "inflasi", "pangan",
]

SENTRA_KEYWORDS = [
    "brebes", "karawang", "magelang", "cianjur", "probolinggo",
]

FETCH_INTERVAL_SEC = 30 * 60
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
FALLBACK_FILE = LOG_DIR / "news_fallback.jsonl"
SEEN_FILE = LOG_DIR / "news_seen.txt"
USER_AGENT = "Mozilla/5.0 (LUMBUNG-bot/0.1; +https://github.com/lumbung)"

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("producer_news")


def make_kafka_producer():
    try:
        from kafka import KafkaProducer
    except ImportError:
        log.warning("kafka-python belum terinstall - fallback file.")
        return None
    try:
        p = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda v: v.encode("utf-8") if v else None,
            acks="all", retries=3, linger_ms=200,
            request_timeout_ms=5000, api_version_auto_timeout_ms=3000,
        )
        log.info(f"Kafka connected: {KAFKA_BOOTSTRAP}")
        return p
    except Exception as e:
        log.warning(f"Kafka unavailable ({type(e).__name__}: {e}) - fallback file.")
        return None


def load_seen():
    if not SEEN_FILE.exists():
        return set()
    return set(SEEN_FILE.read_text(encoding="utf-8").splitlines())


def save_seen(seen):
    SEEN_FILE.write_text("\n".join(sorted(seen)[-5000:]), encoding="utf-8")


def fetch_feed(url):
    try:
        import feedparser
    except ImportError:
        log.error("feedparser belum terinstall. Install: pip install feedparser")
        sys.exit(2)
    r = requests.get(url, timeout=15, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return feedparser.parse(r.content)


def strip_html(text):
    return re.sub(r"<[^>]+>", "", text or "").strip()


def find_matches(text, keywords):
    text_lower = text.lower()
    return sorted({k for k in keywords if k in text_lower})


def build_message(source, entry):
    title = strip_html(entry.get("title", ""))
    summary = strip_html(entry.get("summary", "") or entry.get("description", ""))
    full_text = f"{title}  {summary}"

    komoditas = find_matches(full_text, KOMODITAS_KEYWORDS)
    supply = find_matches(full_text, SUPPLY_KEYWORDS)
    sentra = find_matches(full_text, SENTRA_KEYWORDS)

    if not komoditas:
        return None  # bukan berita pangan

    score = (len(komoditas) * 3) + len(supply) + (len(sentra) * 2)

    link = entry.get("link", "")
    art_id = hashlib.sha1(link.encode("utf-8")).hexdigest()[:16] if link else None

    now = datetime.now(timezone.utc).isoformat()
    return {
        "source": source,
        "article_id": art_id,
        "url": link,
        "title": title,
        "summary": summary[:500],
        "published": entry.get("published") or entry.get("updated"),
        "komoditas_matched": komoditas,
        "supply_keywords": supply,
        "sentra_matched": sentra,
        "relevance_score": score,
        "fetched_at_utc": now,
        "ingestion_ts": now,
    }


def send_or_fallback(producer, msg, dry_run=False):
    key = msg["article_id"] or "noid"
    if dry_run:
        log.info(f"[DRY-RUN] {key} | score={msg['relevance_score']} | "
                 f"komoditas={msg['komoditas_matched']} | {msg['title'][:80]}")
        return
    if producer is not None:
        try:
            producer.send(TOPIC, key=key, value=msg)
            log.info(f"-> Kafka [{TOPIC}] score={msg['relevance_score']} "
                     f"komoditas={msg['komoditas_matched']} | {msg['title'][:60]}")
            return
        except Exception as e:
            log.error(f"Kafka send gagal ({e}) - fallback file.")
    with FALLBACK_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(msg, ensure_ascii=False) + "\n")
    log.info(f"-> FILE  {FALLBACK_FILE.name} key={key}")


def run_once(producer, dry_run=False):
    seen = load_seen()
    total_relevant = 0
    total_skipped = 0
    new_seen = set()

    for source, url in RSS_FEEDS:
        log.info(f"Fetch {source}: {url}")
        try:
            feed = fetch_feed(url)
        except Exception as e:
            log.error(f"  fetch gagal: {type(e).__name__}: {e}")
            continue

        n_relevant = 0
        for entry in feed.entries:
            msg = build_message(source, entry)
            if msg is None:
                continue
            art_id = msg["article_id"]
            if art_id in seen:
                continue
            new_seen.add(art_id)
            send_or_fallback(producer, msg, dry_run=dry_run)
            n_relevant += 1

        total_relevant += n_relevant
        total_skipped += max(0, len(feed.entries) - n_relevant)
        log.info(f"  -> {n_relevant} relevant dari {len(feed.entries)} artikel")

    if not dry_run and producer is not None:
        try:
            producer.flush(timeout=5)
        except Exception:
            pass

    if not dry_run:
        save_seen(seen | new_seen)

    log.info(f"TOTAL relevant: {total_relevant} | skipped: {total_skipped}")
    return total_relevant


def main():
    parser = argparse.ArgumentParser(description="LUMBUNG news producer")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--interval", type=int, default=FETCH_INTERVAL_SEC)
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("LUMBUNG producer_news_pangan - RSS -> Kafka")
    log.info(f"Feeds : {[s for s, _ in RSS_FEEDS]}")
    log.info(f"Topic : {TOPIC}")
    log.info(f"Mode  : once={args.once} dry_run={args.dry_run}")
    log.info("=" * 60)

    producer = None if args.dry_run else make_kafka_producer()

    if args.once:
        n = run_once(producer, dry_run=args.dry_run)
        return 0 if n >= 0 else 1

    while True:
        run_once(producer, dry_run=args.dry_run)
        log.info(f"Sleep {args.interval}s...")
        time.sleep(args.interval)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log.info("Interrupted. Bye.")
        sys.exit(0)
