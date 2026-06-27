"""
LUMBUNG — Pipeline Orchestrator
Owner: tim

Jalankan urutan pipeline end-to-end:
  1. Seed historical prices -> HDFS
  2. Kafka producers (--once) -> Kafka
  3. Consumer Kafka -> HDFS
  4. Lakehouse: Bronze (baca HDFS) -> Silver -> Gold -> Export ke HDFS
  5. ML: NLP extraction -> Feature Importance -> Risk Index -> Evaluation
  6. Alert Engine
  7. Dashboard (dijalankan terpisah)

USAGE:
  python orchestrate.py               # jalankan semua tahap
  python orchestrate.py --skip-kafka   # skip tahap Kafka (pakai data HDFS yg sudah ada)
  python orchestrate.py --skip-ingest  # skip batch ingest
  python orchestrate.py --only-ml      # hanya jalankan tahap ML + Alerts
  python orchestrate.py --only-lake    # hanya jalankan Lakehouse
  python orchestrate.py --only-dash    # hanya info dashboard
"""

from __future__ import annotations
import argparse
import logging
import subprocess
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("orchestrator")

BASE_DIR = Path(__file__).resolve().parent
PYTHON = sys.executable


def run_step(name: str, script: str, extra_args: list[str] | None = None) -> bool:
    cmd = [PYTHON, str(BASE_DIR / script)] + (extra_args or [])
    log.info(f">> [{name}] {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, check=True, cwd=str(BASE_DIR))
        log.info(f"OK [{name}] selesai (exit={result.returncode})")
        return True
    except subprocess.CalledProcessError as e:
        log.error(f"GAGAL [{name}] (exit={e.returncode})")
        return False
    except FileNotFoundError:
        log.error(f"GAGAL [{name}] script tidak ditemukan: {script}")
        return False


def run_batch_ingest() -> None:
    log.info("=" * 50)
    log.info("TAHAP 1: Batch Ingest")
    log.info("=" * 50)
    run_step("BPS Produksi",    "batch_ingest/ingest_bps_produksi.py")
    run_step("BPS Impor-Ekspor", "batch_ingest/ingest_bps_imporekspor.py")
    run_step("Bulog Stok",      "batch_ingest/ingest_bulog_stok.py")
    run_step("Pupuk Harga",     "batch_ingest/ingest_pupuk_harga.py")


def run_seed() -> None:
    log.info("=" * 50)
    log.info("TAHAP 2: Seed Historical Prices -> HDFS")
    log.info("=" * 50)
    run_step("Historical Seed", "lakehouse/seed_historical_prices.py")


def run_kafka_producers() -> None:
    log.info("=" * 50)
    log.info("TAHAP 3: Kafka Producers (mode --once)")
    log.info("=" * 50)
    scripts = [
        ("Price Bapanas",      "kafka/producer_price_bapanas.py"),
        ("Price PIHPS",        "kafka/producer_price_pihps.py"),
        ("Price Siskaperbapo", "kafka/producer_price_siskaperbapo.py"),
        ("Weather",            "kafka/producer_weather.py"),
        ("News Pangan",        "kafka/producer_news_pangan.py"),
        ("Kurs BI",            "kafka/producer_kurs_bi.py"),
    ]
    for name, script in scripts:
        run_step(name, script, ["--once"])


def run_consumer() -> None:
    log.info("=" * 50)
    log.info("TAHAP 4: Consumer Kafka -> HDFS")
    log.info("=" * 50)
    run_step("Consumer HDFS", "hdfs/consumer_to_hdfs.py", ["--once"])


def run_lakehouse() -> None:
    log.info("=" * 50)
    log.info("TAHAP 5: Lakehouse (Bronze -> Silver -> Gold)")
    log.info("=" * 50)
    ok = run_step("Bronze", "lakehouse/01_bronze.py")
    if not ok:
        log.warning("Bronze gagal, Silver dan Gold mungkin tidak optimal.")
    run_step("Silver", "lakehouse/02_silver.py")
    run_step("Gold",   "lakehouse/03_gold.py")
    run_step("Export HDFS", "lakehouse/export_to_hdfs.py")


def run_ml() -> None:
    log.info("=" * 50)
    log.info("TAHAP 6: Machine Learning Pipeline")
    log.info("=" * 50)
    run_step("NLP Extractor",     "ml/nlp_keyword_extractor.py")
    run_step("Feature Importance", "ml/feature_importance.py")
    run_step("Risk Index",        "ml/compute_risk_index.py")
    run_step("Evaluation",        "ml/evaluation.py")
    run_step("Recommendation",    "ml/recommendation_llm.py")


def run_alerts() -> None:
    log.info("=" * 50)
    log.info("TAHAP 7: Alert Engine")
    log.info("=" * 50)
    run_step("Alert Engine", "alerts/alert_engine.py")


def run_dashboard() -> None:
    log.info("=" * 50)
    log.info("TAHAP 8: Dashboard Flask")
    log.info("=" * 50)
    log.info("Jalankan dashboard secara manual:")
    log.info("  python dashboard/app.py")
    log.info("  Akses di: http://localhost:5000")


def main() -> int:
    parser = argparse.ArgumentParser(description="LUMBUNG Pipeline Orchestrator")
    parser.add_argument("--skip-kafka",  action="store_true", help="Skip Kafka producers + consumer")
    parser.add_argument("--skip-ingest", action="store_true", help="Skip batch ingest")
    parser.add_argument("--only-ml",     action="store_true", help="Hanya jalankan ML + Alerts")
    parser.add_argument("--only-lake",   action="store_true", help="Hanya jalankan Lakehouse")
    parser.add_argument("--only-dash",   action="store_true", help="Hanya info dashboard")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("LUMBUNG -- Pipeline Orchestrator dimulai")
    log.info("=" * 60)

    if args.only_dash:
        run_dashboard()
        return 0

    if args.only_ml:
        run_ml()
        run_alerts()
        return 0

    if args.only_lake:
        run_lakehouse()
        return 0

    # Full pipeline
    if not args.skip_ingest:
        run_batch_ingest()

    run_seed()

    if not args.skip_kafka:
        run_kafka_producers()
        run_consumer()

    run_lakehouse()
    run_ml()
    run_alerts()
    run_dashboard()

    log.info("")
    log.info("=" * 60)
    log.info("Pipeline selesai. Jalankan dashboard:")
    log.info("   python dashboard/app.py")
    log.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
