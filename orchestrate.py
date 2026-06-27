"""
LUMBUNG — Pipeline Orchestrator
Owner: tim

Jalankan urutan pipeline end-to-end secara lokal (tanpa Airflow):
  1. Batch ingest (World Bank, BPS, dll)
  2. [Opsional] Kafka producers + consumer (jika Kafka tersedia)
  3. Lakehouse: Bronze → Silver → Gold
  4. Export Gold ke JSON
  5. ML: NLP extraction → Risk Index → Feature Importance → Evaluation
  6. Alert Engine
  7. LLM Recommendation
  8. Dashboard (dijalankan terpisah)

USAGE:
  python orchestrate.py               # jalankan semua tahap
  python orchestrate.py --skip-kafka  # skip tahap Kafka (mode offline)
  python orchestrate.py --only-ml     # hanya jalankan tahap ML
  python orchestrate.py --only-dash   # hanya jalankan dashboard
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

BASE_DIR   = Path(__file__).resolve().parent
PYTHON     = sys.executable


def run_step(name: str, script: str, extra_args: list[str] | None = None) -> bool:
    """
    Jalankan satu langkah pipeline sebagai subprocess.
    Kembalikan True jika sukses.
    """
    cmd = [PYTHON, str(BASE_DIR / script)] + (extra_args or [])
    log.info(f"▶ [{name}] {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, check=True, cwd=str(BASE_DIR))
        log.info(f"✓ [{name}] selesai (exit={result.returncode})")
        return True
    except subprocess.CalledProcessError as e:
        log.error(f"✗ [{name}] gagal (exit={e.returncode})")
        return False
    except FileNotFoundError:
        log.error(f"✗ [{name}] script tidak ditemukan: {script}")
        return False


def run_snapshot_generator() -> None:
    log.info("=" * 50)
    log.info("TAHAP 0: Generate Synthetic Snapshot")
    log.info("=" * 50)
    run_step("Snapshot Generator", "scripts/generate_snapshot.py")


def run_batch_ingest() -> None:
    log.info("=" * 50)
    log.info("TAHAP 1: Batch Ingest")
    log.info("=" * 50)
    run_step("BPS Produksi",   "batch_ingest/ingest_bps_produksi.py")
    run_step("BPS Impor-Ekspor","batch_ingest/ingest_bps_imporekspor.py")
    run_step("Bulog Stok",     "batch_ingest/ingest_bulog_stok.py")
    run_step("Pupuk Harga",    "batch_ingest/ingest_pupuk_harga.py")


def run_kafka_producers() -> None:
    log.info("=" * 50)
    log.info("TAHAP 2: Kafka Producers (mode --once)")
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


def run_lakehouse() -> None:
    log.info("=" * 50)
    log.info("TAHAP 3: Lakehouse (Bronze → Silver → Gold)")
    log.info("=" * 50)
    ok = run_step("Bronze", "lakehouse/01_bronze.py")
    if not ok:
        log.warning("Bronze gagal, Silver dan Gold mungkin tidak optimal.")
    run_step("Silver", "lakehouse/02_silver.py")
    run_step("Gold",   "lakehouse/03_gold.py")
    run_step("Export", "lakehouse/export_gold.py")


def run_ml() -> None:
    log.info("=" * 50)
    log.info("TAHAP 4: Machine Learning Pipeline")
    log.info("=" * 50)
    run_step("NLP Extractor",    "ml/nlp_keyword_extractor.py")
    run_step("Feature Importance","ml/feature_importance.py")
    run_step("Risk Index",       "ml/compute_risk_index.py")
    run_step("Evaluation",       "ml/evaluation.py")
    run_step("Recommendation",   "ml/recommendation_llm.py")


def run_alerts() -> None:
    log.info("=" * 50)
    log.info("TAHAP 5: Alert Engine")
    log.info("=" * 50)
    run_step("Alert Engine", "alerts/alert_engine.py")


def run_dashboard() -> None:
    log.info("=" * 50)
    log.info("TAHAP 6: Dashboard Flask")
    log.info("=" * 50)
    log.info("Jalankan dashboard secara manual:")
    log.info("  python dashboard/app.py")
    log.info("  Akses di: http://localhost:5000")


def main() -> int:
    parser = argparse.ArgumentParser(description="LUMBUNG Pipeline Orchestrator")
    parser.add_argument("--skip-kafka",  action="store_true", help="Skip Kafka producers")
    parser.add_argument("--skip-ingest", action="store_true", help="Skip batch ingest")
    parser.add_argument("--only-ml",     action="store_true", help="Hanya jalankan ML pipeline")
    parser.add_argument("--only-dash",   action="store_true", help="Hanya info dashboard")
    parser.add_argument("--generate-snapshot", action="store_true", help="Generate synthetic Big Data snapshot sebelum pipeline")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("🌾 LUMBUNG — Pipeline Orchestrator dimulai")
    log.info("=" * 60)

    if args.only_dash:
        run_dashboard()
        return 0

    if args.only_ml:
        run_ml()
        run_alerts()
        return 0

    if args.generate_snapshot:
        run_snapshot_generator()

    if not args.skip_ingest:
        run_batch_ingest()

    if not args.skip_kafka:
        run_kafka_producers()

    run_lakehouse()
    run_ml()
    run_alerts()
    run_dashboard()

    log.info("\n" + "=" * 60)
    log.info("✅ Pipeline selesai. Jalankan dashboard:")
    log.info("   python dashboard/app.py")
    log.info("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
