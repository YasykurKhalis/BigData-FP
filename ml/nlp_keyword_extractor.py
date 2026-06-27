"""
LUMBUNG — NLP Keyword Extractor dari berita pangan
Owner: Hanif

Menganalisis stream artikel berita untuk mendeteksi sinyal gangguan pasokan.
Menghasilkan:
  - keyword_velocity: kecepatan kemunculan kata kunci (sinyal leading indicator)
  - relevance_score: skor relevansi artikel terhadap komoditas
  - signal_summary: ringkasan sinyal per komoditas per hari

Input : temp_buffer/streaming/news/**/*.jsonl (hasil Kafka consumer)
Output: temp_buffer/export/nlp_signals.json
"""

from __future__ import annotations
import json
import logging
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("nlp_extractor")

BASE_DIR = Path(__file__).resolve().parent.parent
NEWS_DIR  = BASE_DIR / "temp_buffer" / "streaming" / "news"
SILVER_NEWS_DIR = BASE_DIR / "temp_buffer" / "lakehouse" / "silver" / "silver_news"
EXPORT_DIR = BASE_DIR / "temp_buffer" / "export"

# ── Kamus Sinyal ──────────────────────────────────────────────────────────────

KOMODITAS_KEYWORDS: dict[str, list[str]] = {
    "beras":           ["beras", "padi", "gabah", "beras medium", "beras premium"],
    "cabai_rawit_merah": ["cabai rawit", "cabe rawit", "rawit merah", "cabai rawit merah"],
    "cabai_keriting":  ["cabai keriting", "cabe keriting", "cabai merah keriting"],
    "bawang_merah":    ["bawang merah"],
    "bawang_putih":    ["bawang putih"],
}

GANGGUAN_PASOKAN: dict[str, int] = {
    # Produksi / alam  (bobot tinggi = 3)
    "gagal panen":    3, "puso":           3, "kekeringan":     3,
    "banjir":         3, "banjir bandang": 3, "angin kencang":  3,
    "el nino":        3, "la nina":        3, "rob":            2,
    "hama":           2, "wereng":         2, "organisme pengganggu": 2,
    # Distribusi / kebijakan (bobot sedang = 2)
    "kelangkaan":     2, "stok menipis":   2, "pasokan terganggu": 2,
    "ekspor dibatasi":2, "impor tertunda": 2, "karantina":      2,
    "operasi pasar":  1, "bulog":          1, "bapanas":        1,
    # Harga (bobot rendah = 1)
    "lonjakan harga": 2, "harga naik":     1, "harga melonjak": 2,
    "mahal":          1, "langka":         2,
}

SENTRA_KEYWORDS: list[str] = [
    "brebes", "karawang", "magelang", "cianjur", "probolinggo",
    "temanggung", "wonosobo", "nganjuk", "indramayu", "sragen",
    "blitar", "garut", "batu", "solok", "tegal",
]


# ── Fungsi Inti ───────────────────────────────────────────────────────────────

def _normalize(text: str) -> str:
    """Lowercase dan hilangkan HTML/whitespace berlebih."""
    text = re.sub(r"<[^>]+>", " ", text or "")
    return re.sub(r"\s+", " ", text.lower()).strip()


def extract_signals(article: dict[str, Any]) -> dict[str, Any]:
    """
    Analisis satu artikel berita dan kembalikan record sinyal.
    Menggabungkan title + summary untuk analisis teks.
    """
    title   = _normalize(article.get("title", ""))
    summary = _normalize(article.get("summary", ""))
    full    = f"{title}  {summary}"

    # 1. Komoditas yang disebut
    komoditas_matched: list[str] = []
    for komoditas, keywords in KOMODITAS_KEYWORDS.items():
        if any(kw in full for kw in keywords):
            komoditas_matched.append(komoditas)

    if not komoditas_matched:
        return {}  # bukan artikel pangan relevan

    # 2. Sinyal gangguan pasokan beserta bobotnya
    gangguan_found: dict[str, int] = {}
    for phrase, weight in GANGGUAN_PASOKAN.items():
        if phrase in full:
            gangguan_found[phrase] = weight

    # 3. Sentra yang disebut
    sentra_found = [s for s in SENTRA_KEYWORDS if s in full]

    # 4. Hitung skor sinyal
    # Skor dasar dari komoditas + gangguan + sentra
    signal_score = (
        len(komoditas_matched) * 3
        + sum(gangguan_found.values())
        + len(sentra_found) * 2
    )

    # 5. Klasifikasi sentimen sinyal (bullish = harga naik, bearish = harga turun)
    bullish_phrases = [
        "harga naik", "lonjakan", "melonjak", "mahal", "kelangkaan",
        "gagal panen", "kekeringan", "banjir", "hama", "langka",
    ]
    bearish_phrases = [
        "harga turun", "melimpah", "surplus", "panen raya", "stok cukup",
        "operasi pasar berhasil",
    ]
    bullish_count = sum(1 for p in bullish_phrases if p in full)
    bearish_count = sum(1 for p in bearish_phrases if p in full)

    if bullish_count > bearish_count:
        sentiment = "bullish"     # tekanan kenaikan harga
    elif bearish_count > bullish_count:
        sentiment = "bearish"
    else:
        sentiment = "neutral"

    # Parse tanggal dari ingestion_ts atau published
    ts_raw = article.get("ingestion_ts") or article.get("fetched_at_utc", "")
    try:
        date_str = ts_raw[:10]  # ambil YYYY-MM-DD
    except Exception:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    return {
        "article_id":        article.get("article_id"),
        "source":            article.get("source"),
        "title":             article.get("title", "")[:200],
        "date":              date_str,
        "komoditas_matched": komoditas_matched,
        "gangguan_signals":  gangguan_found,
        "sentra_matched":    sentra_found,
        "signal_score":      signal_score,
        "sentiment":         sentiment,
    }


def compute_velocity(
    signals: list[dict],
    window_days: int = 7,
) -> dict[str, dict[str, Any]]:
    """
    Hitung keyword velocity per komoditas untuk N hari terakhir.
    Velocity = jumlah artikel relevan hari ini / rata-rata 7 hari lalu.
    Return dict: {komoditas: {daily_counts, velocity, avg_score, top_signals}}
    """
    today     = datetime.now(timezone.utc).date()
    cutoff    = today - timedelta(days=window_days * 2)

    # Kelompokkan per komoditas per hari
    counts:  defaultdict[str, Counter] = defaultdict(Counter)    # komoditas → {date → count}
    scores:  defaultdict[str, list]    = defaultdict(list)
    phrases: defaultdict[str, Counter] = defaultdict(Counter)

    for sig in signals:
        date = sig.get("date", "")
        try:
            d = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            continue
        if d < cutoff:
            continue
        for kom in sig.get("komoditas_matched", []):
            counts[kom][str(d)] += 1
            scores[kom].append(sig.get("signal_score", 0))
            for phrase in sig.get("gangguan_signals", {}):
                phrases[kom][phrase] += 1

    result: dict[str, dict] = {}
    for kom in KOMODITAS_KEYWORDS:
        daily = counts[kom]
        today_count = daily.get(str(today), 0)
        recent_dates = sorted(daily.keys())[-window_days:]
        recent_avg   = (
            sum(daily[d] for d in recent_dates) / len(recent_dates)
            if recent_dates else 0
        )
        velocity = round(today_count / recent_avg, 2) if recent_avg > 0 else 0.0
        avg_score = round(sum(scores[kom]) / len(scores[kom]), 2) if scores[kom] else 0.0
        top_signals = phrases[kom].most_common(5)

        result[kom] = {
            "daily_counts":  daily,
            "today_count":   today_count,
            "recent_avg":    round(recent_avg, 2),
            "velocity":      velocity,
            "avg_score":     avg_score,
            "top_signals":   top_signals,
        }

    return result


def run_extraction() -> dict[str, Any]:
    """
    Baca semua JSONL dari news directory, ekstrak sinyal, hitung velocity,
    dan simpan ke export/nlp_signals.json.
    """
    # Kumpulkan semua artikel — prioritas: Silver Delta table, fallback lokal
    all_articles: list[dict] = []

    # Coba baca dari Silver Delta table (sumber utama)
    try:
        from deltalake import DeltaTable
        if Path(SILVER_NEWS_DIR / "_delta_log").exists():
            df_news = DeltaTable(str(SILVER_NEWS_DIR)).to_pandas()
            all_articles = df_news.to_dict(orient="records")
            log.info(f"Dimuat {len(all_articles)} artikel dari silver_news Delta table")
    except Exception as e:
        log.warning(f"Gagal baca silver_news: {e}")

    # Fallback ke lokal JSONL
    if not all_articles:
        if not NEWS_DIR.exists():
            log.warning(f"News directory tidak ditemukan: {NEWS_DIR}")
        else:
            for jsonl_file in sorted(NEWS_DIR.rglob("*.jsonl")):
                try:
                    with open(jsonl_file, encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if line:
                                all_articles.append(json.loads(line))
                except Exception as e:
                    log.warning(f"Gagal membaca {jsonl_file}: {e}")

    log.info(f"Total artikel dimuat: {len(all_articles)}")

    # Ekstrak sinyal
    signals: list[dict] = []
    for art in all_articles:
        sig = extract_signals(art)
        if sig:
            signals.append(sig)

    log.info(f"Sinyal diekstrak: {len(signals)} dari {len(all_articles)} artikel")

    # Hitung velocity
    velocity_data = compute_velocity(signals)

    # Ringkasan output
    output: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_articles_processed": len(all_articles),
        "total_signals_extracted":  len(signals),
        "velocity_per_komoditas":   velocity_data,
        "recent_signals":           signals[-50:],  # 50 sinyal terbaru untuk dashboard
    }

    # Simpan ke export
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = EXPORT_DIR / "nlp_signals.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)

    log.info(f"Disimpan ke {out_path}")
    return output


if __name__ == "__main__":
    result = run_extraction()
    print(f"\n=== NLP Signal Summary ===")
    for kom, data in result["velocity_per_komoditas"].items():
        print(
            f"  {kom:25s} | today={data['today_count']:3d} "
            f"| velocity={data['velocity']:.2f} "
            f"| avg_score={data['avg_score']:.1f}"
        )
    print(f"\nTotal sinyal: {result['total_signals_extracted']}")
    sys.exit(0)
