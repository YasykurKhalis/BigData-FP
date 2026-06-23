"""
LUMBUNG — Generasi Rekomendasi via Gemini API
Owner: Hanif

Membaca risk_index.json + price_forecast.json + nlp_signals.json,
lalu memanggil Gemini untuk menghasilkan rekomendasi tindakan:
  - Untuk Pemerintah/TPID: kapan dan komoditas apa untuk operasi pasar
  - Untuk Pedagang/UMKM: strategi stok dan penetapan HPP

Output: temp_buffer/export/recommendations.json
"""

from __future__ import annotations
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("recommendation_llm")

BASE_DIR   = Path(__file__).resolve().parent.parent
EXPORT_DIR = BASE_DIR / "temp_buffer" / "export"

KOMODITAS_LABEL = {
    "beras":             "Beras",
    "cabai_rawit_merah": "Cabai Rawit Merah",
    "cabai_keriting":    "Cabai Merah Keriting",
    "bawang_merah":      "Bawang Merah",
    "bawang_putih":      "Bawang Putih",
}

LEVEL_LABEL = {
    "AMAN":    "aman",
    "WASPADA": "waspada",
    "SIAGA":   "siaga",
    "KRITIS":  "kritis",
}


# ── Loader ────────────────────────────────────────────────────────────────────

def _load_json(path: Path) -> Any:
    if not path.exists():
        log.warning(f"File tidak ditemukan: {path}")
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ── Prompt Builder ────────────────────────────────────────────────────────────

def build_context_summary(
    risk_data:     dict,
    forecast_data: dict,
    nlp_data:      dict,
) -> str:
    """Rangkum kondisi terkini menjadi teks konteks untuk LLM."""
    lines = [
        "=== RINGKASAN KONDISI PANGAN TERKINI ===",
        f"Tanggal analisis: {datetime.now(timezone.utc).strftime('%d %B %Y')}",
        "",
        "--- Indeks Risiko Lonjakan Harga ---",
    ]

    risk_indices = risk_data.get("risk_indices", {})
    for komoditas, data in risk_indices.items():
        label = KOMODITAS_LABEL.get(komoditas, komoditas)
        comp  = data.get("components", {})
        lines.append(
            f"• {label}: {data['risk_index']:.1f}/100 [{data['level']}] "
            f"(harga={comp.get('price_signal',0):.1f}, "
            f"cuaca={comp.get('weather_signal',0):.1f}, "
            f"berita={comp.get('news_signal',0):.1f})"
        )

    lines += ["", "--- Prediksi Harga 7 Hari ke Depan ---"]
    forecasts = forecast_data.get("forecasts", {}) if forecast_data else {}
    for komoditas, data in forecasts.items():
        label = KOMODITAS_LABEL.get(komoditas, komoditas)
        chg   = data.get("predicted_change_pct", 0)
        curr  = data.get("current_price", 0)
        fcast = data.get("forecast_price_7d", 0)
        arrow = "↑" if chg > 0 else ("↓" if chg < 0 else "→")
        lines.append(
            f"• {label}: Rp{curr:,.0f} {arrow} Rp{fcast:,.0f} ({chg:+.1f}%)"
        )

    if nlp_data:
        velocity = nlp_data.get("velocity_per_komoditas", {})
        high_vel = [
            (KOMODITAS_LABEL.get(k, k), v["velocity"])
            for k, v in velocity.items()
            if v.get("velocity", 0) >= 2.0
        ]
        if high_vel:
            lines += ["", "--- Sinyal Berita Signifikan (velocity ≥ 2×) ---"]
            for label, vel in high_vel:
                lines.append(f"• {label}: velocity={vel:.1f}x rata-rata 7 hari")

    return "\n".join(lines)


def build_prompt_government(context: str, high_risk: list[dict]) -> str:
    """Buat prompt untuk rekomendasi pemerintah/TPID."""
    komoditas_kritis = ", ".join(
        KOMODITAS_LABEL.get(r["komoditas"], r["komoditas"])
        for r in high_risk
    ) or "tidak ada komoditas kritis saat ini"

    return f"""Kamu adalah analis ketahanan pangan senior yang memberikan rekomendasi kepada Tim Pengendalian Inflasi Daerah (TPID) dan Bapanas.

{context}

Komoditas dengan risiko tinggi/kritis: {komoditas_kritis}

Berikan REKOMENDASI TINDAKAN KONKRET untuk pemerintah/TPID mencakup:
1. Komoditas mana yang perlu operasi pasar segera (dan estimasi volumenya)
2. Waktu terbaik untuk intervensi distribusi stok Bulog
3. Monitoring khusus sentra produksi yang berisiko
4. Pesan kunci untuk komunikasi publik agar tidak memicu panic buying

Format respons: poin-poin singkat, bahasa formal, fokus pada tindakan yang bisa langsung dieksekusi.
Maksimal 200 kata."""


def build_prompt_umkm(context: str, high_risk: list[dict]) -> str:
    """Buat prompt untuk rekomendasi pedagang/UMKM kuliner."""
    komoditas_kritis = ", ".join(
        KOMODITAS_LABEL.get(r["komoditas"], r["komoditas"])
        for r in high_risk
    ) or "kondisi harga relatif stabil"

    return f"""Kamu adalah konsultan bisnis kuliner yang membantu pedagang dan pelaku UMKM makanan menghadapi volatilitas harga bahan baku.

{context}

Komoditas berisiko naik: {komoditas_kritis}

Berikan SARAN PRAKTIS untuk pedagang/UMKM kuliner mencakup:
1. Komoditas mana yang sebaiknya distok lebih awal (dan berapa hari ke depan)
2. Cara menyesuaikan Harga Pokok Penjualan (HPP) menu secara bertahap
3. Alternatif bahan baku jika harga komoditas utama melonjak
4. Tips negosiasi dengan supplier di tengah kenaikan harga

Format respons: poin-poin singkat, bahasa informal/ramah, fokus pada langkah praktis.
Maksimal 200 kata."""


# ── Gemini API Call ───────────────────────────────────────────────────────────

def call_gemini(prompt: str) -> str | None:
    """
    Panggil Gemini API menggunakan google-genai.
    Membutuhkan GEMINI_API_KEY di environment variable.
    """
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        log.warning("GEMINI_API_KEY tidak ditemukan di environment. Skip API call.")
        return None

    try:
        from google import genai
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model="gemini-2.0-flash",
            contents=prompt,
        )
        return response.text
    except Exception as e:
        log.error(f"Gemini API error: {type(e).__name__}: {e}")
        return None


def generate_fallback_recommendation(
    context_summary: str,
    audience: str,
    high_risk: list[dict],
) -> str:
    """
    Rekomendasi template jika Gemini API tidak tersedia.
    Berbasis aturan sederhana dari data risk index.
    """
    if not high_risk:
        if audience == "government":
            return (
                "Kondisi harga pangan saat ini dalam batas aman. "
                "Disarankan untuk tetap memantau perkembangan cuaca di sentra produksi "
                "dan melakukan pembaruan data stok Bulog secara rutin."
            )
        else:
            return (
                "Harga bahan baku relatif stabil minggu ini. "
                "Pertahankan level stok normal dan manfaatkan momentum ini "
                "untuk membangun buffer stok kecil sebagai antisipasi."
            )

    kritis_labels = [
        KOMODITAS_LABEL.get(r["komoditas"], r["komoditas"])
        for r in high_risk
        if r["level"] in ("KRITIS", "SIAGA")
    ]
    waspada_labels = [
        KOMODITAS_LABEL.get(r["komoditas"], r["komoditas"])
        for r in high_risk
        if r["level"] == "WASPADA"
    ]

    if audience == "government":
        lines = ["**Rekomendasi untuk TPID/Bapanas:**"]
        if kritis_labels:
            lines.append(
                f"⚠️ PRIORITAS TINGGI: {', '.join(kritis_labels)} menunjukkan sinyal risiko "
                f"kritis. Rekomendasikan koordinasi operasi pasar dalam 3-5 hari ke depan."
            )
        if waspada_labels:
            lines.append(
                f"• Monitor: {', '.join(waspada_labels)} memasuki zona waspada. "
                f"Siapkan distribusi stok cadangan dan koordinasi dengan distributor daerah."
            )
        lines.append(
            "• Aktifkan sistem monitoring harga harian di pasar induk utama "
            "dan pantau laporan cuaca sentra produksi selama 7 hari ke depan."
        )
    else:
        lines = ["**Saran untuk Pedagang & UMKM Kuliner:**"]
        if kritis_labels:
            lines.append(
                f"🔴 Pertimbangkan stok tambahan {', '.join(kritis_labels)} untuk 7-10 hari "
                f"ke depan sebelum harga melonjak lebih tinggi."
            )
        if waspada_labels:
            lines.append(
                f"🟡 {', '.join(waspada_labels)}: Pantau harga pasar harian dan mulai "
                f"kalkukasi ulang HPP menu yang menggunakan bahan ini."
            )
        lines.append(
            "• Pertimbangkan menu alternatif atau ukuran porsi yang lebih fleksibel "
            "sebagai strategi buffer terhadap kenaikan harga bahan baku."
        )

    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def run_recommendation() -> dict[str, Any]:
    """
    Baca semua data, buat konteks, panggil Gemini (atau fallback),
    simpan ke export/recommendations.json.
    """
    risk_data     = _load_json(EXPORT_DIR / "risk_index.json")     or {}
    forecast_data = _load_json(EXPORT_DIR / "price_forecast.json") or {}
    nlp_data      = _load_json(EXPORT_DIR / "nlp_signals.json")    or {}

    if not risk_data:
        log.warning("risk_index.json kosong. Rekomendasi mungkin kurang akurat.")

    # Susun ringkasan konteks
    context_summary = build_context_summary(risk_data, forecast_data, nlp_data)

    # Identifikasi komoditas berisiko tinggi
    high_risk = [
        {"komoditas": k, "level": v["level"], "index": v["risk_index"]}
        for k, v in risk_data.get("risk_indices", {}).items()
        if v.get("level") in ("WASPADA", "SIAGA", "KRITIS")
    ]
    high_risk.sort(key=lambda x: x["index"], reverse=True)

    log.info(f"Komoditas berisiko tinggi: {[r['komoditas'] for r in high_risk]}")

    # Generate rekomendasi pemerintah
    prompt_gov = build_prompt_government(context_summary, high_risk)
    log.info("Meminta rekomendasi untuk pemerintah...")
    rec_government = call_gemini(prompt_gov) or generate_fallback_recommendation(
        context_summary, "government", high_risk
    )

    # Generate rekomendasi UMKM
    prompt_umkm = build_prompt_umkm(context_summary, high_risk)
    log.info("Meminta rekomendasi untuk UMKM...")
    rec_umkm = call_gemini(prompt_umkm) or generate_fallback_recommendation(
        context_summary, "umkm", high_risk
    )

    output: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "context_summary": context_summary,
        "high_risk_komoditas": high_risk,
        "recommendations": {
            "government": rec_government,
            "umkm": rec_umkm,
        },
        "gemini_used": bool(os.getenv("GEMINI_API_KEY")),
    }

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = EXPORT_DIR / "recommendations.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info(f"Rekomendasi disimpan ke {out_path}")
    return output


if __name__ == "__main__":
    result = run_recommendation()
    print("\n" + "=" * 60)
    print("REKOMENDASI PEMERINTAH/TPID:")
    print("=" * 60)
    print(result["recommendations"]["government"])
    print("\n" + "=" * 60)
    print("REKOMENDASI PEDAGANG/UMKM KULINER:")
    print("=" * 60)
    print(result["recommendations"]["umkm"])
    sys.exit(0)
