"""
LUMBUNG — Flask Dashboard
Owner: Hanif

Membaca JSON export dari temp_buffer/export/ dan menyajikannya
melalui Flask dengan API JSON dan template HTML.

Routes:
  GET /                     → Halaman utama (ringkasan + alert)
  GET /komoditas            → Tren harga per komoditas
  GET /peta_sentra          → Peta sentra produksi
  GET /rekomendasi          → Rekomendasi tindakan
  GET /evaluasi             → Evaluasi model
  GET /lakehouse            → Demo Delta Lake Time Travel

  API:
  GET /api/risk_index       → JSON risk index semua komoditas
  GET /api/alerts           → JSON alert aktif
  GET /api/forecast         → JSON prediksi harga 7 hari
  GET /api/feature_store    → JSON feature store (sample)
  GET /api/nlp_signals      → JSON sinyal NLP
  GET /api/evaluation       → JSON metrik evaluasi
  GET /api/recommendations  → JSON rekomendasi LLM
"""

from __future__ import annotations
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template, abort
from flask_cors import CORS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("dashboard")

app = Flask(__name__)
CORS(app)

BASE_DIR   = Path(__file__).resolve().parent.parent
EXPORT_DIR = BASE_DIR / "temp_buffer" / "export"

KOMODITAS_LABEL = {
    "beras":             "Beras",
    "cabai_rawit_merah": "Cabai Rawit Merah",
    "cabai_keriting":    "Cabai Merah Keriting",
    "bawang_merah":      "Bawang Merah",
    "bawang_putih":      "Bawang Putih",
    "gula_pasir":        "Gula Pasir",
    "minyak_goreng":     "Minyak Goreng",
    "daging_ayam":       "Daging Ayam",
    "telur_ayam":        "Telur Ayam",
    "daging_sapi":       "Daging Sapi",
}


# ── Helper ────────────────────────────────────────────────────────────────────

def _read_json(filename: str, default: Any = None) -> Any:
    """Baca file JSON dari EXPORT_DIR. Kembalikan default jika tidak ada."""
    path = EXPORT_DIR / filename
    if not path.exists():
        return default
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.error(f"Gagal membaca {path}: {e}")
        return default


def _get_risk_data() -> dict:
    return _read_json("risk_index.json", {})


def _get_alerts() -> dict:
    return _read_json("alerts.json", {"alerts": [], "total_alerts": 0})


def _get_forecast() -> dict:
    return _read_json("price_forecast.json", {})


def _get_feature_importance() -> dict:
    return _read_json("feature_importance.json", {})


def _get_nlp_signals() -> dict:
    return _read_json("nlp_signals.json", {})


def _get_evaluation() -> dict:
    return _read_json("evaluation.json", {})


def _get_recommendations() -> dict:
    return _read_json("recommendations.json", {})


def _get_feature_store_sample(n: int = 200) -> list:
    """Kembalikan N baris terakhir dari feature_store.json."""
    data = _read_json("feature_store.json", [])
    if isinstance(data, list):
        return data[-n:]
    return []


def _now_label() -> str:
    return datetime.now(timezone.utc).strftime("%d %B %Y, %H:%M UTC")


# ── HTML Routes ───────────────────────────────────────────────────────────────

@app.route("/")
def index():
    risk_data   = _get_risk_data()
    alerts_data = _get_alerts()
    forecast    = _get_forecast()

    risk_indices = risk_data.get("risk_indices", {})
    alerts       = alerts_data.get("alerts", [])

    # Tambahkan label ke risk indices
    for kom, data in risk_indices.items():
        data["label"] = KOMODITAS_LABEL.get(kom, kom)

    # Sinkronkan badge risk card dengan alert level (alert engine lebih akurat
    # karena mempertimbangkan forecast ML, sedangkan risk_index bisa stale)
    for alert in alerts:
        kom = alert.get("komoditas", "")
        alert_level = alert.get("level", "")
        if kom in risk_indices and alert_level:
            current_level = risk_indices[kom].get("level", "AMAN")
            level_order = {"AMAN": 0, "WASPADA": 1, "SIAGA": 2, "KRITIS": 3}
            if level_order.get(alert_level, 0) > level_order.get(current_level, 0):
                risk_indices[kom]["level"] = alert_level

    # Hitung statistik ringkasan
    summary_stats = {
        "total_komoditas": len(risk_indices),
        "kritis":  sum(1 for d in risk_indices.values() if d.get("level") == "KRITIS"),
        "siaga":   sum(1 for d in risk_indices.values() if d.get("level") == "SIAGA"),
        "waspada": sum(1 for d in risk_indices.values() if d.get("level") == "WASPADA"),
        "aman":    sum(1 for d in risk_indices.values() if d.get("level") == "AMAN"),
    }

    return render_template(
        "index.html",
        risk_indices=risk_indices,
        alerts=alerts[:5],  # tampilkan 5 alert teratas
        summary_stats=summary_stats,
        forecast=forecast.get("forecasts", {}),
        komoditas_label=KOMODITAS_LABEL,
        updated_at=_now_label(),
    )


@app.route("/komoditas")
def komoditas():
    forecast    = _get_forecast()
    fi_data     = _get_feature_importance()
    risk_data   = _get_risk_data()

    forecasts    = forecast.get("forecasts", {})
    models       = fi_data.get("models", {})
    risk_indices = risk_data.get("risk_indices", {})

    # Siapkan data per komoditas untuk template
    komoditas_data = {}
    for kom, label in KOMODITAS_LABEL.items():
        fc  = forecasts.get(kom, {})
        mod = models.get(kom, {})
        ri  = risk_indices.get(kom, {})
        komoditas_data[kom] = {
            "label":                label,
            "current_price":        fc.get("current_price"),
            "forecast_price_7d":    fc.get("forecast_price_7d"),
            "predicted_change_pct": fc.get("predicted_change_pct"),
            "forecast_timeline":    fc.get("forecast_timeline", []),
            "risk_index":           ri.get("risk_index"),
            "risk_level":           ri.get("level"),
            "mape":                 mod.get("metrics", {}).get("mape_avg"),
            "top_features":         mod.get("top_3_features", []),
        }

    return render_template(
        "komoditas.html",
        komoditas_data=komoditas_data,
        komoditas_label=KOMODITAS_LABEL,
        updated_at=_now_label(),
    )


@app.route("/peta_sentra")
def peta_sentra():
    risk_data = _get_risk_data()
    risk_indices = risk_data.get("risk_indices", {})

    # Import sentra mapper
    try:
        import sys
        sys.path.insert(0, str(BASE_DIR / "ml"))
        from sentra_mapper import get_all_sentra_flat
        sentra_list = get_all_sentra_flat()
    except Exception as e:
        log.warning(f"Gagal import sentra_mapper: {e}")
        sentra_list = []

    # Gabungkan dengan risk level
    for s in sentra_list:
        kom = s.get("komoditas", "")
        ri  = risk_indices.get(kom, {})
        s["risk_index"] = ri.get("risk_index", 0)
        s["risk_level"] = ri.get("level", "AMAN")

    return render_template(
        "peta_sentra.html",
        sentra_list=sentra_list,
        sentra_json=json.dumps(sentra_list, ensure_ascii=False),
        komoditas_label=KOMODITAS_LABEL,
        updated_at=_now_label(),
    )


@app.route("/rekomendasi")
def rekomendasi():
    rec_data  = _get_recommendations()
    risk_data = _get_risk_data()

    high_risk = rec_data.get("high_risk_komoditas", [])
    for r in high_risk:
        r["label"] = KOMODITAS_LABEL.get(r.get("komoditas", ""), r.get("komoditas", ""))

    return render_template(
        "rekomendasi.html",
        rec_government=rec_data.get("recommendations", {}).get("government", ""),
        rec_umkm=rec_data.get("recommendations", {}).get("umkm", ""),
        context_summary=rec_data.get("context_summary", ""),
        high_risk=high_risk,
        gemini_used=rec_data.get("gemini_used", False),
        updated_at=_now_label(),
    )


@app.route("/evaluasi")
def evaluasi():
    eval_data   = _get_evaluation()
    fi_data     = _get_feature_importance()

    per_komoditas = eval_data.get("per_komoditas", {})
    summary       = eval_data.get("summary", {})
    models        = fi_data.get("models", {})

    # Gabungkan feature importance ke hasil evaluasi
    eval_combined = {}
    for kom, label in KOMODITAS_LABEL.items():
        ev  = per_komoditas.get(kom, {})
        mod = models.get(kom, {})
        eval_combined[kom] = {
            "label":              label,
            "baseline_mape":      ev.get("baseline", {}).get("mape"),
            "baseline_da":        ev.get("baseline", {}).get("da"),
            "xgb_mape":           ev.get("xgboost", {}).get("mape"),
            "current_risk_index": ev.get("current_risk_index"),
            "feature_importance": mod.get("feature_importance", {}),
            "top_3_features":     mod.get("top_3_features", []),
            "n_samples":          ev.get("n_samples"),
        }

    return render_template(
        "evaluasi.html",
        eval_combined=eval_combined,
        summary=summary,
        komoditas_label=KOMODITAS_LABEL,
        updated_at=_now_label(),
    )


@app.route("/lakehouse")
def lakehouse():
    """Halaman demo Delta Lake Time Travel."""
    feature_store = _get_feature_store_sample(n=50)
    fi_meta = _get_feature_importance()
    generated_at = fi_meta.get("generated_at", "N/A")

    return render_template(
        "lakehouse.html",
        feature_store_sample=feature_store[:10],
        total_records=len(feature_store),
        generated_at=generated_at,
        updated_at=_now_label(),
    )


# ── JSON API ──────────────────────────────────────────────────────────────────

@app.route("/api/risk_index")
def api_risk_index():
    data = _get_risk_data()
    return jsonify(data)


@app.route("/api/alerts")
def api_alerts():
    data = _get_alerts()
    return jsonify(data)


@app.route("/api/forecast")
def api_forecast():
    data = _get_forecast()
    return jsonify(data)


@app.route("/api/feature_store")
def api_feature_store():
    sample = _get_feature_store_sample(n=100)
    return jsonify({"data": sample, "count": len(sample)})


@app.route("/api/nlp_signals")
def api_nlp_signals():
    data = _get_nlp_signals()
    return jsonify(data)


@app.route("/api/evaluation")
def api_evaluation():
    data = _get_evaluation()
    return jsonify(data)


@app.route("/api/recommendations")
def api_recommendations():
    data = _get_recommendations()
    return jsonify(data)


@app.route("/api/status")
def api_status():
    """Health check + status file export."""
    files = {
        "risk_index":       (EXPORT_DIR / "risk_index.json").exists(),
        "alerts":           (EXPORT_DIR / "alerts.json").exists(),
        "price_forecast":   (EXPORT_DIR / "price_forecast.json").exists(),
        "feature_store":    (EXPORT_DIR / "feature_store.json").exists(),
        "nlp_signals":      (EXPORT_DIR / "nlp_signals.json").exists(),
        "evaluation":       (EXPORT_DIR / "evaluation.json").exists(),
        "recommendations":  (EXPORT_DIR / "recommendations.json").exists(),
    }
    return jsonify({
        "status":      "ok",
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "export_files": files,
        "all_ready":   all(files.values()),
    })


# ── Entrypoint ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info(f"EXPORT_DIR: {EXPORT_DIR}")
    log.info("Starting LUMBUNG Dashboard on http://0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
