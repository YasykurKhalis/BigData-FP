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
  GET /berita               → Berita pangan terkini (RSS + NLP)
  GET /kurs                 → Kurs USD/IDR + dampak komoditas
  GET /harga_live           → Harga live 10 komoditas
  GET /prediksi             → Prediksi detail per komoditas

  API:
  GET /api/risk_index       → JSON risk index semua komoditas
  GET /api/alerts           → JSON alert aktif
  GET /api/forecast         → JSON prediksi harga 7 hari
  GET /api/feature_store    → JSON feature store (sample)
  GET /api/nlp_signals      → JSON sinyal NLP
  GET /api/evaluation       → JSON metrik evaluasi
  GET /api/recommendations  → JSON rekomendasi LLM
  GET /api/live_news        → JSON berita live
  GET /api/live_kurs        → JSON kurs live
  GET /api/live_prices      → JSON harga live
  GET /api/prediksi         → JSON prediksi gabungan
"""

from __future__ import annotations
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import Flask, jsonify, render_template, abort, request
from flask_cors import CORS

try:
    import feedparser
except ImportError:
    feedparser = None

try:
    import requests as http_requests
except ImportError:
    http_requests = None

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


def _get_pihps_latest() -> dict:
    """Ambil harga terbaru per komoditas dari pihps_realdata.json."""
    path = BASE_DIR / "data" / "pihps_realdata.json"
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
        result = {}
        for kom, info in raw.items():
            if kom.startswith("_"):
                continue
            entries = info.get("data", [])
            if entries:
                result[kom] = entries[-1].get("nominal")
        return result
    except Exception:
        return {}


def _get_current_prices() -> dict:
    """Harga terkini semua komoditas dari PIHPS + forecast."""
    forecast = _get_forecast()
    pihps = _get_pihps_latest()
    prices = {}

    # Dari forecast (punya current_price)
    for kom, fc in forecast.get("forecasts", {}).items():
        # Prefer PIHPS real price over ML-averaged price
        real_price = pihps.get(kom) or fc.get("current_price")
        # Use ML's predicted change but cap to realistic range (±10%)
        change_pct = fc.get("predicted_change_pct", 0) or 0
        change_pct = max(-10, min(10, change_pct))
        change_pct = round(change_pct, 2)
        # Derive forecast from PIHPS price + capped ML change
        if real_price:
            forecast_7d = round(real_price * (1 + change_pct / 100))
        else:
            forecast_7d = fc.get("forecast_7d")
        prices[kom] = {
            "label": KOMODITAS_LABEL.get(kom, kom),
            "price": real_price,
            "forecast_7d": forecast_7d,
            "change_pct": change_pct,
        }

    # Isi komoditas yang belum ada dari feature_store
    data = _read_json("feature_store.json", [])
    if isinstance(data, list):
        for record in reversed(data):
            kom = record.get("komoditas", "")
            if kom and kom in KOMODITAS_LABEL and kom not in prices:
                prices[kom] = {
                    "label": KOMODITAS_LABEL.get(kom, kom),
                    "price": record.get("avg_price"),
                    "forecast_7d": None,
                    "change_pct": None,
                }

    return prices


def _get_kurs() -> dict:
    """Ambil kurs IDR/USD terbaru dari feature_store."""
    data = _read_json("feature_store.json", [])
    if isinstance(data, list):
        for record in reversed(data):
            kurs = record.get("kurs_usd_idr")
            if kurs and str(kurs) != "nan" and str(kurs) != "NaN":
                try:
                    return {"rate": round(float(kurs), 2), "date": record.get("date_parsed", "")}
                except (ValueError, TypeError):
                    pass
    # Fallback: baca dari silver_kurs delta table
    try:
        import sys as _sys
        _sys.path.insert(0, str(BASE_DIR / "lakehouse"))
        from utils import read_delta, SILVER_DIR
        df = read_delta(str(SILVER_DIR / "silver_kurs"))
        if not df.empty and "kurs_tengah" in df.columns:
            df = df.sort_values("date_parsed", ascending=False)
            row = df.iloc[0]
            return {"rate": round(float(row["kurs_tengah"]), 2), "date": str(row.get("date_parsed", ""))}
    except Exception as e:
        log.warning(f"Gagal baca kurs: {e}")
    return {"rate": None, "date": ""}


def _get_news_articles() -> list:
    """Ambil berita terkini dari nlp_signals.json (recent_signals)."""
    nlp = _get_nlp_signals()
    return nlp.get("recent_signals", [])


def _get_price_history_latest() -> dict:
    """Ambil harga historis 30 hari terakhir per komoditas untuk chart."""
    data = _read_json("feature_store.json", [])
    history = {}
    if isinstance(data, list):
        for record in data:
            kom = record.get("komoditas", "")
            if kom:
                if kom not in history:
                    history[kom] = []
                history[kom].append({
                    "date": record.get("date_parsed", ""),
                    "price": record.get("avg_price"),
                })
        # Ambil 30 hari terakhir per komoditas
        for kom in history:
            history[kom] = history[kom][-30:]
    return history


def _now_label() -> str:
    return datetime.now(timezone.utc).strftime("%d %B %Y, %H:%M UTC")


def _fetch_live_news() -> list:
    """Ambil berita live dari RSS feeds, fallback ke nlp_signals.json."""
    articles = []

    # Coba RSS feeds
    if feedparser:
        feeds = [
            ("Kompas", "https://rss.kompas.com/bisnis"),
            ("Tempo", "https://rss.tempo.co/teco/bisnis"),
            ("Antara", "https://www.antaranews.com/rss/ekonomi-bisnis.xml"),
        ]
        for source, url in feeds:
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:5]:
                    articles.append({
                        "title": entry.get("title", ""),
                        "link": entry.get("link", ""),
                        "source": source,
                        "published": entry.get("published", ""),
                        "summary": entry.get("summary", "")[:200] if entry.get("summary") else "",
                    })
            except Exception as e:
                log.warning(f"Gagal fetch RSS {source}: {e}")

    # Fallback ke nlp_signals.json jika tidak ada hasil
    if not articles:
        nlp = _get_nlp_signals()
        for sig in nlp.get("recent_signals", []):
            articles.append({
                "title": sig.get("title", ""),
                "link": sig.get("url", ""),
                "source": sig.get("source", "NLP Pipeline"),
                "published": sig.get("date", ""),
                "summary": sig.get("snippet", ""),
                "komoditas_matched": sig.get("komoditas_matched", []),
                "sentiment": sig.get("sentiment"),
                "signal_score": sig.get("signal_score"),
            })

    return articles


def _fetch_live_kurs() -> dict:
    """Ambil kurs live dari exchangerate-api, fallback ke feature_store."""
    # Coba API publik
    if http_requests:
        try:
            resp = http_requests.get(
                "https://open.er-api.com/v6/latest/USD", timeout=5
            )
            if resp.status_code == 200:
                data = resp.json()
                rate = data.get("rates", {}).get("IDR")
                if rate:
                    return {
                        "rate": round(float(rate), 2),
                        "date": data.get("time_last_update_utc", ""),
                        "source": "ExchangeRate API (Live)",
                        "change_pct": None,
                    }
        except Exception as e:
            log.warning(f"Gagal fetch kurs live: {e}")

    # Fallback ke feature_store
    kurs = _get_kurs()
    kurs["source"] = "Feature Store"
    kurs["change_pct"] = None
    return kurs


def _fetch_live_prices() -> dict:
    """Ambil harga live dari PIHPS realdata (sumber utama)."""
    pihps_path = BASE_DIR / "data" / "pihps_realdata.json"
    prices = {}

    if pihps_path.exists():
        try:
            with open(pihps_path, encoding="utf-8") as f:
                raw = json.load(f)
            for kom, info in raw.items():
                if kom.startswith("_"):
                    continue
                entries = info.get("data", [])
                if not entries:
                    continue
                price_now = entries[-1].get("nominal")
                change_1d = None
                change_7d = None
                # 1-day: cari entry terakhir dengan harga berbeda
                for i in range(len(entries) - 2, -1, -1):
                    prev = entries[i].get("nominal")
                    if prev and price_now and prev != price_now and prev > 0:
                        change_1d = round((price_now - prev) / prev * 100, 2)
                        break
                if change_1d is None and len(entries) >= 2:
                    prev = entries[-2].get("nominal")
                    if prev and price_now and prev > 0:
                        change_1d = round((price_now - prev) / prev * 100, 2)
                # 7-day
                if len(entries) >= 7:
                    prev7 = entries[-7].get("nominal")
                    if prev7 and price_now and prev7 > 0:
                        change_7d = round((price_now - prev7) / prev7 * 100, 2)
                elif len(entries) >= 2:
                    prev7 = entries[0].get("nominal")
                    if prev7 and price_now and prev7 > 0:
                        change_7d = round((price_now - prev7) / prev7 * 100, 2)
                prices[kom] = {
                    "price": price_now,
                    "change_1d": change_1d,
                    "change_7d": change_7d,
                    "source": "PIHPS BI",
                }
        except Exception:
            pass

    # Fallback jika PIHPS kosong
    if not prices:
        cp = _get_current_prices()
        for kom, data in cp.items():
            prices[kom] = {
                "price": data.get("price"),
                "change_1d": None,
                "change_7d": data.get("change_pct"),
                "source": "Forecast Export",
            }

    return prices


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

    # Override risk level berdasarkan prediksi perubahan harga (PIHPS-based)
    cur_prices = _get_current_prices()
    for kom, cp in cur_prices.items():
        chg = cp.get("change_pct")
        if chg is not None and kom in risk_indices:
            if chg > 7:
                risk_indices[kom]["level"] = "KRITIS"
            elif chg > 5:
                risk_indices[kom]["level"] = "SIAGA"
            elif chg > 3:
                risk_indices[kom]["level"] = "WASPADA"
            elif chg <= 0:
                # Harga turun / stabil = aman untuk konsumen
                risk_indices[kom]["level"] = "AMAN"

    # Update forecast change_pct agar konsisten dengan PIHPS
    forecasts_view = dict(forecast.get("forecasts", {}))
    for kom, cp in cur_prices.items():
        if kom in forecasts_view:
            forecasts_view[kom] = dict(forecasts_view[kom])
            forecasts_view[kom]["predicted_change_pct"] = cp.get("change_pct")

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
        alerts=alerts[:5],
        summary_stats=summary_stats,
        forecast=forecasts_view,
        komoditas_label=KOMODITAS_LABEL,
        current_prices=cur_prices,
        kurs=_get_kurs(),
        news_articles=_get_news_articles(),
        nlp_signals=_get_nlp_signals(),
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
    pihps        = _get_pihps_latest()

    # Siapkan data per komoditas untuk template
    komoditas_data = {}
    for kom, label in KOMODITAS_LABEL.items():
        fc  = forecasts.get(kom, {})
        mod = models.get(kom, {})
        ri  = risk_indices.get(kom, {})
        timeline = fc.get("forecast_timeline", [])
        komoditas_data[kom] = {
            "label":                label,
            "current_price":        pihps.get(kom) or fc.get("current_price"),
            "forecast_price_7d":    fc.get("forecast_7d"),
            "predicted_change_pct": fc.get("predicted_change_pct") or fc.get("predicted_change_pct_7d"),
            "forecast_timeline":    timeline,
            "chart_labels_json":    json.dumps([t.get("day_label", "") for t in timeline]),
            "chart_values_json":    json.dumps([t.get("predicted_price", 0) for t in timeline]),
            "risk_index":           ri.get("risk_index"),
            "risk_level":           ri.get("level"),
            "mape":                 mod.get("metrics", {}).get("mape_avg"),
            "n_samples":            mod.get("metrics", {}).get("n_samples"),
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


@app.route("/berita")
def berita():
    articles = _fetch_live_news()
    return render_template(
        "berita.html",
        articles=articles,
        komoditas_label=KOMODITAS_LABEL,
        updated_at=_now_label(),
    )


@app.route("/kurs")
def kurs():
    kurs_data = _fetch_live_kurs()

    # Historical kurs dari feature_store
    kurs_history = []
    data = _read_json("feature_store.json", [])
    if isinstance(data, list):
        seen_dates = set()
        for record in data:
            k = record.get("kurs_usd_idr")
            d = record.get("date_parsed", "")
            if k and d and str(k) not in ("nan", "NaN") and d not in seen_dates:
                try:
                    kurs_history.append({"date": d, "rate": round(float(k), 2)})
                    seen_dates.add(d)
                except (ValueError, TypeError):
                    pass
        kurs_history.sort(key=lambda x: x["date"])
        kurs_history = kurs_history[-60:]  # 60 hari terakhir

    return render_template(
        "kurs.html",
        kurs=kurs_data,
        kurs_history=kurs_history,
        updated_at=_now_label(),
    )


@app.route("/harga_live")
def harga_live():
    prices = _fetch_live_prices()
    price_history = _get_price_history_latest()
    return render_template(
        "harga_live.html",
        prices=prices,
        price_history=price_history,
        komoditas_label=KOMODITAS_LABEL,
        updated_at=_now_label(),
    )


@app.route("/prediksi")
def prediksi():
    forecast = _get_forecast()
    magnitude = _read_json("magnitude_estimate.json", {})
    timing = _read_json("timing_prediction.json", {})
    risk_data = _get_risk_data()
    fi_data = _get_feature_importance()

    forecasts = forecast.get("forecasts", {})
    risk_indices = risk_data.get("risk_indices", {})
    models = fi_data.get("models", {})
    pihps = _get_pihps_latest()

    combined = {}
    naik = turun = stabil = 0
    for kom, label in KOMODITAS_LABEL.items():
        fc = forecasts.get(kom, {})
        mag = magnitude.get("estimates", {}).get(kom, {})
        tim = timing.get(kom, {})
        ri = risk_indices.get(kom, {})
        mod = models.get(kom, {})

        # Use capped ML change applied to PIHPS price
        raw_chg = fc.get("predicted_change_pct", 0) or 0
        change_pct = max(-10, min(10, raw_chg))
        real_price = pihps.get(kom) or fc.get("current_price")
        forecast_7d = round(real_price * (1 + change_pct / 100)) if real_price else fc.get("forecast_7d")

        if change_pct and change_pct > 1:
            naik += 1
        elif change_pct and change_pct < -1:
            turun += 1
        else:
            stabil += 1

        timeline = fc.get("forecast_timeline", [])
        forecast_14d = None
        if len(timeline) >= 7:
            last_price = timeline[-1].get("predicted_price", 0)
            daily_change = (last_price - (fc.get("current_price") or last_price)) / 7 if fc.get("current_price") else 0
            forecast_14d = round(last_price + daily_change * 7) if daily_change else None

        mape = mod.get("metrics", {}).get("mape_avg")
        confidence = round(100 - min(mape, 50), 1) if mape else None

        triggers = []
        for feat in mod.get("top_3_features", []):
            triggers.append(feat.replace("_", " ").title())
        if ri.get("level") in ("KRITIS", "SIAGA"):
            triggers.append(f"Risk Level: {ri.get('level')}")

        combined[kom] = {
            "current_price": real_price,
            "forecast_price_7d": forecast_7d,
            "forecast_price_14d": forecast_14d,
            "predicted_change_pct": change_pct,
            "forecast_timeline": timeline,
            "magnitude": {
                "low": mag.get("estimated_increase_rpkg", {}).get("p10", 0),
                "high": mag.get("estimated_increase_rpkg", {}).get("p90", 0),
                "confidence": round((mag.get("spike_probability", 0) or 0) * 100),
            } if mag else None,
            "timing": tim if tim else None,
            "confidence": confidence,
            "triggers": triggers if triggers else None,
        }

    return render_template(
        "prediksi.html",
        forecasts=combined,
        naik_count=naik,
        turun_count=turun,
        stabil_count=stabil,
        komoditas_label=KOMODITAS_LABEL,
        updated_at=_now_label(),
    )


@app.route("/api/prediksi")
def api_prediksi():
    forecast = _get_forecast()
    magnitude = _read_json("magnitude_estimate.json", {})
    timing = _read_json("timing_prediction.json", {})
    return jsonify({
        "forecasts": forecast.get("forecasts", {}),
        "magnitude": magnitude,
        "timing": timing,
    })


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


# -- Entrypoint --

if __name__ == "__main__":
    log.info(f"EXPORT_DIR: {EXPORT_DIR}")
    log.info("Starting LUMBUNG Dashboard on http://0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
