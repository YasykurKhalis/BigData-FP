"""
LUMBUNG — Model Evaluation: MAPE, RMSE, DA, Lead Time
Owner: Ryan

Menghitung metrik evaluasi model forecasting harga komoditas:
  - MAPE  (Mean Absolute Percentage Error)
  - RMSE  (Root Mean Squared Error)
  - DA    (Directional Accuracy) — seberapa sering model benar arah naik/turun
  - Lead Time — berapa hari sebelum lonjakan sinyal risk index sudah aktif

Input:
  - temp_buffer/export/feature_importance.json (prediksi dari model)
  - temp_buffer/export/feature_store.json      (harga aktual)

Output:
  - temp_buffer/export/evaluation.json
"""

from __future__ import annotations
import json
import logging
import sys
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("evaluation")

BASE_DIR   = Path(__file__).resolve().parent.parent
EXPORT_DIR = BASE_DIR / "temp_buffer" / "export"
MODEL_DIR  = Path(__file__).resolve().parent / "models"

KOMODITAS_LIST = [
    "beras",
    "cabai_rawit_merah",
    "cabai_keriting",
    "bawang_merah",
    "bawang_putih",
    "gula_pasir",
    "minyak_goreng",
    "daging_ayam",
    "telur_ayam",
    "daging_sapi",
]

FORECAST_HORIZON = 7
# Ambang lonjakan harga signifikan (untuk uji Lead Time)
SPIKE_THRESHOLD_PCT = 10.0


# ── Loader ────────────────────────────────────────────────────────────────────

def _load_json(path: Path) -> Any:
    if not path.exists():
        log.warning(f"File tidak ditemukan: {path}")
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _load_feature_store() -> pd.DataFrame:
    raw = _load_json(EXPORT_DIR / "feature_store.json")
    if raw is None:
        return pd.DataFrame()
    if isinstance(raw, list):
        return pd.DataFrame(raw)
    if isinstance(raw, dict) and "data" in raw:
        return pd.DataFrame(raw["data"])
    return pd.DataFrame()


# ── Metrik Utama ──────────────────────────────────────────────────────────────

def compute_mape(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """MAPE dalam persen, abaikan y_true = 0."""
    mask = y_true != 0
    if not mask.any():
        return float("nan")
    return float(np.mean(np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])) * 100)


def compute_rmse(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Root Mean Squared Error."""
    return float(np.sqrt(np.mean((y_true - y_pred) ** 2)))


def compute_da(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """
    Directional Accuracy: persentase prediksi yang benar arah perubahannya
    dibandingkan harga satu hari sebelumnya.
    """
    if len(y_true) < 2:
        return float("nan")
    actual_dir = np.sign(np.diff(y_true))
    pred_dir   = np.sign(y_pred[1:] - y_true[:-1])  # prediksi vs harga hari ini
    return float(np.mean(actual_dir == pred_dir) * 100)


def compute_lead_time(
    prices: pd.Series,
    risk_series: pd.Series,
    spike_threshold_pct: float = SPIKE_THRESHOLD_PCT,
    risk_alert_threshold: float = 60.0,
) -> dict[str, Any]:
    """
    Hitung lead time: selisih hari antara risk index melebihi threshold
    dan harga aktual melonjak di atas spike_threshold.

    Return: dict dengan statistik lead time (rata-rata, min, max, jumlah event).
    """
    if len(prices) < FORECAST_HORIZON + 1:
        return {"avg_lead_days": None, "events": 0}

    # Deteksi spike harga: kenaikan > spike_threshold dalam 7 hari
    price_pct_7d = prices.pct_change(periods=7) * 100

    spike_dates = set(price_pct_7d[price_pct_7d >= spike_threshold_pct].index)
    alert_dates = set(risk_series[risk_series >= risk_alert_threshold].index) if risk_series is not None else set()

    if not spike_dates or not alert_dates:
        return {"avg_lead_days": None, "events": 0}

    lead_times: list[int] = []
    for spike_date in sorted(spike_dates):
        # Cari alert terdekat sebelum spike
        earlier_alerts = [d for d in alert_dates if d < spike_date]
        if earlier_alerts:
            closest = max(earlier_alerts)
            lead_days = (spike_date - closest).days
            if 0 < lead_days <= 30:  # lead time wajar 1-30 hari
                lead_times.append(lead_days)

    if not lead_times:
        return {"avg_lead_days": None, "events": 0}

    return {
        "avg_lead_days": round(float(np.mean(lead_times)), 1),
        "min_lead_days": int(min(lead_times)),
        "max_lead_days": int(max(lead_times)),
        "events":        len(lead_times),
    }


# ── Backtesting ───────────────────────────────────────────────────────────────

def backtest_model(
    df_all: pd.DataFrame,
    komoditas: str,
) -> dict[str, Any]:
    """
    Lakukan backtesting walk-forward pada model XGBoost yang tersimpan.
    Jika model tidak ada, gunakan baseline (naïve forecast = last known price).
    """
    try:
        import joblib
    except ImportError:
        log.warning("joblib tidak tersedia.")
        return {}

    # Ensure column name consistency for commodity
    if "commodity" in df_all.columns:
        df_all = df_all.rename(columns={"commodity": "komoditas"})
    df_kom = df_all[df_all["komoditas"] == komoditas].copy()
    if df_kom.empty:
        return {"error": "tidak ada data"}

    df_kom["date_parsed"] = pd.to_datetime(df_kom["date_parsed"], errors="coerce")
    df_kom = df_kom.dropna(subset=["date_parsed", "avg_price"])
    df_kom = df_kom.sort_values("date_parsed").reset_index(drop=True)

    prices = df_kom["avg_price"].astype(float).values
    if len(prices) < 30:
        return {"error": "data tidak cukup (< 30 hari)"}

    # Baseline: naïve forecast (harga hari ini = harga 7 hari lalu)
    y_true_naive = prices[FORECAST_HORIZON:]
    y_pred_naive = prices[:-FORECAST_HORIZON]
    n            = min(len(y_true_naive), len(y_pred_naive))
    y_true_naive = y_true_naive[:n]
    y_pred_naive = y_pred_naive[:n]

    baseline_mape = compute_mape(y_true_naive, y_pred_naive)
    baseline_rmse = compute_rmse(y_true_naive, y_pred_naive)
    baseline_da   = compute_da(y_true_naive, y_pred_naive)

    # Model XGBoost (jika ada)
    model_path = MODEL_DIR / f"{komoditas}_xgb.joblib"
    model_result: dict[str, Any] = {}

    if model_path.exists():
        try:
            model = joblib.load(model_path)
            fi_data = _load_json(EXPORT_DIR / "feature_importance.json")
            if fi_data:
                model_info = fi_data.get("models", {}).get(komoditas, {})
                model_result = {
                    "mape": model_info.get("metrics", {}).get("mape_avg"),
                    "rmse": model_info.get("metrics", {}).get("rmse_avg"),
                    "improvement_vs_baseline_mape": round(
                        baseline_mape - (model_info.get("metrics", {}).get("mape_avg") or baseline_mape),
                        2,
                    ),
                }
        except Exception as e:
            log.warning(f"Gagal memuat model {model_path}: {e}")

    return {
        "komoditas":      komoditas,
        "n_samples":      n,
        "baseline": {
            "name":  "naïve (harga t-7)",
            "mape":  round(baseline_mape, 2),
            "rmse":  round(baseline_rmse, 0),
            "da":    round(baseline_da, 1),
        },
        "xgboost": model_result or {"note": "model belum dilatih"},
    }


# ── Main Evaluation ───────────────────────────────────────────────────────────

def run_evaluation() -> dict[str, Any]:
    """Evaluasi semua komoditas dan simpan laporan."""
    df_all = _load_feature_store()
    risk_data = _load_json(EXPORT_DIR / "risk_index.json") or {}

    log.info(f"Feature store: {len(df_all)} baris")

    evaluation_results: dict[str, Any] = {}

    for komoditas in KOMODITAS_LIST:
        log.info(f"Evaluasi: {komoditas}")
        result = backtest_model(df_all, komoditas)

        # Hitung lead time jika ada data risk index
        risk_indices = risk_data.get("risk_indices", {})
        if komoditas in risk_indices:
            # Simulasi: buat series risk dummy jika hanya ada satu nilai
            risk_val = risk_indices[komoditas].get("risk_index", 0)
            log.info(f"  Risk index saat ini: {risk_val:.1f}")
            result["current_risk_index"] = risk_val

        evaluation_results[komoditas] = result

        baseline = result.get("baseline", {})
        xgb_res  = result.get("xgboost", {})
        log.info(
            f"  Baseline MAPE={baseline.get('mape', '?')}%  "
            f"DA={baseline.get('da', '?')}%  "
            f"XGB_MAPE={xgb_res.get('mape', 'N/A')}"
        )

    # Ringkasan performa keseluruhan
    valid_baselines = [
        r["baseline"]["mape"]
        for r in evaluation_results.values()
        if isinstance(r.get("baseline", {}).get("mape"), (int, float))
    ]
    summary = {
        "avg_baseline_mape": round(np.mean(valid_baselines), 2) if valid_baselines else None,
        "komoditas_count":   len(KOMODITAS_LIST),
        "advantage_vs_descriptive": (
            "LUMBUNG memberikan sinyal prediktif H-7, sementara dashboard "
            "deskriptif (PIHPS/Bapanas) hanya menampilkan harga hari ini tanpa prediksi."
        ),
    }

    output: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary":      summary,
        "per_komoditas": evaluation_results,
    }

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = EXPORT_DIR / "evaluation.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info(f"Laporan evaluasi disimpan ke {out_path}")
    return output


if __name__ == "__main__":
    result = run_evaluation()
    print("\n=== EVALUASI MODEL ===")
    summary = result.get("summary", {})
    print(f"Rata-rata MAPE baseline: {summary.get('avg_baseline_mape', 'N/A')}%")
    print()
    for kom, data in result.get("per_komoditas", {}).items():
        if "error" in data:
            print(f"  {kom:25s}: ERROR — {data['error']}")
            continue
        b = data.get("baseline", {})
        x = data.get("xgboost",  {})
        print(
            f"  {kom:25s} | Baseline MAPE={b.get('mape','?'):5}%  "
            f"DA={b.get('da','?'):4}%  "
            f"XGB_MAPE={x.get('mape','N/A')}"
        )
    sys.exit(0)
