"""
LUMBUNG — Model Regresi Besaran Kenaikan Harga (Quantile Regression)
Owner: Ryan

Memprediksi magnitude kenaikan harga dalam Rp/kg dan persentase menggunakan
XGBoost quantile regression (p10, p50, p90).

Output:
  - ml/models/{komoditas}_magnitude.joblib
  - temp_buffer/export/magnitude_estimate.json
"""

from __future__ import annotations
import json
import logging
import sys
import warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("train_magnitude")

BASE_DIR   = Path(__file__).resolve().parent.parent
EXPORT_DIR = BASE_DIR / "temp_buffer" / "export"
MODEL_DIR  = Path(__file__).resolve().parent / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

KOMODITAS_LIST = [
    "beras", "cabai_rawit_merah", "cabai_keriting",
    "bawang_merah", "bawang_putih", "gula_pasir",
    "minyak_goreng", "daging_ayam", "telur_ayam", "daging_sapi",
]

MIN_ROWS = 30
SPIKE_THRESHOLD_PCT = 5.0  # >5% weekly change = spike


def _load_feature_store() -> pd.DataFrame:
    path = EXPORT_DIR / "feature_store.json"
    if not path.exists():
        return _dummy_data()
    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
        df = pd.DataFrame(raw if isinstance(raw, list) else raw.get("data", []))
        return df if not df.empty else _dummy_data()
    except Exception:
        return _dummy_data()


def _dummy_data() -> pd.DataFrame:
    log.info("Membuat data dummy 365 hari...")
    base = {
        "beras": 13500, "cabai_rawit_merah": 85000, "cabai_keriting": 55000,
        "bawang_merah": 38000, "bawang_putih": 42000, "gula_pasir": 20300,
        "minyak_goreng": 20550, "daging_ayam": 37200, "telur_ayam": 29750,
        "daging_sapi": 149200,
    }
    rng = np.random.default_rng(42)
    today = datetime.now(timezone.utc).date()
    rows = []
    for kom, bp in base.items():
        price = float(bp)
        vol = 0.02 if kom == "beras" else 0.05
        for i in range(365):
            dt = today - timedelta(days=364 - i)
            price = max(bp * 0.5, price * (1 + rng.normal(0, vol)))
            rows.append({
                "date_parsed": str(dt), "komoditas": kom,
                "avg_price": round(price),
                "precipitation_sum": float(rng.uniform(0, 80)),
                "temperature_max": float(rng.uniform(28, 38)),
                "news_score": float(rng.uniform(0, 15)),
                "news_velocity": float(rng.uniform(0, 5)),
            })
    return pd.DataFrame(rows)


FEATURE_COLS = [
    "price_lag_1d", "price_lag_3d", "price_lag_7d", "price_lag_14d",
    "price_ma_7d", "price_ma_14d", "price_ma_30d",
    "price_std_7d", "price_std_14d",
    "price_pct_7d", "price_pct_30d",
    "precip_roll7", "temp_roll7",
    "news_score", "news_velocity",
    "coeff_var_14d", "month",
]


def _engineer(df: pd.DataFrame, kom: str) -> pd.DataFrame:
    if "commodity" in df.columns:
        df = df.rename(columns={"commodity": "komoditas"})
    sub = df[df["komoditas"] == kom].copy()
    sub["date_parsed"] = pd.to_datetime(sub["date_parsed"], errors="coerce")
    sub = sub.dropna(subset=["date_parsed", "avg_price"]).sort_values("date_parsed").reset_index(drop=True)
    if len(sub) < MIN_ROWS:
        return pd.DataFrame()

    p = sub["avg_price"].astype(float)
    for lag in [1, 3, 7, 14]:
        sub[f"price_lag_{lag}d"] = p.shift(lag)
    for w in [7, 14, 30]:
        sub[f"price_ma_{w}d"] = p.rolling(w).mean()
    for w in [7, 14]:
        sub[f"price_std_{w}d"] = p.rolling(w).std()
    sub["price_pct_7d"] = p.pct_change(7) * 100
    sub["price_pct_30d"] = p.pct_change(30) * 100

    ma14 = p.rolling(14).mean()
    std14 = p.rolling(14).std()
    sub["coeff_var_14d"] = (std14 / ma14 * 100).fillna(0)

    sub["precip_roll7"] = sub.get("precipitation_sum", pd.Series(0, index=sub.index)).rolling(7).mean().fillna(0)
    sub["temp_roll7"] = sub.get("temperature_max", pd.Series(30, index=sub.index)).rolling(7).mean().fillna(30)
    for col in ["news_score", "news_velocity"]:
        if col not in sub.columns:
            sub[col] = 0.0
    sub["month"] = sub["date_parsed"].dt.month

    # Target: 7-day forward change
    sub["target_change_rpkg"] = p.shift(-7) - p
    sub["target_change_pct"] = (p.shift(-7) / p - 1) * 100

    sub = sub.dropna(subset=[c for c in FEATURE_COLS if c in sub.columns] + ["target_change_rpkg"])
    return sub


def run_all() -> dict:
    try:
        from xgboost import XGBRegressor
        import joblib
    except ImportError as e:
        log.error(f"Dependensi tidak tersedia: {e}")
        return {}

    df_all = _load_feature_store()
    log.info(f"Loaded {len(df_all)} rows")
    estimates = {}

    for kom in KOMODITAS_LIST:
        log.info(f"--- Magnitude model: {kom} ---")
        df = _engineer(df_all, kom)
        if df.empty or len(df) < 20:
            log.warning(f"  {kom}: data tidak cukup, skip")
            continue

        feature_cols = [c for c in FEATURE_COLS if c in df.columns]
        X = df[feature_cols].values

        # Detect historical spikes
        weekly_pct = df["target_change_pct"].values
        spike_mask = np.abs(weekly_pct) > SPIKE_THRESHOLD_PCT
        n_spikes = int(spike_mask.sum())
        max_spike = float(np.max(np.abs(weekly_pct))) if len(weekly_pct) > 0 else 0.0
        spike_prob = n_spikes / len(weekly_pct) if len(weekly_pct) > 0 else 0.0

        # Train quantile models for Rp/kg magnitude
        rpkg_quantiles = {}
        pct_quantiles = {}
        for q, label in [(0.1, "p10"), (0.5, "p50"), (0.9, "p90")]:
            # Rp/kg model
            model_rpkg = XGBRegressor(
                n_estimators=150, max_depth=4, learning_rate=0.05,
                objective="reg:quantileerror", quantile_alpha=q,
                random_state=42, verbosity=0,
            )
            y_rpkg = df["target_change_rpkg"].values
            model_rpkg.fit(X, y_rpkg)
            pred_rpkg = float(model_rpkg.predict(X[-1:].reshape(1, -1))[0])
            rpkg_quantiles[label] = round(pred_rpkg)

            # Pct model
            model_pct = XGBRegressor(
                n_estimators=150, max_depth=4, learning_rate=0.05,
                objective="reg:quantileerror", quantile_alpha=q,
                random_state=42, verbosity=0,
            )
            y_pct = df["target_change_pct"].values
            model_pct.fit(X, y_pct)
            pred_pct = float(model_pct.predict(X[-1:].reshape(1, -1))[0])
            pct_quantiles[label] = round(pred_pct, 2)

            if label == "p50":
                joblib.dump(model_rpkg, MODEL_DIR / f"{kom}_magnitude.joblib")

        estimates[kom] = {
            "estimated_increase_rpkg": rpkg_quantiles,
            "estimated_increase_pct": pct_quantiles,
            "spike_probability": round(spike_prob, 3),
            "historical_max_spike_pct": round(max_spike, 1),
            "n_spike_events_trained": n_spikes,
        }
        log.info(f"  {kom}: p50={rpkg_quantiles['p50']:+,} Rp/kg ({pct_quantiles['p50']:+.1f}%), spike_prob={spike_prob:.1%}")

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    output = {"generated_at": datetime.now(timezone.utc).isoformat(), "estimates": estimates}
    out_path = EXPORT_DIR / "magnitude_estimate.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    log.info(f"Exported: {out_path}")
    return output


if __name__ == "__main__":
    result = run_all()
    print("\n=== Magnitude Estimate Summary ===")
    for kom, d in result.get("estimates", {}).items():
        rpkg = d["estimated_increase_rpkg"]
        pct = d["estimated_increase_pct"]
        print(f"  {kom:25s} | Rp/kg: [{rpkg['p10']:+,} ~ {rpkg['p50']:+,} ~ {rpkg['p90']:+,}] | %: [{pct['p10']:+.1f} ~ {pct['p50']:+.1f} ~ {pct['p90']:+.1f}] | spike_prob={d['spike_probability']:.1%}")
    sys.exit(0)
