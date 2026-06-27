"""
LUMBUNG — Standalone XGBoost Price Forecaster (7-hari & 14-hari)
Owner: Ryan

Melatih XGBoost regressor per komoditas untuk meramalkan harga 7 dan 14 hari
ke depan, dengan lag features, rolling statistics, dan fitur kalender.

Output:
  - ml/models/{komoditas}_forecast.joblib
  - temp_buffer/export/price_forecast.json
"""

from __future__ import annotations
import json
import logging
import sys
import warnings
from datetime import datetime, timedelta, timezone
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
log = logging.getLogger("train_price_forecast")

BASE_DIR   = Path(__file__).resolve().parent.parent
EXPORT_DIR = BASE_DIR / "temp_buffer" / "export"
MODEL_DIR  = Path(__file__).resolve().parent / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

KOMODITAS_LIST = [
    "beras", "cabai_rawit_merah", "cabai_keriting",
    "bawang_merah", "bawang_putih", "gula_pasir",
    "minyak_goreng", "daging_ayam", "telur_ayam", "daging_sapi",
]

HORIZONS = [7, 14]
MIN_ROWS_REQUIRED = 30

FEATURE_COLS = [
    "price_lag_1d", "price_lag_3d", "price_lag_7d", "price_lag_14d", "price_lag_30d",
    "price_ma_7d", "price_ma_14d", "price_ma_30d",
    "price_std_7d", "price_std_14d", "price_std_30d",
    "price_pct_7d", "price_pct_30d",
    "day_of_week", "month", "trend_idx",
    "precip_roll7", "temp_roll7",
    "news_score", "news_velocity",
]


def _load_feature_store() -> pd.DataFrame:
    path = EXPORT_DIR / "feature_store.json"
    if not path.exists():
        log.warning("feature_store.json tidak ditemukan. Gunakan data dummy.")
        return _generate_dummy_data()
    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
        if isinstance(raw, list):
            df = pd.DataFrame(raw)
        elif isinstance(raw, dict) and "data" in raw:
            df = pd.DataFrame(raw["data"])
        else:
            df = pd.DataFrame()
        if df.empty:
            return _generate_dummy_data()
        komoditas_col = "komoditas" if "komoditas" in df.columns else "commodity"
        min_rows = MIN_ROWS_REQUIRED + max(HORIZONS)
        if komoditas_col not in df.columns or (
            df.groupby(komoditas_col).size() < min_rows
        ).any():
            return _generate_dummy_data()
        return df
    except Exception as e:
        log.error(f"Gagal memuat feature_store: {e}. Gunakan data dummy.")
        return _generate_dummy_data()


def _generate_dummy_data() -> pd.DataFrame:
    log.info("Membuat data dummy (365 hari) untuk demo model...")
    base_prices = {
        "beras": 13500.0, "cabai_rawit_merah": 85000.0,
        "cabai_keriting": 55000.0, "bawang_merah": 38000.0,
        "bawang_putih": 42000.0, "gula_pasir": 20300.0,
        "minyak_goreng": 20550.0, "daging_ayam": 37200.0,
        "telur_ayam": 29750.0, "daging_sapi": 149200.0,
    }
    records = []
    today = datetime.now(timezone.utc).date()
    rng = np.random.default_rng(seed=42)
    for komoditas, base in base_prices.items():
        price = base
        for i in range(365):
            date = today - timedelta(days=364 - i)
            volatility = 0.02 if komoditas == "beras" else 0.05
            shock = rng.normal(0, volatility)
            price = max(base * 0.5, price * (1.0 + shock))
            records.append({
                "date_parsed": str(date), "komoditas": komoditas,
                "avg_price": round(price, 0),
                "precipitation_sum": float(rng.uniform(0, 80)),
                "temperature_max": float(rng.uniform(28, 38)),
                "news_score": float(rng.uniform(0, 15)),
                "news_velocity": float(rng.uniform(0, 5)),
            })
    return pd.DataFrame(records)


def engineer_features(df: pd.DataFrame, komoditas: str, horizon: int) -> pd.DataFrame:
    if "commodity" in df.columns:
        df = df.rename(columns={"commodity": "komoditas"})
    df_kom = df[df["komoditas"] == komoditas].copy()
    df_kom["date_parsed"] = pd.to_datetime(df_kom["date_parsed"], errors="coerce")
    df_kom = df_kom.dropna(subset=["date_parsed", "avg_price"])
    df_kom = df_kom.sort_values("date_parsed").reset_index(drop=True)
    if len(df_kom) < MIN_ROWS_REQUIRED:
        return pd.DataFrame()

    p = df_kom["avg_price"].astype(float)
    for lag in [1, 3, 7, 14, 30]:
        df_kom[f"price_lag_{lag}d"] = p.shift(lag)
    for window in [7, 14, 30]:
        df_kom[f"price_ma_{window}d"] = p.rolling(window).mean()
        df_kom[f"price_std_{window}d"] = p.rolling(window).std()
    df_kom["price_pct_7d"] = p.pct_change(periods=7) * 100
    df_kom["price_pct_30d"] = p.pct_change(periods=30) * 100
    df_kom["day_of_week"] = df_kom["date_parsed"].dt.dayofweek
    df_kom["month"] = df_kom["date_parsed"].dt.month
    df_kom["trend_idx"] = np.arange(len(df_kom), dtype=float)

    if "precipitation_sum" in df_kom.columns:
        df_kom["precip_roll7"] = df_kom["precipitation_sum"].rolling(7).mean()
    else:
        df_kom["precip_roll7"] = 0.0
    if "temperature_max" in df_kom.columns:
        df_kom["temp_roll7"] = df_kom["temperature_max"].rolling(7).mean()
    else:
        df_kom["temp_roll7"] = 0.0
    if "news_score" not in df_kom.columns:
        df_kom["news_score"] = 0.0
    if "news_velocity" not in df_kom.columns:
        df_kom["news_velocity"] = 0.0

    df_kom[f"target_price_{horizon}d"] = p.shift(-horizon)
    drop_cols = [c for c in FEATURE_COLS if c in df_kom.columns] + [f"target_price_{horizon}d"]
    df_kom = df_kom.dropna(subset=drop_cols)
    return df_kom


def train_model(df_feat: pd.DataFrame, komoditas: str, horizon: int) -> dict[str, Any] | None:
    try:
        import xgboost as xgb
        import joblib
        from sklearn.model_selection import TimeSeriesSplit
        from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error
    except ImportError as e:
        log.error(f"Dependensi ML tidak tersedia: {e}")
        return None

    feature_cols = [c for c in FEATURE_COLS if c in df_feat.columns]
    target_col = f"target_price_{horizon}d"
    X = df_feat[feature_cols].values
    y = df_feat[target_col].values

    tscv = TimeSeriesSplit(n_splits=3)
    mape_scores, rmse_scores = [], []
    best_model, best_score = None, float("inf")

    for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
        model = xgb.XGBRegressor(
            n_estimators=200, max_depth=4, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8, random_state=42, verbosity=0,
        )
        model.fit(X[train_idx], y[train_idx])
        y_pred = model.predict(X[val_idx])
        mape = mean_absolute_percentage_error(y[val_idx], y_pred) * 100
        rmse = np.sqrt(mean_squared_error(y[val_idx], y_pred))
        mape_scores.append(mape)
        rmse_scores.append(rmse)
        if rmse < best_score:
            best_score = rmse
            best_model = model
        log.info(f"  H={horizon}d Fold {fold+1}: MAPE={mape:.2f}% RMSE={rmse:.0f}")

    if best_model is None:
        return None

    model_path = MODEL_DIR / f"{komoditas}_forecast_{horizon}d.joblib"
    joblib.dump(best_model, model_path)

    last_row = df_feat[feature_cols].iloc[-1].values.reshape(1, -1)
    forecast_price = float(best_model.predict(last_row)[0])
    current_price = float(df_feat["avg_price"].iloc[-1])

    return {
        "forecast_price": round(forecast_price, 0),
        "current_price": round(current_price, 0),
        "mape_avg": round(float(np.mean(mape_scores)), 2),
        "rmse_avg": round(float(np.mean(rmse_scores)), 0),
    }


def generate_forecast_timeline(current_price, forecast_7d, forecast_14d):
    today = datetime.now(timezone.utc).date()
    timeline = []
    for i in range(1, 15):
        date = today + timedelta(days=i)
        if i <= 7:
            predicted = current_price + (forecast_7d - current_price) * (i / 7.0)
        else:
            predicted = forecast_7d + (forecast_14d - forecast_7d) * ((i - 7) / 7.0)
        timeline.append({"date": str(date), "predicted_price": round(predicted, 0), "day_label": f"H+{i}"})
    return timeline


def run_all() -> dict[str, Any]:
    df_all = _load_feature_store()
    log.info(f"Total baris feature store: {len(df_all)}")
    forecasts = {}

    for komoditas in KOMODITAS_LIST:
        log.info(f"\n--- Training forecast: {komoditas} ---")
        df_7 = engineer_features(df_all, komoditas, horizon=7)
        if df_7.empty:
            log.warning(f"  Data tidak cukup untuk {komoditas}")
            continue

        result_7 = train_model(df_7, komoditas, horizon=7)
        df_14 = engineer_features(df_all, komoditas, horizon=14)
        result_14 = train_model(df_14, komoditas, horizon=14) if not df_14.empty else None

        if result_7 is None:
            continue

        current_price = result_7["current_price"]
        forecast_7d = result_7["forecast_price"]
        forecast_14d = result_14["forecast_price"] if result_14 else forecast_7d
        mape_7 = result_7["mape_avg"]
        rmse = result_7["rmse_avg"]

        change_7 = round((forecast_7d - current_price) / current_price * 100, 2) if current_price else 0.0
        change_14 = round((forecast_14d - current_price) / current_price * 100, 2) if current_price else 0.0

        timeline = generate_forecast_timeline(current_price, forecast_7d, forecast_14d)

        forecasts[komoditas] = {
            "current_price": current_price,
            "forecast_7d": forecast_7d,
            "forecast_14d": forecast_14d,
            "predicted_change_pct_7d": change_7,
            "predicted_change_pct_14d": change_14,
            "predicted_change_pct": change_7,  # backward compat
            "forecast_timeline": timeline,
            "confidence_lower": round(forecast_7d - rmse, 0),
            "confidence_upper": round(forecast_7d + rmse, 0),
            "model_mape": mape_7,
        }
        log.info(f"  {komoditas}: {current_price:,.0f} -> 7d={forecast_7d:,.0f}({change_7:+.1f}%) MAPE={mape_7:.1f}%")

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    output = {"generated_at": datetime.now(timezone.utc).isoformat(), "forecasts": forecasts}
    out_path = EXPORT_DIR / "price_forecast.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    log.info(f"Exported: {out_path} ({len(forecasts)} komoditas)")
    return output


if __name__ == "__main__":
    result = run_all()
    print("\n=== Price Forecast Summary ===")
    for kom, data in result.get("forecasts", {}).items():
        if "error" in data:
            print(f"  {kom}: ERROR -- {data['error']}")
            continue
        print(f"  {kom:25s} | now={data['current_price']:>10,.0f} 7d={data['forecast_7d']:>10,.0f}({data['predicted_change_pct_7d']:+.1f}%) MAPE={data['model_mape']:.1f}%")
    sys.exit(0)
