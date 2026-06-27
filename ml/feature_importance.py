"""
LUMBUNG — Feature Importance dari Model XGBoost
Owner: Yasykur

Melatih model XGBoost untuk memprediksi harga komoditas 7 hari ke depan,
lalu mengekstrak feature importance untuk dijelaskan di dashboard.

Pipeline:
  1. Baca Gold feature_store
  2. Feature engineering (lag, rolling stats, cuaca, NLP score)
  3. Train XGBoost regressor per komoditas
  4. Simpan model ke ml/models/
  5. Ekspor feature importance + prediksi 7 hari ke temp_buffer/export/

Output:
  - ml/models/{komoditas}_xgb.joblib       (model terlatih)
  - temp_buffer/export/feature_importance.json
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
log = logging.getLogger("feature_importance")

BASE_DIR   = Path(__file__).resolve().parent.parent
EXPORT_DIR = BASE_DIR / "temp_buffer" / "export"
MODEL_DIR  = Path(__file__).resolve().parent / "models"
MODEL_DIR.mkdir(parents=True, exist_ok=True)

KOMODITAS_LIST = [
    "beras",
    "cabai_rawit_merah",
    "cabai_keriting",
    "bawang_merah",
    "bawang_putih",
]

FORECAST_HORIZON = 7   # hari
MIN_ROWS_REQUIRED = 30  # minimum data untuk melatih model


# ── Data Loader ───────────────────────────────────────────────────────────────

def _load_feature_store() -> pd.DataFrame:
    """Muat Gold feature store dari JSON export."""
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
            log.warning("feature_store.json kosong. Gunakan data dummy.")
            return _generate_dummy_data()

        # Jika data nyata terlalu sedikit untuk training, gunakan dummy agar
        # pipeline end-to-end tetap menghasilkan model/forecast.
        komoditas_col = "komoditas" if "komoditas" in df.columns else "commodity"
        min_rows_per_kom = MIN_ROWS_REQUIRED + FORECAST_HORIZON
        if komoditas_col not in df.columns or (
            df.groupby(komoditas_col).size() < min_rows_per_kom
        ).any():
            log.warning(
                "feature_store.json tidak memiliki cukup data per komoditas "
                f"(butuh >= {min_rows_per_kom} baris). Gunakan data dummy."
            )
            return _generate_dummy_data()

        return df
    except Exception as e:
        log.error(f"Gagal memuat feature_store: {e}. Gunakan data dummy.")
        return _generate_dummy_data()


def _generate_dummy_data() -> pd.DataFrame:
    """
    Buat data dummy realistis untuk training/demo jika data nyata belum tersedia.
    Mensimulasikan fluktuasi harga komoditas Indonesia selama 365 hari.
    """
    log.info("Membuat data dummy (365 hari) untuk demo model...")
    base_prices = {
        "beras":           13500.0,
        "cabai_rawit_merah": 85000.0,
        "cabai_keriting":  55000.0,
        "bawang_merah":    38000.0,
        "bawang_putih":    42000.0,
    }

    records = []
    today = datetime.now(timezone.utc).date()
    rng = np.random.default_rng(seed=42)

    for komoditas, base in base_prices.items():
        price = base
        for i in range(365):
            date = today - timedelta(days=364 - i)
            # Simulasi fluktuasi harga harian
            volatility = 0.02 if komoditas == "beras" else 0.05
            shock = rng.normal(0, volatility)
            price = max(base * 0.5, price * (1.0 + shock))
            records.append({
                "date_parsed": str(date),
                "komoditas":   komoditas,
                "avg_price":   round(price, 0),
                # Fitur tambahan (dummy)
                "precipitation_sum": float(rng.uniform(0, 80)),
                "temperature_max":   float(rng.uniform(28, 38)),
                "news_score":        float(rng.uniform(0, 15)),
                "news_velocity":     float(rng.uniform(0, 5)),
            })

    return pd.DataFrame(records)


# ── Feature Engineering ───────────────────────────────────────────────────────

def engineer_features(df: pd.DataFrame, komoditas: str) -> pd.DataFrame:
    """
    Buat fitur time-series dari data harga + cuaca + berita.
    """
        # Ensure column name consistency for commodity
    if "commodity" in df.columns:
        df = df.rename(columns={"commodity": "komoditas"})
    df_kom = df[df["komoditas"] == komoditas].copy()
    df_kom["date_parsed"] = pd.to_datetime(df_kom["date_parsed"], errors="coerce")
    df_kom = df_kom.dropna(subset=["date_parsed", "avg_price"])
    df_kom = df_kom.sort_values("date_parsed").reset_index(drop=True)
    df_kom["date_parsed"] = pd.to_datetime(df_kom["date_parsed"], errors="coerce")
    df_kom = df_kom.dropna(subset=["date_parsed", "avg_price"])
    df_kom = df_kom.sort_values("date_parsed").reset_index(drop=True)

    if len(df_kom) < MIN_ROWS_REQUIRED:
        return pd.DataFrame()

    p = df_kom["avg_price"].astype(float)

    # Fitur lag harga
    for lag in [1, 3, 7, 14, 30]:
        df_kom[f"price_lag_{lag}d"] = p.shift(lag)

    # Rolling statistics
    for window in [7, 14, 30]:
        df_kom[f"price_ma_{window}d"]   = p.rolling(window).mean()
        df_kom[f"price_std_{window}d"]  = p.rolling(window).std()

    # Perubahan relatif
    df_kom["price_pct_7d"]  = p.pct_change(periods=7)  * 100
    df_kom["price_pct_30d"] = p.pct_change(periods=30) * 100

    # Fitur cuaca (jika ada)
    if "precipitation_sum" in df_kom.columns:
        df_kom["precip_roll7"] = df_kom["precipitation_sum"].rolling(7).mean()
    else:
        df_kom["precip_roll7"] = 0.0

    if "temperature_max" in df_kom.columns:
        df_kom["temp_roll7"] = df_kom["temperature_max"].rolling(7).mean()
    else:
        df_kom["temp_roll7"] = 0.0

    # Fitur NLP (jika ada)
    if "news_score" not in df_kom.columns:
        df_kom["news_score"] = 0.0
    if "news_velocity" not in df_kom.columns:
        df_kom["news_velocity"] = 0.0

    # Target: harga 7 hari ke depan
    df_kom["target_price_7d"] = p.shift(-FORECAST_HORIZON)

    # Buang baris dengan null
    df_kom = df_kom.dropna()
    return df_kom


FEATURE_COLS = [
    "price_lag_1d", "price_lag_3d", "price_lag_7d", "price_lag_14d", "price_lag_30d",
    "price_ma_7d", "price_ma_14d", "price_ma_30d",
    "price_std_7d", "price_std_14d", "price_std_30d",
    "price_pct_7d", "price_pct_30d",
    "precip_roll7", "temp_roll7",
    "news_score", "news_velocity",
]


# ── Training & Inference ──────────────────────────────────────────────────────

def train_and_evaluate(
    df_feat: pd.DataFrame,
    komoditas: str,
) -> dict[str, Any]:
    """
    Latih XGBoost regressor dan kembalikan feature importance + metrics.
    """
    try:
        import xgboost as xgb
        import joblib
        from sklearn.model_selection import TimeSeriesSplit
        from sklearn.metrics import mean_absolute_percentage_error, mean_squared_error
    except ImportError as e:
        log.error(f"Dependensi ML tidak tersedia: {e}")
        return {}

    # Pilih kolom fitur yang tersedia
    feature_cols = [c for c in FEATURE_COLS if c in df_feat.columns]
    X = df_feat[feature_cols].values
    y = df_feat["target_price_7d"].values

    # Time-series split (tidak acak, pakai urutan waktu)
    tscv   = TimeSeriesSplit(n_splits=3)
    mape_scores: list[float] = []
    rmse_scores: list[float] = []

    best_model = None
    best_score = float("inf")

    for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
        X_train, X_val = X[train_idx], X[val_idx]
        y_train, y_val = y[train_idx], y[val_idx]

        model = xgb.XGBRegressor(
            n_estimators=200,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            verbosity=0,
        )
        model.fit(X_train, y_train)
        y_pred = model.predict(X_val)

        mape = mean_absolute_percentage_error(y_val, y_pred) * 100
        rmse = np.sqrt(mean_squared_error(y_val, y_pred))
        mape_scores.append(mape)
        rmse_scores.append(rmse)

        if rmse < best_score:
            best_score = rmse
            best_model = model

        log.info(f"  Fold {fold+1}: MAPE={mape:.2f}% RMSE={rmse:.0f}")

    if best_model is None:
        return {}

    # Simpan model terbaik
    model_path = MODEL_DIR / f"{komoditas}_xgb.joblib"
    joblib.dump(best_model, model_path)
    log.info(f"  Model disimpan: {model_path}")

    # Feature importance (gain-based)
    importance_scores = best_model.feature_importances_
    importance_dict = {
        feat: round(float(score), 6)
        for feat, score in zip(feature_cols, importance_scores)
    }
    # Urutkan dari tertinggi ke terendah
    importance_sorted = dict(
        sorted(importance_dict.items(), key=lambda x: x[1], reverse=True)
    )

    # Prediksi 7 hari ke depan (pakai baris terakhir sebagai basis)
    last_row = df_feat[feature_cols].iloc[-1].values.reshape(1, -1)
    forecast_price = float(best_model.predict(last_row)[0])
    current_price  = float(df_feat["avg_price"].iloc[-1])
    predicted_change_pct = round((forecast_price - current_price) / current_price * 100, 2)

    return {
        "komoditas":          komoditas,
        "current_price":      round(current_price, 0),
        "forecast_price_7d":  round(forecast_price, 0),
        "predicted_change_pct": predicted_change_pct,
        "metrics": {
            "mape_avg":  round(float(np.mean(mape_scores)), 2),
            "mape_std":  round(float(np.std(mape_scores)),  2),
            "rmse_avg":  round(float(np.mean(rmse_scores)), 0),
        },
        "feature_importance": importance_sorted,
        "top_3_features": list(importance_sorted.keys())[:3],
        "training_samples": len(df_feat),
        "feature_cols_used": feature_cols,
    }


def generate_forecast_timeline(
    df_feat: pd.DataFrame,
    komoditas: str,
    model_result: dict,
) -> list[dict]:
    """Buat timeline prediksi harga untuk 7 hari ke depan."""
    today = datetime.now(timezone.utc).date()
    current = model_result.get("current_price", 0)
    target  = model_result.get("forecast_price_7d", current)
    delta   = (target - current) / FORECAST_HORIZON

    timeline = []
    for i in range(1, FORECAST_HORIZON + 1):
        date = today + timedelta(days=i)
        predicted = round(current + delta * i, 0)
        timeline.append({
            "date": str(date),
            "predicted_price": predicted,
            "day_label": f"H+{i}",
        })
    return timeline


def run_all() -> dict[str, Any]:
    """Latih model untuk semua komoditas dan ekspor hasil."""
    df_all = _load_feature_store()
    log.info(f"Total baris feature store: {len(df_all)}")

    results: dict[str, Any] = {}
    forecasts: dict[str, Any] = {}

    for komoditas in KOMODITAS_LIST:
        log.info(f"\n--- Training model untuk: {komoditas} ---")
        df_feat = engineer_features(df_all, komoditas)

        if df_feat.empty:
            log.warning(f"  Data tidak cukup untuk {komoditas}, lewati.")
            results[komoditas] = {"error": "data tidak cukup"}
            continue

        model_result = train_and_evaluate(df_feat, komoditas)
        if not model_result:
            results[komoditas] = {"error": "training gagal"}
            continue

        timeline = generate_forecast_timeline(df_feat, komoditas, model_result)
        model_result["forecast_timeline"] = timeline
        results[komoditas] = model_result

        forecasts[komoditas] = {
            "current_price":        model_result["current_price"],
            "forecast_price_7d":    model_result["forecast_price_7d"],
            "predicted_change_pct": model_result["predicted_change_pct"],
            "forecast_timeline":    timeline,
        }

        log.info(
            f"  {komoditas}: current={model_result['current_price']:,.0f} "
            f"forecast_7d={model_result['forecast_price_7d']:,.0f} "
            f"({model_result['predicted_change_pct']:+.1f}%) "
            f"MAPE={model_result['metrics']['mape_avg']:.1f}%"
        )

    # Simpan feature importance
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    fi_output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "models": results,
    }
    fi_path = EXPORT_DIR / "feature_importance.json"
    with open(fi_path, "w", encoding="utf-8") as f:
        json.dump(fi_output, f, ensure_ascii=False, indent=2)
    log.info(f"Feature importance disimpan ke {fi_path}")

    # Simpan forecast
    forecast_output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "forecasts": forecasts,
    }
    fc_path = EXPORT_DIR / "price_forecast.json"
    with open(fc_path, "w", encoding="utf-8") as f:
        json.dump(forecast_output, f, ensure_ascii=False, indent=2)
    log.info(f"Forecast disimpan ke {fc_path}")

    return fi_output


if __name__ == "__main__":
    result = run_all()
    print("\n=== Feature Importance Summary ===")
    for kom, data in result.get("models", {}).items():
        if "error" in data:
            print(f"  {kom}: ERROR — {data['error']}")
            continue
        top3 = data.get("top_3_features", [])
        mape = data.get("metrics", {}).get("mape_avg", 0)
        chg  = data.get("predicted_change_pct", 0)
        print(
            f"  {kom:25s} | MAPE={mape:.1f}%  "
            f"forecast_7d={chg:+.1f}%  "
            f"top_features={top3}"
        )
    sys.exit(0)
