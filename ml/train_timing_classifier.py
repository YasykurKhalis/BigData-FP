"""
LUMBUNG — Klasifikasi Window Waktu Lonjakan Harga
Owner: Ryan

Memprediksi KAPAN lonjakan harga akan terjadi menggunakan XGBoost classifier.
Window prediksi: 1-7 hari, 8-14 hari, 15-30 hari, >30 hari/tidak ada.

Output:
  - ml/models/{komoditas}_timing.joblib
  - temp_buffer/export/timing_prediction.json
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
log = logging.getLogger("train_timing")

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
SPIKE_THRESHOLD_PCT = 5.0

WINDOW_LABELS = ["1-7 hari", "8-14 hari", "15-30 hari", ">30 hari / tidak ada"]
WINDOW_MAP = {0: "1-7 hari", 1: "8-14 hari", 2: "15-30 hari", 3: ">30 hari / tidak ada"}

FEATURE_COLS = [
    "price_momentum_3d", "price_momentum_7d", "price_momentum_14d",
    "price_ma_7d", "price_ma_14d", "price_std_7d", "price_std_14d",
    "precip_roll7", "precip_anomaly",
    "temp_roll7", "news_score", "news_velocity",
    "month", "is_ramadan_season", "is_yearend",
]

TRIGGER_LABELS = {
    "price_momentum_3d": "momentum harga 3 hari",
    "price_momentum_7d": "momentum harga 7 hari",
    "price_momentum_14d": "momentum harga 14 hari",
    "price_ma_7d": "rata-rata harga 7 hari",
    "price_ma_14d": "rata-rata harga 14 hari",
    "price_std_7d": "volatilitas harga 7 hari",
    "price_std_14d": "volatilitas harga 14 hari",
    "precip_roll7": "curah hujan sentra produksi",
    "precip_anomaly": "anomali curah hujan",
    "temp_roll7": "suhu sentra produksi",
    "news_score": "intensitas berita pangan",
    "news_velocity": "kecepatan penyebaran berita",
    "month": "faktor musiman (bulan)",
    "is_ramadan_season": "musim Ramadan/Lebaran",
    "is_yearend": "akhir tahun/Natal",
}


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


def _engineer(df: pd.DataFrame, kom: str) -> pd.DataFrame:
    if "commodity" in df.columns:
        df = df.rename(columns={"commodity": "komoditas"})
    sub = df[df["komoditas"] == kom].copy()
    sub["date_parsed"] = pd.to_datetime(sub["date_parsed"], errors="coerce")
    sub = sub.dropna(subset=["date_parsed", "avg_price"]).sort_values("date_parsed").reset_index(drop=True)
    if len(sub) < MIN_ROWS:
        return pd.DataFrame()

    p = sub["avg_price"].astype(float)

    # Price momentum
    sub["price_momentum_3d"] = p.pct_change(3) * 100
    sub["price_momentum_7d"] = p.pct_change(7) * 100
    sub["price_momentum_14d"] = p.pct_change(14) * 100

    for w in [7, 14]:
        sub[f"price_ma_{w}d"] = p.rolling(w).mean()
        sub[f"price_std_{w}d"] = p.rolling(w).std()

    # Weather
    if "precipitation_sum" in sub.columns:
        sub["precip_roll7"] = sub["precipitation_sum"].rolling(7).mean().fillna(0)
        sub["precip_anomaly"] = (sub["precipitation_sum"] - sub["precip_roll7"]).fillna(0)
    else:
        sub["precip_roll7"] = 0.0
        sub["precip_anomaly"] = 0.0
    if "temperature_max" in sub.columns:
        sub["temp_roll7"] = sub["temperature_max"].rolling(7).mean().fillna(30)
    else:
        sub["temp_roll7"] = 30.0

    for col in ["news_score", "news_velocity"]:
        if col not in sub.columns:
            sub[col] = 0.0

    sub["month"] = sub["date_parsed"].dt.month
    sub["is_ramadan_season"] = sub["month"].isin([3, 4]).astype(float)
    sub["is_yearend"] = sub["month"].isin([12, 1]).astype(float)

    # Target: days until next spike (>5% weekly change)
    weekly_pct = p.pct_change(7) * 100
    spike_dates = sub.loc[weekly_pct.abs() > SPIKE_THRESHOLD_PCT, "date_parsed"].values

    days_to_spike = []
    for idx, row_date in enumerate(sub["date_parsed"]):
        future_spikes = spike_dates[spike_dates > row_date]
        if len(future_spikes) > 0:
            delta = (pd.Timestamp(future_spikes[0]) - row_date).days
            days_to_spike.append(delta)
        else:
            days_to_spike.append(999)

    sub["days_to_spike"] = days_to_spike

    # Map to window classes
    def _to_class(d):
        if d <= 7:
            return 0
        elif d <= 14:
            return 1
        elif d <= 30:
            return 2
        else:
            return 3

    sub["target_class"] = sub["days_to_spike"].apply(_to_class)

    feat_cols = [c for c in FEATURE_COLS if c in sub.columns]
    sub = sub.dropna(subset=feat_cols)
    return sub


def run_all() -> dict:
    try:
        from xgboost import XGBClassifier
        import joblib
    except ImportError as e:
        log.error(f"Dependensi tidak tersedia: {e}")
        return {}

    df_all = _load_feature_store()
    log.info(f"Loaded {len(df_all)} rows")
    predictions = {}

    for kom in KOMODITAS_LIST:
        log.info(f"--- Timing classifier: {kom} ---")
        df = _engineer(df_all, kom)
        if df.empty or len(df) < 20:
            log.warning(f"  {kom}: data tidak cukup")
            continue

        feat_cols = [c for c in FEATURE_COLS if c in df.columns]
        X = df[feat_cols].values
        y = df["target_class"].values

        # Remap labels to contiguous 0..n_classes-1
        from sklearn.preprocessing import LabelEncoder
        le = LabelEncoder()
        y_encoded = le.fit_transform(y.astype(int))
        n_classes = len(le.classes_)

        # Class weights for imbalance
        class_counts = np.bincount(y_encoded, minlength=n_classes)
        total = len(y_encoded)
        weights = np.array([total / (n_classes * max(c, 1)) for c in class_counts])
        sample_weights = np.array([weights[int(yi)] for yi in y_encoded])

        model = XGBClassifier(
            n_estimators=200, max_depth=4, learning_rate=0.05,
            objective="multi:softprob", num_class=n_classes,
            subsample=0.8, colsample_bytree=0.8,
            random_state=42, verbosity=0, use_label_encoder=False,
            eval_metric="mlogloss",
        )
        model.fit(X, y_encoded, sample_weight=sample_weights)

        joblib.dump(model, MODEL_DIR / f"{kom}_timing.joblib")

        # Predict on last row
        last_X = X[-1:].reshape(1, -1)
        proba = model.predict_proba(last_X)[0]
        pred_encoded = int(np.argmax(proba))
        pred_class = int(le.inverse_transform([pred_encoded])[0])

        # Window probabilities — map back to original classes
        window_probs = {}
        for orig_cls, label in WINDOW_MAP.items():
            if orig_cls in le.classes_:
                enc_idx = int(np.where(le.classes_ == orig_cls)[0][0])
                window_probs[label] = round(float(proba[enc_idx]), 3)
            else:
                window_probs[label] = 0.0

        # Key triggers from feature importance
        fi = model.feature_importances_
        top_indices = np.argsort(fi)[::-1][:3]
        key_triggers = []
        for idx in top_indices:
            if idx < len(feat_cols):
                col = feat_cols[idx]
                key_triggers.append(TRIGGER_LABELS.get(col, col))

        confidence = round(float(proba[pred_encoded]), 3)

        predictions[kom] = {
            "predicted_window": WINDOW_MAP[pred_class],
            "window_probabilities": window_probs,
            "confidence": confidence,
            "key_triggers": key_triggers,
        }
        log.info(f"  {kom}: window={WINDOW_MAP[pred_class]} conf={confidence:.1%} triggers={key_triggers}")

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    output = {"generated_at": datetime.now(timezone.utc).isoformat(), "predictions": predictions}
    out_path = EXPORT_DIR / "timing_prediction.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    log.info(f"Exported: {out_path}")
    return output


if __name__ == "__main__":
    result = run_all()
    print("\n=== Timing Prediction Summary ===")
    for kom, d in result.get("predictions", {}).items():
        print(f"  {kom:25s} | window={d['predicted_window']:20s} conf={d['confidence']:.1%} | triggers: {', '.join(d['key_triggers'])}")
    sys.exit(0)
