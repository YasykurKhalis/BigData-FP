"""
LUMBUNG — Early Warning Alert Engine
Owner: Hanif

Membaca risk_index.json dan price_forecast.json, lalu memicu alert
jika indeks risiko melewati threshold yang dikonfigurasi di alert_config.yml.

Fitur:
  - Multi-level alert: WASPADA (60), SIAGA (80), KRITIS (80+)
  - Cooldown per komoditas (mencegah alert berulang dalam N jam)
  - Multiple channel output: dashboard JSON, log file
  - Perubahan harga 7 hari (price_change_pct_7d)

Output: temp_buffer/export/alerts.json
"""

from __future__ import annotations
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("alert_engine")

BASE_DIR    = Path(__file__).resolve().parent.parent
EXPORT_DIR  = BASE_DIR / "temp_buffer" / "export"
CONFIG_PATH = Path(__file__).resolve().parent / "alert_config.yml"
COOLDOWN_PATH = EXPORT_DIR / "alert_cooldown.json"

KOMODITAS_LABEL = {
    "beras":             "Beras",
    "cabai_rawit_merah": "Cabai Rawit Merah",
    "cabai_keriting":    "Cabai Merah Keriting",
    "bawang_merah":      "Bawang Merah",
    "bawang_putih":      "Bawang Putih",
}


# ── Config & Cooldown ─────────────────────────────────────────────────────────

def load_config() -> dict:
    """Baca alert_config.yml."""
    if not CONFIG_PATH.exists():
        log.warning("alert_config.yml tidak ditemukan. Gunakan default.")
        return {
            "thresholds": {
                "risk_index":       {"warning": 60, "critical": 80},
                "price_change_pct_7d": {"warning": 10, "critical": 20},
            },
            "cooldown_hours": 12,
            "channels": {"dashboard": True, "log": True},
        }
    with open(CONFIG_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_cooldown() -> dict[str, str]:
    """
    Baca state cooldown: dict {komoditas: last_alert_timestamp_iso}.
    """
    if not COOLDOWN_PATH.exists():
        return {}
    try:
        with open(COOLDOWN_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_cooldown(cooldown: dict[str, str]) -> None:
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    with open(COOLDOWN_PATH, "w", encoding="utf-8") as f:
        json.dump(cooldown, f, ensure_ascii=False, indent=2)


def is_in_cooldown(komoditas: str, cooldown: dict, cooldown_hours: int) -> bool:
    """Kembalikan True jika komoditas masih dalam masa cooldown."""
    last_str = cooldown.get(komoditas)
    if not last_str:
        return False
    try:
        last_dt = datetime.fromisoformat(last_str)
        return datetime.now(timezone.utc) - last_dt < timedelta(hours=cooldown_hours)
    except Exception:
        return False


# ── Loader ────────────────────────────────────────────────────────────────────

def _load_json(path: Path) -> Any:
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ── Alert Checker ─────────────────────────────────────────────────────────────

def determine_alert_level(
    risk_index: float,
    price_change_pct: float | None,
    thresholds: dict,
) -> str | None:
    """
    Tentukan level alert berdasarkan risk_index dan perubahan harga.
    Kembalikan: 'KRITIS', 'SIAGA', 'WASPADA', atau None.
    """
    ri_thresh  = thresholds.get("risk_index", {})
    pct_thresh = thresholds.get("price_change_pct_7d", {})

    # Periksa risk index
    if risk_index >= ri_thresh.get("critical", 80):
        return "KRITIS"
    if risk_index >= ri_thresh.get("warning",  60):
        level = "SIAGA" if risk_index >= 70 else "WASPADA"
    else:
        level = None

    # Perubahan harga bisa meningkatkan level
    if price_change_pct is not None:
        if price_change_pct >= pct_thresh.get("critical", 20):
            if level in (None, "WASPADA", "SIAGA"):
                level = "KRITIS"
        elif price_change_pct >= pct_thresh.get("warning", 10):
            if level is None:
                level = "WASPADA"

    return level


def build_alert_message(
    komoditas: str,
    level: str,
    risk_index: float,
    price_change_pct: float | None,
    forecast_price: float | None,
    current_price:  float | None,
    top_signals:    list[str],
) -> dict[str, Any]:
    """Bangun dict pesan alert lengkap."""
    label      = KOMODITAS_LABEL.get(komoditas, komoditas)
    now        = datetime.now(timezone.utc)
    alert_date = now.strftime("%d %B %Y")

    # Bangun teks pesan
    msg_parts = [f"[{level}] {label} — Indeks Risiko: {risk_index:.1f}/100"]
    if price_change_pct is not None:
        direction = "naik" if price_change_pct > 0 else "turun"
        msg_parts.append(f"Perubahan harga 7 hari: {price_change_pct:+.1f}% ({direction})")
    if current_price and forecast_price:
        msg_parts.append(
            f"Proyeksi H+7: Rp{current_price:,.0f} → Rp{forecast_price:,.0f}"
        )
    if top_signals:
        msg_parts.append(f"Sinyal dominan: {', '.join(top_signals[:3])}")

    action_map = {
        "KRITIS":  "Segera lakukan koordinasi operasi pasar dan cek stok Bulog.",
        "SIAGA":   "Persiapkan cadangan distribusi dan pantau harga pasar harian.",
        "WASPADA": "Tingkatkan monitoring dan siapkan rencana intervensi.",
    }
    msg_parts.append(f"Rekomendasi: {action_map.get(level, '')}")

    return {
        "id":              f"{komoditas}_{now.strftime('%Y%m%d%H%M%S')}",
        "komoditas":       komoditas,
        "komoditas_label": label,
        "level":           level,
        "risk_index":      risk_index,
        "price_change_pct_7d": price_change_pct,
        "current_price":   current_price,
        "forecast_price_7d": forecast_price,
        "top_signals":     top_signals,
        "message":         " | ".join(msg_parts),
        "timestamp":       now.isoformat(),
        "date_label":      alert_date,
    }


# ── Main Engine ───────────────────────────────────────────────────────────────

def run_alert_engine() -> dict[str, Any]:
    """
    Baca semua data, evaluasi setiap komoditas, buat alert yang diperlukan,
    respek cooldown, dan simpan ke alerts.json.
    """
    config   = load_config()
    cooldown = load_cooldown()

    thresholds     = config.get("thresholds", {})
    cooldown_hours = config.get("cooldown_hours", 12)
    channels       = config.get("channels", {"dashboard": True, "log": True})

    risk_data     = _load_json(EXPORT_DIR / "risk_index.json")     or {}
    forecast_data = _load_json(EXPORT_DIR / "price_forecast.json") or {}
    nlp_data      = _load_json(EXPORT_DIR / "nlp_signals.json")    or {}

    risk_indices = risk_data.get("risk_indices", {})
    forecasts    = forecast_data.get("forecasts", {}) if forecast_data else {}
    nlp_velocity = nlp_data.get("velocity_per_komoditas", {})

    alerts_triggered: list[dict] = []
    alerts_suppressed: list[str] = []

    for komoditas in config.get("komoditas", list(KOMODITAS_LABEL.keys())):
        ri_data   = risk_indices.get(komoditas, {})
        risk_val  = ri_data.get("risk_index", 0.0)
        fc_data   = forecasts.get(komoditas, {})
        pct_change = fc_data.get("predicted_change_pct")
        curr_price = fc_data.get("current_price")
        fcst_price = fc_data.get("forecast_price_7d")

        # Cek top sinyal berita
        nlp_kom    = nlp_velocity.get(komoditas, {})
        top_sigs   = [s[0] for s in nlp_kom.get("top_signals", [])]

        level = determine_alert_level(risk_val, pct_change, thresholds)

        if level is None:
            continue  # tidak perlu alert

        # Cek cooldown
        if is_in_cooldown(komoditas, cooldown, cooldown_hours):
            alerts_suppressed.append(komoditas)
            log.info(
                f"  {komoditas}: [{level}] risiko={risk_val:.1f} — SUPPRESSED (cooldown)"
            )
            continue

        # Buat alert
        alert = build_alert_message(
            komoditas=komoditas,
            level=level,
            risk_index=risk_val,
            price_change_pct=pct_change,
            forecast_price=fcst_price,
            current_price=curr_price,
            top_signals=top_sigs,
        )
        alerts_triggered.append(alert)
        cooldown[komoditas] = datetime.now(timezone.utc).isoformat()

        # Log channel
        if channels.get("log", True):
            log.warning(f"ALERT [{level}] {komoditas}: {alert['message']}")

    # Simpan cooldown state
    save_cooldown(cooldown)

    # Susun output
    now = datetime.now(timezone.utc)
    output: dict[str, Any] = {
        "generated_at":       now.isoformat(),
        "total_alerts":       len(alerts_triggered),
        "suppressed":         alerts_suppressed,
        "alerts":             alerts_triggered,
        "summary": {
            "kritis":  sum(1 for a in alerts_triggered if a["level"] == "KRITIS"),
            "siaga":   sum(1 for a in alerts_triggered if a["level"] == "SIAGA"),
            "waspada": sum(1 for a in alerts_triggered if a["level"] == "WASPADA"),
        },
    }

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = EXPORT_DIR / "alerts.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info(
        f"Alert engine selesai: {len(alerts_triggered)} alert aktif, "
        f"{len(alerts_suppressed)} di-suppress. Disimpan ke {out_path}"
    )
    return output


if __name__ == "__main__":
    result = run_alert_engine()
    summary = result.get("summary", {})
    print(f"\n=== ALERT ENGINE RESULTS ===")
    print(
        f"KRITIS={summary.get('kritis',0)}  "
        f"SIAGA={summary.get('siaga',0)}  "
        f"WASPADA={summary.get('waspada',0)}  "
        f"(suppressed: {len(result.get('suppressed',[]))})"
    )
    if result["alerts"]:
        print("\nAlert aktif:")
        for a in result["alerts"]:
            print(f"  [{a['level']:7s}] {a['komoditas_label']:25s} risk={a['risk_index']:.1f}")
    sys.exit(0)
