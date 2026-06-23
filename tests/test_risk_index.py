"""LUMBUNG — Unit tests Risk Index & NLP Extractor. Owner: Yasykur"""

import sys
from pathlib import Path

# Tambahkan root project ke path agar bisa import ml modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


# ── Test compute_risk_index ───────────────────────────────────────────────────

from ml.compute_risk_index import (
    classify_risk,
    compute_price_signal,
    compute_weather_signal,
    compute_news_signal,
)


class TestClassifyRisk:
    def test_aman(self):
        assert classify_risk(30.0)  == "AMAN"
        assert classify_risk(0.0)   == "AMAN"
        assert classify_risk(39.9)  == "AMAN"

    def test_waspada(self):
        assert classify_risk(40.0) == "WASPADA"
        assert classify_risk(59.9) == "WASPADA"

    def test_siaga(self):
        assert classify_risk(60.0) == "SIAGA"
        assert classify_risk(79.9) == "SIAGA"

    def test_kritis(self):
        assert classify_risk(80.0)  == "KRITIS"
        assert classify_risk(100.0) == "KRITIS"


class TestPriceSignal:
    def _make_rows(self, prices, commodity="beras"):
        from datetime import datetime, timedelta, timezone
        today = datetime.now(timezone.utc).date()
        return [
            {
                "date_parsed": str(today - timedelta(days=len(prices) - 1 - i)),
                "commodity":   commodity,
                "avg_price":   p,
            }
            for i, p in enumerate(prices)
        ]

    def test_no_data_returns_midpoint(self):
        """Jika tidak ada data, sinyal harga harus mengembalikan 50."""
        result = compute_price_signal([], "beras")
        assert result == 50.0

    def test_stable_prices(self):
        """Harga stabil → sinyal di kisaran tengah (tidak ekstrem)."""
        rows = self._make_rows([10000] * 35)
        signal = compute_price_signal(rows, "beras")
        assert 0.0 <= signal <= 100.0

    def test_rising_prices_higher_signal(self):
        """Harga naik konsisten → sinyal lebih tinggi dari harga stabil."""
        stable_rows  = self._make_rows([10000] * 35)
        rising_rows  = self._make_rows(list(range(8000, 8000 + 35 * 200, 200)), "beras")
        stable_sig   = compute_price_signal(stable_rows,  "beras")
        rising_sig   = compute_price_signal(rising_rows,  "beras")
        assert rising_sig >= stable_sig

    def test_returns_bounded(self):
        """Output selalu antara 0 dan 100."""
        import random
        rng = random.Random(42)
        rows = self._make_rows([rng.randint(5000, 200000) for _ in range(40)])
        sig = compute_price_signal(rows, "beras")
        assert 0.0 <= sig <= 100.0


class TestWeatherSignal:
    def test_no_records_returns_zero(self):
        result = compute_weather_signal([], "beras", {"karawang": 1.0})
        assert result == 0.0

    def test_no_matching_sentra(self):
        """Record cuaca untuk sentra yang bukan sentra komoditas → skor 0."""
        records = [{"sentra": "xyz", "current": {"precipitation": 100}, "ingestion_ts": "2026-01-01"}]
        result  = compute_weather_signal(records, "beras", {"karawang": 1.0})
        assert result == 0.0

    def test_heavy_rain_raises_signal(self):
        """Curah hujan berat (>50mm) harus menghasilkan sinyal > 0."""
        records = [{
            "sentra": "karawang",
            "current": {
                "precipitation":    80.0,
                "weather_code":     80,
                "temperature_2m":   30.0,
            },
            "ingestion_ts": "2026-06-23T10:00:00",
        }]
        result = compute_weather_signal(records, "beras", {"karawang": 1.0})
        assert result > 0.0

    def test_normal_weather_low_signal(self):
        """Cuaca normal → sinyal rendah."""
        records = [{
            "sentra": "karawang",
            "current": {
                "precipitation":   0.0,
                "weather_code":    0,
                "temperature_2m":  28.0,
            },
            "ingestion_ts": "2026-06-23T10:00:00",
        }]
        result = compute_weather_signal(records, "beras", {"karawang": 1.0})
        assert result == 0.0

    def test_output_bounded(self):
        """Output tidak boleh melebihi 100."""
        records = [{
            "sentra": "karawang",
            "current": {
                "precipitation":   999.0,
                "weather_code":    99,
                "temperature_2m":  45.0,
            },
            "ingestion_ts": "2026-06-23T10:00:00",
        }]
        result = compute_weather_signal(records, "beras", {"karawang": 1.0})
        assert result <= 100.0


class TestNewsSignal:
    def test_empty_data_returns_zero(self):
        result = compute_news_signal({}, "beras")
        assert result == 0.0

    def test_high_velocity_raises_signal(self):
        """Velocity tinggi → sinyal berita tinggi."""
        nlp_data = {
            "velocity_per_komoditas": {
                "beras": {
                    "velocity":  4.0,   # 4x rata-rata
                    "avg_score": 12.0,
                }
            }
        }
        result = compute_news_signal(nlp_data, "beras")
        assert result > 60.0

    def test_low_velocity_low_signal(self):
        """Velocity rendah → sinyal berita rendah."""
        nlp_data = {
            "velocity_per_komoditas": {
                "beras": {
                    "velocity":  0.5,
                    "avg_score": 1.0,
                }
            }
        }
        result = compute_news_signal(nlp_data, "beras")
        assert result < 30.0

    def test_output_bounded(self):
        """Output tidak melebihi 100."""
        nlp_data = {
            "velocity_per_komoditas": {
                "beras": {"velocity": 999.0, "avg_score": 999.0}
            }
        }
        result = compute_news_signal(nlp_data, "beras")
        assert result <= 100.0


# ── Test NLP Extractor ────────────────────────────────────────────────────────

from ml.nlp_keyword_extractor import extract_signals, compute_velocity


class TestExtractSignals:
    def test_irrelevant_article_returns_empty(self):
        article = {
            "title":       "Pertandingan sepak bola nasional",
            "summary":     "Tim nasional menang 3-0 dalam laga persahabatan.",
            "ingestion_ts": "2026-06-23T10:00:00",
        }
        result = extract_signals(article)
        assert result == {}

    def test_relevant_article_detected(self):
        article = {
            "title":       "Harga beras naik tajam akibat kekeringan di sentra produksi Karawang",
            "summary":     "Petani di Karawang melaporkan gagal panen akibat el nino yang berkepanjangan.",
            "ingestion_ts": "2026-06-23T10:00:00",
        }
        result = extract_signals(article)
        assert result != {}
        assert "beras" in result.get("komoditas_matched", [])
        assert result["signal_score"] > 0

    def test_sentiment_bullish_on_price_spike(self):
        article = {
            "title":       "Harga cabai rawit melonjak Rp50.000 akibat kelangkaan",
            "summary":     "Pasokan cabai rawit langka karena banjir di Brebes.",
            "ingestion_ts": "2026-06-23T10:00:00",
        }
        result = extract_signals(article)
        assert result.get("sentiment") == "bullish"

    def test_gangguan_detected(self):
        article = {
            "title":       "Banjir bandang merendam sawah beras di Karawang",
            "summary":     "Ribuan hektar lahan padi terendam banjir bandang.",
            "ingestion_ts": "2026-06-23T10:00:00",
        }
        result = extract_signals(article)
        assert "banjir" in result.get("gangguan_signals", {}) or \
               "banjir bandang" in result.get("gangguan_signals", {})


class TestComputeVelocity:
    def _make_signals(self, komoditas, dates_and_scores):
        """Buat list sinyal dummy."""
        signals = []
        for date, score in dates_and_scores:
            signals.append({
                "komoditas_matched": [komoditas],
                "date":              date,
                "signal_score":      score,
                "gangguan_signals":  {},
            })
        return signals

    def test_empty_signals(self):
        result = compute_velocity([])
        assert "beras" in result
        assert result["beras"]["today_count"] == 0

    def test_velocity_calculation(self):
        from datetime import datetime, timedelta, timezone
        today = datetime.now(timezone.utc).date()

        signals = self._make_signals("beras", [
            (str(today - timedelta(days=3)), 5),
            (str(today - timedelta(days=2)), 5),
            (str(today - timedelta(days=1)), 5),
        ])
        result = compute_velocity(signals)
        assert result["beras"]["recent_avg"] >= 0
        assert isinstance(result["beras"]["velocity"], float)
