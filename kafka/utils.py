"""
LUMBUNG — Kafka utilities
Owner: Ryan

Helper bersama untuk semua producer:
  - bootstrap server config
  - serializer JSON
  - retry wrapper
  - logging helper
"""

KAFKA_BOOTSTRAP = "localhost:9092"

SENTRA_PRODUKSI = {
    "brebes":      {"lat": -6.872, "lon": 109.046, "komoditas": ["cabai", "bawang"]},
    "karawang":    {"lat": -6.305, "lon": 107.305, "komoditas": ["beras"]},
    "magelang":    {"lat": -7.475, "lon": 110.218, "komoditas": ["bawang"]},
    "cianjur":     {"lat": -6.817, "lon": 107.142, "komoditas": ["beras"]},
    "probolinggo": {"lat": -7.754, "lon": 113.215, "komoditas": ["bawang"]},
}

KOMODITAS = [
    "beras",
    "cabai_rawit_merah",
    "cabai_keriting",
    "bawang_merah",
    "bawang_putih",
]
