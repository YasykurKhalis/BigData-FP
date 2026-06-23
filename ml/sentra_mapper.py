"""
LUMBUNG — Mapping komoditas → sentra produksi utama
Owner: Hanif

Menyediakan informasi geografis sentra produksi per komoditas
untuk digunakan oleh risk index, peta dashboard, dan alert engine.
"""

from __future__ import annotations

# Mapping komoditas → daftar sentra produksi utama beserta koordinat & bobot
SENTRA_MAP: dict[str, list[dict]] = {
    "beras": [
        {"nama": "karawang",    "provinsi": "Jawa Barat",   "lat": -6.305,  "lon": 107.305, "bobot": 0.35},
        {"nama": "cianjur",     "provinsi": "Jawa Barat",   "lat": -6.817,  "lon": 107.142, "bobot": 0.20},
        {"nama": "banyuwangi",  "provinsi": "Jawa Timur",   "lat": -8.219,  "lon": 114.369, "bobot": 0.15},
        {"nama": "indramayu",   "provinsi": "Jawa Barat",   "lat": -6.329,  "lon": 108.323, "bobot": 0.20},
        {"nama": "sragen",      "provinsi": "Jawa Tengah",  "lat": -7.424,  "lon": 111.030, "bobot": 0.10},
    ],
    "cabai_rawit_merah": [
        {"nama": "brebes",      "provinsi": "Jawa Tengah",  "lat": -6.872,  "lon": 109.046, "bobot": 0.30},
        {"nama": "temanggung",  "provinsi": "Jawa Tengah",  "lat": -7.308,  "lon": 110.162, "bobot": 0.25},
        {"nama": "blitar",      "provinsi": "Jawa Timur",   "lat": -8.097,  "lon": 112.163, "bobot": 0.20},
        {"nama": "probolinggo", "provinsi": "Jawa Timur",   "lat": -7.754,  "lon": 113.215, "bobot": 0.15},
        {"nama": "magelang",    "provinsi": "Jawa Tengah",  "lat": -7.475,  "lon": 110.218, "bobot": 0.10},
    ],
    "cabai_keriting": [
        {"nama": "brebes",      "provinsi": "Jawa Tengah",  "lat": -6.872,  "lon": 109.046, "bobot": 0.35},
        {"nama": "wonosobo",    "provinsi": "Jawa Tengah",  "lat": -7.360,  "lon": 109.900, "bobot": 0.25},
        {"nama": "batu",        "provinsi": "Jawa Timur",   "lat": -7.868,  "lon": 112.528, "bobot": 0.20},
        {"nama": "garut",       "provinsi": "Jawa Barat",   "lat": -7.211,  "lon": 107.906, "bobot": 0.20},
    ],
    "bawang_merah": [
        {"nama": "brebes",      "provinsi": "Jawa Tengah",  "lat": -6.872,  "lon": 109.046, "bobot": 0.40},
        {"nama": "magelang",    "provinsi": "Jawa Tengah",  "lat": -7.475,  "lon": 110.218, "bobot": 0.25},
        {"nama": "nganjuk",     "provinsi": "Jawa Timur",   "lat": -7.604,  "lon": 111.900, "bobot": 0.20},
        {"nama": "solok",       "provinsi": "Sumatra Barat","lat": -0.798,  "lon": 100.652, "bobot": 0.15},
    ],
    "bawang_putih": [
        {"nama": "temanggung",  "provinsi": "Jawa Tengah",  "lat": -7.308,  "lon": 110.162, "bobot": 0.30},
        {"nama": "wonosobo",    "provinsi": "Jawa Tengah",  "lat": -7.360,  "lon": 109.900, "bobot": 0.25},
        {"nama": "tegal",       "provinsi": "Jawa Tengah",  "lat": -6.869,  "lon": 109.127, "bobot": 0.25},
        {"nama": "solok",       "provinsi": "Sumatra Barat","lat": -0.798,  "lon": 100.652, "bobot": 0.20},
    ],
}

# Normalisasi alias → canonical key
ALIAS_MAP: dict[str, str] = {
    "cabai": "cabai_rawit_merah",
    "cabe":  "cabai_rawit_merah",
    "rawit": "cabai_rawit_merah",
    "cabai rawit": "cabai_rawit_merah",
    "cabai rawit merah": "cabai_rawit_merah",
    "cabai keriting": "cabai_keriting",
    "cabai merah keriting": "cabai_keriting",
    "bawang": "bawang_merah",
    "bawang merah": "bawang_merah",
    "bawang putih": "bawang_putih",
    "beras": "beras",
    "padi": "beras",
    "gabah": "beras",
}


def normalize_komoditas(name: str) -> str | None:
    """Normalisasi nama komoditas ke canonical key."""
    key = name.lower().strip()
    return ALIAS_MAP.get(key, key if key in SENTRA_MAP else None)


def get_sentra(komoditas: str) -> list[dict]:
    """Kembalikan list sentra untuk komoditas tertentu (sudah ternormalisasi)."""
    key = normalize_komoditas(komoditas)
    if key is None:
        return []
    return SENTRA_MAP.get(key, [])


def get_all_sentra_flat() -> list[dict]:
    """
    Kembalikan semua sentra dalam format flat list untuk visualisasi peta.
    Setiap record berisi: komoditas, nama, provinsi, lat, lon, bobot.
    """
    result = []
    for komoditas, sentras in SENTRA_MAP.items():
        for s in sentras:
            result.append({**s, "komoditas": komoditas})
    return result


def get_sentra_names_for_weather() -> set[str]:
    """Kembalikan set nama sentra yang digunakan oleh weather producer."""
    all_names: set[str] = set()
    for sentras in SENTRA_MAP.values():
        for s in sentras:
            all_names.add(s["nama"])
    return all_names


if __name__ == "__main__":
    print("=== Daftar Sentra Produksi ===")
    for komoditas, sentras in SENTRA_MAP.items():
        print(f"\n{komoditas.upper()}:")
        for s in sentras:
            print(f"  - {s['nama']} ({s['provinsi']}) | bobot={s['bobot']}")
    print(f"\nTotal komoditas: {len(SENTRA_MAP)}")
    print(f"Total sentra unik: {len(get_sentra_names_for_weather())}")
