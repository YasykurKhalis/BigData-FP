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
    "gula_pasir": [
        {"nama": "kediri",      "provinsi": "Jawa Timur",   "lat": -7.816,  "lon": 112.011, "bobot": 0.35},
        {"nama": "malang",      "provinsi": "Jawa Timur",   "lat": -7.978,  "lon": 112.634, "bobot": 0.30},
        {"nama": "lampung",     "provinsi": "Lampung",      "lat": -5.450,  "lon": 105.267, "bobot": 0.35},
    ],
    "minyak_goreng": [
        {"nama": "riau",        "provinsi": "Riau",              "lat":  0.508,  "lon": 101.448, "bobot": 0.40},
        {"nama": "kalbar",      "provinsi": "Kalimantan Barat",  "lat": -0.026,  "lon": 109.342, "bobot": 0.30},
        {"nama": "sumut",       "provinsi": "Sumatera Utara",    "lat":  2.116,  "lon":  99.545, "bobot": 0.30},
    ],
    "daging_ayam": [
        {"nama": "bogor",       "provinsi": "Jawa Barat",   "lat": -6.597,  "lon": 106.806, "bobot": 0.35},
        {"nama": "blitar",      "provinsi": "Jawa Timur",   "lat": -8.097,  "lon": 112.163, "bobot": 0.35},
        {"nama": "semarang",    "provinsi": "Jawa Tengah",  "lat": -6.966,  "lon": 110.420, "bobot": 0.30},
    ],
    "telur_ayam": [
        {"nama": "blitar",      "provinsi": "Jawa Timur",   "lat": -8.097,  "lon": 112.163, "bobot": 0.40},
        {"nama": "bandung",     "provinsi": "Jawa Barat",   "lat": -6.905,  "lon": 107.614, "bobot": 0.30},
        {"nama": "semarang",    "provinsi": "Jawa Tengah",  "lat": -6.966,  "lon": 110.420, "bobot": 0.30},
    ],
    "daging_sapi": [
        {"nama": "sumbawa",     "provinsi": "NTB",           "lat": -8.488,  "lon": 117.395, "bobot": 0.35},
        {"nama": "kupang",      "provinsi": "NTT",           "lat": -10.179, "lon": 123.607, "bobot": 0.30},
        {"nama": "tuban",       "provinsi": "Jawa Timur",    "lat": -6.899,  "lon": 112.049, "bobot": 0.35},
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
    "gula pasir": "gula_pasir",
    "gula": "gula_pasir",
    "minyak goreng": "minyak_goreng",
    "minyak": "minyak_goreng",
    "daging ayam": "daging_ayam",
    "ayam": "daging_ayam",
    "telur ayam": "telur_ayam",
    "telur": "telur_ayam",
    "daging sapi": "daging_sapi",
    "sapi": "daging_sapi",
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
