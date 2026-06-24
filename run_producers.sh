#!/usr/bin/env bash
# Run all Kafka producers once
cd "$(dirname "$0")"
python kafka/producer_price_bapanas.py --once
python kafka/producer_price_pihps.py --once
python kafka/producer_price_siskaperbapo.py --once
python kafka/producer_weather.py --once
python kafka/producer_news_pangan.py --once
python kafka/producer_kurs_bi.py --once
