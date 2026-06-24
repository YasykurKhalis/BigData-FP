@echo off
REM ============================================================
REM LUMBUNG - One-click demo script (Windows)
REM Owner: Ryan (5027231046)
REM
REM Memerlukan: Docker Desktop running, Python 3.13, lib terinstall
REM ============================================================

setlocal
chcp 65001 > nul
cd /d "%~dp0"

echo.
echo ============================================================
echo  LUMBUNG - DEMO PIPELINE
echo ============================================================
echo.

echo [1/9] Cek Hadoop containers...
docker ps --filter name=lumbung-namenode --format "  {{.Names}}  {{.Status}}" | findstr "Up" > nul
if errorlevel 1 (
    echo   -^> Hadoop down, starting...
    docker compose -f docker-compose-hadoop.yml up -d
    echo   -^> waiting 60s untuk warm-up...
    timeout /t 60 /nobreak > nul
) else (
    echo   -^> Hadoop sudah jalan, skip.
)

echo.
echo [2/9] Cek Kafka container...
docker ps --filter name=lumbung-kafka --format "  {{.Names}}  {{.Status}}" | findstr "Up" > nul
if errorlevel 1 (
    echo   -^> Kafka down, starting...
    docker compose -f docker-compose-kafka.yml up -d
    echo   -^> waiting 25s...
    timeout /t 25 /nobreak > nul
) else (
    echo   -^> Kafka sudah jalan, skip.
)

echo.
echo [3/9] Setup 6 Kafka topics...
python setup_kafka_topics.py

echo.
echo [4/9] Smoke test HDFS connectivity...
python hdfs\test_hdfs_connection.py
if errorlevel 1 (
    echo.
    echo  ERROR: HDFS smoke test gagal. Abort demo.
    pause
    exit /b 1
)

echo.
echo [5/9] Seed historical prices (180 hari) -^> HDFS...
python lakehouse\seed_historical_prices.py

echo.
echo [6/9] 6 Producers -^> Kafka...
python kafka\producer_price_bapanas.py --once
python kafka\producer_price_pihps.py --once
python kafka\producer_price_siskaperbapo.py --once
python kafka\producer_weather.py --once
python kafka\producer_news_pangan.py --once
python kafka\producer_kurs_bi.py --once

echo.
echo [7/9] Consumer Kafka -^> HDFS...
python hdfs\consumer_to_hdfs.py --once

echo.
echo [8/9] Lakehouse Pipeline (Bronze -^> Silver -^> Gold)...
python lakehouse\01_bronze.py
python lakehouse\02_silver.py
python lakehouse\03_gold.py

echo.
echo [9/9] Dashboard...
start "LUMBUNG Dashboard" python dashboard\app.py

echo.
echo ============================================================
echo  DEMO SELESAI - BUKA DI BROWSER:
echo.
echo  Dashboard   : http://localhost:5000
echo  NameNode UI : http://localhost:9870
echo  HDFS Data   : http://localhost:9870/explorer.html#/data/lumbung
echo  YARN RM UI  : http://localhost:8088
echo ============================================================
echo.
pause
