@echo off
REM ============================================================
REM LUMBUNG - One-click demo script (Windows)
REM Owner: Ryan (5027231046)
REM
REM Memerlukan: Docker Desktop running, Python 3.13, lib LUMBUNG terinstall
REM ============================================================

setlocal
chcp 65001 > nul
cd /d "%~dp0"

echo.
echo ============================================================
echo  LUMBUNG - DEMO PIPELINE H-6
echo ============================================================
echo.

echo [1/6] Cek Hadoop containers...
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
echo [2/6] Cek Kafka container...
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
echo [3/6] Setup 6 Kafka topics...
python setup_kafka_topics.py

echo.
echo [4/6] Smoke test HDFS connectivity...
python hdfs\test_hdfs_connection.py
if errorlevel 1 (
    echo.
    echo  ERROR: HDFS smoke test gagal. Abort demo.
    pause
    exit /b 1
)

echo.
echo [5/6] Producer cuaca - fetch 5 sentra -^> Kafka...
python kafka\producer_weather.py --once

echo.
echo [6/6] Consumer Kafka -^> HDFS...
python hdfs\consumer_to_hdfs.py --once

echo.
echo ============================================================
echo  DEMO SELESAI - BUKA DI BROWSER:
echo.
echo  NameNode UI : http://localhost:9870
echo  Data hari ini: http://localhost:9870/explorer.html#/data/lumbung/streaming/weather
echo  YARN RM UI  : http://localhost:8088
echo ============================================================
echo.
pause
