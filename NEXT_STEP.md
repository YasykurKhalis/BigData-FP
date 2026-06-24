# NEXT_STEP.md

## What has been done in this workspace

1. **Integrated HDFS push logic** into the bronze, silver and gold scripts, removing the separate `push_lakehouse_to_hdfs.*` helpers.
2. **Clean‑up**: deleted the obsolete `batch_ingest/` directory and any redundant scripts that were no longer required.
3. **Fixed column‑name compatibility** in several ML modules:
   - Added alias handling for the historic `commodity` → `komoditas` column in `feature_importance.py` and `evaluation.py`.
4. **Resolved `UnboundLocalError`** in `ml/evaluation.py` by:
   - Renaming the `commodity` column on the full DataFrame (`df_all`) before filtering.
   - Using the correctly scoped `df_kom` variable for subsequent processing.
5. **Adjusted dummy‑data generator** (conceptually) to produce a full year (365 days) of synthetic records so that the `MIN_ROWS_REQUIRED = 30` threshold is always satisfied during early testing.
6. **Added detailed guidance** on the required data schema for the ML pipeline (date, komoditas, avg_price, optional weather and news features) and explained why each top‑level data source (Food Price, Weather, News) is needed.
7. **Created `NEXT_STEP.md`** (this file) to capture the current state and outline the work that must be completed by the next contributor.

## 🚀 ROADMAP TO COMPLETION: The Grand Vision

The ultimate goal of this project is to build an **Intelligent Food Security Command Center**. This system will not only monitor prices and supply but fundamentally predict market volatility, generate mitigative policy recommendations via LLM, and provide real-time alerts to stakeholders to ensure national food stability.

### Phase 1: Real-World Data Integration (Immediate Action)
- [ ] **Acquire the public datasets recommended**: Food-price (FAO, PIHPS, Bapanas), Weather (BMKG), and News-Sentiment datasets.
- [ ] **Place raw files & pipeline implementation**: Build `raw_data/` and implement the **Bronze ingest script** (`lakehouse/01_bronze.py`) to map data to the standard schema (`date_parsed`, `komoditas`, `avg_price`, `precipitation`, `temperature`, `news_score`).
- [ ] **Verify end‑to‑end batch flow**: Run `orchestrate.py` and confirm `ml/evaluation.py` generates meaningful baselines without relying on synthetic data.

### Phase 2: ML Model Calibration & Advanced Analytics
- [ ] **Train High-Fidelity Forecasting Models**: Shift from baseline evaluation to robust model training (`train_price_forecast.py`, `train_magnitude_model.py`, `train_timing_classifier.py`). Use real historical data to tune hyperparameters.
- [ ] **Deploy the Multidimensional Risk Index**: Refine `ml/compute_risk_index.py` using accurate feature importance weights. Unify sentiment, weather shocks, and price trends.
- [ ] **LLM Recommendation Engine Integration**: Connect `ml/recommendation_llm.py` so that extreme spikes predicted by the models automatically trigger contextualized mitigation strategies (e.g., "Operasi Pasar", "Impor Logistik") for decision-makers.

### Phase 3: Real-Time Stream Processing & Orchestration
- [ ] **Activate Kafka Producers & Consumers**: Transition from purely batch to hybrid streaming. Spin up the containers (`docker-compose-kafka.yml`) and ensure `run_producers.sh` flawlessly streams live BI exchange rates, public food prices, weather metrics, and news.
- [ ] **Streaming into Lakehouse/HDFS**: Deploy `consumer_to_hdfs.py` to ingest real-time data efficiently into the Bronze layer schema.
- [ ] **Robust Orchestration**: Finalize Airflow DAG configurations (`dags/lumbung_batch_dag.py`) for fault-tolerant unattended daily runs.

### Phase 4: Command Center Dashboard & Active Alerting
- [ ] **Launch the Web Dashboard View**: Run the `dashboard/app.py` platform. Link the web views and APIs to the optimized Gold layer datasets. Ensure `sentra_mapper.py` displays insightful geographical data.
- [ ] **Deploy the Alert Delivery System**: Verify `alerts/alert_engine.py` continually surveys trends and ML forecasts. Set up automated notifications according to `alerts/alert_config.yml`.

### Phase 5: Scalability & Final Validation 
- [ ] **Infrastructure Scaling (Hadoop)**: Validate data replication and fault tolerance across the Hadoop cluster via `docker-compose-hadoop.yml` ensuring our data lakehouse scales gracefully.
- [ ] **End-to-End Validation**: Run `tests/` and evaluate pipeline integrity using `validation/validate_pipeline.py`.
- [ ] **Project Sign-Off**: Delivery of the intelligent Big Data ecosystem.

---
*By completing these phases, "Lumbung" will transcend from a data collection pipeline into a fully functional, visionary Big Data ecosystem capable of actively driving data-informed agricultural, economic, and food-security policies.*
