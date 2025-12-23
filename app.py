# ============================================================
# app.py
# Streamlit Quant Monitoring Platform (ORCHESTRATION ONLY)
# ============================================================

import streamlit as st
import pandas as pd

from config import (
    DECISION_DATE,
    ENTRY_DATE,
    MASTER_CSV,
    REGIME_TABLE,
    MACRO_MAP,
    RESULTS_DIR
)

from data_pipeline import run_daily_data_pipeline, get_master_date_range
from corporate_cleaner import clean_corporate_events
from regime_engine import integrate_regimes
from feature_engineer import add_features
from model_engine import train_and_save_models
from trade_engine import live_trade_decision
from performance_engine import run_weekly_performance_check

# ============================================================
# STREAMLIT PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Quant Monitoring System",
    layout="wide"
)

st.title("📈 Quant Trading – Real-Time Monitoring")

# ============================================================
# SIDEBAR
# ============================================================
st.sidebar.header("📅 Data Controls")

# -------------------------------
# 1. Show Current Master Range
# -------------------------------
current_start, current_end = get_master_date_range()

st.sidebar.markdown(f"""
**Current Master Data Range:**
*   Start: `{current_start}`
*   End:   `{current_end}`
""")

st.sidebar.divider()

# -------------------------------
# 2. Bhavcopy fetch inputs
# -------------------------------
st.sidebar.subheader("📥 Download New Data")
st.sidebar.info("Select range to fetch and append to Master CSV")

download_start_date = st.sidebar.text_input(
    "Download From (DD-MMM-YYYY)",
    "06-Dec-2025"
)

download_end_date = st.sidebar.text_input(
    "Download To (DD-MMM-YYYY)",
    "19-Dec-2025"
)

st.sidebar.divider()

# -------------------------------
# 3. Decision & Entry dates
# -------------------------------
st.sidebar.subheader("⚙ Strategy Settings")

decision_date_ui = st.sidebar.date_input(
    "Decision Date (After Market Close)",
    value=DECISION_DATE.date()
)

entry_date_ui = pd.to_datetime(decision_date_ui) + pd.tseries.offsets.BDay(1)

st.sidebar.markdown(
    f"""
    **Entry Date (Auto):**  
    `{entry_date_ui.date()}`
    """
)

# -------------------------------
# 4. Training Control
# -------------------------------
st.sidebar.divider()
training_mode = st.sidebar.radio(
    "🧠 Model Strategy",
    ("Use Existing Models", "Retrain & Save New Models"),
    index=0
)

st.sidebar.divider()

run_pipeline = st.sidebar.button("▶ Run Pipeline")
run_performance = st.sidebar.button("📊 Run Weekly Performance")

# ============================================================
# RUN PIPELINE
# ============================================================

if run_pipeline:
    st.subheader("🔄 Running Quant Pipeline")

    # 1. Update Data
    with st.spinner(f"Fetching data from {download_start_date} to {download_end_date}..."):
        run_daily_data_pipeline(download_start_date, download_end_date)
        st.success("✔ Data fetched and appended")

    # 2. Clean Corporate
    with st.spinner("Cleaning corporate actions..."):
        df_clean = clean_corporate_events(MASTER_CSV)
        st.success("✔ Corporate events cleaned")

    # 3. Regimes
    with st.spinner("Integrating regimes..."):
        df_regime = integrate_regimes(
            df_clean,
            REGIME_TABLE,
            MACRO_MAP
        )
        st.success("✔ Regimes integrated")

    # 4. Feature Engineering
    with st.spinner("Building features..."):
        df_feat = add_features(df_regime)
        st.success("✔ Features built")

    # 5. Model Training (CONDITIONAL)
    train_df = df_feat[df_feat["DATE"] <= DECISION_DATE]

    if training_mode == "Retrain & Save New Models":
        with st.spinner("Training new models..."):
            train_and_save_models(train_df)
            st.success("✔ Models retrained & saved")
    else:
        st.info("ℹ Using existing pre-trained models (Skipping training)")

    # 6. Trade Generation
    with st.spinner("Generating live trades..."):
        # We pass df_feat. logic inside uses features at DECISION_DATE
        trade_sheet = live_trade_decision(df_feat)
        st.success("🚀 LIVE trades generated")

    # Display Results
    st.subheader("📌 LIVE TRADES")
    st.dataframe(trade_sheet, use_container_width=True)

    trade_path = f"{RESULTS_DIR}/LIVE_TRADES_{ENTRY_DATE.date()}.csv"
    trade_sheet.to_csv(trade_path, index=False)

    st.info(f"Saved → {trade_path}")

# ============================================================
# RUN WEEKLY PERFORMANCE
# ============================================================

if run_performance:
    st.subheader("📊 Weekly Performance Check")

    summary, model_returns = run_weekly_performance_check(
        exit_date=ENTRY_DATE + pd.Timedelta(days=4)
    )

    st.subheader("📈 Performance Summary")
    st.dataframe(summary, use_container_width=True)

    st.subheader("📉 Model Stock Returns")
    st.dataframe(model_returns, use_container_width=True)

    st.success("✔ Weekly evaluation completed")

# ============================================================
# MASTER DATA PREVIEW
# ============================================================

st.divider()
st.subheader("📂 Master FNO Data (Latest Snapshot)")

try:
    df_master = pd.read_csv(MASTER_CSV, low_memory=False)
    st.dataframe(df_master.tail(100), use_container_width=True)
except:
    st.warning("Master CSV not found yet")