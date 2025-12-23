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
    RESULTS_DIR,
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
    layout="wide",
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

st.sidebar.markdown(
    f"""
**Current Master Data Range:**
*   Start: `{current_start}`
*   End:   `{current_end}`
"""
)

st.sidebar.divider()

# -------------------------------
# 2. Data Update Mode (Toggle)
# -------------------------------

st.sidebar.subheader("📥 Data Operations")

update_data_mode = st.sidebar.checkbox(
    "Download & Append New Data?",
    value=True,
)

if update_data_mode:
    st.sidebar.success("Mode: Fetching new Bhavcopies")

    download_start_date = st.sidebar.text_input(
        "Download From (DD-MMM-YYYY)",
        "06-Dec-2025",
    )

    download_end_date = st.sidebar.text_input(
        "Download To (DD-MMM-YYYY)",
        "19-Dec-2025",
    )
else:
    st.sidebar.warning("Mode: Using Existing Master CSV (No Download)")
    download_start_date = None
    download_end_date = None

st.sidebar.divider()

# -------------------------------
# 3. Strategy Settings
# -------------------------------

st.sidebar.subheader("⚙ Strategy Settings")

decision_date_ui = st.sidebar.date_input(
    "Decision Date (After Market Close)",
    value=DECISION_DATE.date(),
)

entry_date_ui = pd.to_datetime(decision_date_ui) + pd.tseries.offsets.BDay(1)

st.sidebar.markdown(
    f"""
**Entry Date (Auto):**  
`{entry_date_ui.date()}`
"""
)

st.sidebar.divider()

# -------------------------------
# 4. Training Control
# -------------------------------

st.sidebar.subheader("🧠 Model Operations")

training_mode = st.sidebar.radio(
    "Select Model Strategy:",
    (
        "Use Existing Pre-Trained Models",
        "Retrain & Save New Models",
    ),
    index=0,
)

st.sidebar.divider()

# -------------------------------
# Action Buttons
# -------------------------------

run_pipeline = st.sidebar.button("▶ Run Quant Pipeline")
run_performance = st.sidebar.button("📊 Run Weekly Performance")

# ============================================================
# RUN PIPELINE LOGIC
# ============================================================

if run_pipeline:
    st.subheader("🔄 Running Quant Pipeline")

    # ----------------------------------------
    # STEP 1: DOWNLOAD & UPDATE (OPTIONAL)
    # ----------------------------------------
    if update_data_mode:
        with st.spinner(
            f"Fetching data from {download_start_date} to {download_end_date}..."
        ):
            try:
                run_daily_data_pipeline(
                    download_start_date,
                    download_end_date,
                )
                st.success(
                    "✔ Data downloaded, merged, and appended to Master CSV"
                )
            except Exception as e:
                st.error(f"✖ Data Pipeline Failed: {e}")
                st.stop()
    else:
        st.info(
            "ℹ Skipping download. Proceeding with existing Master CSV."
        )

    # ----------------------------------------
    # STEP 2: CLEANING & PRE-PROCESSING
    # ----------------------------------------

    with st.spinner(
        "Processing Corporate Actions & Bad Ticks..."
    ):
        try:
            df_clean = clean_corporate_events(MASTER_CSV)
            st.success("✔ Corporate events cleaned")
        except FileNotFoundError:
            st.error(
                "✖ Master CSV not found. Please enable 'Download' to create it."
            )
            st.stop()

    # ----------------------------------------
    # STEP 3: REGIMES
    # ----------------------------------------

    with st.spinner("Integrating Market Regimes..."):
        df_regime = integrate_regimes(
            df_clean,
            REGIME_TABLE,
            MACRO_MAP,
        )
        st.success("✔ Regimes integrated")

    # ----------------------------------------
    # STEP 4: FEATURE ENGINEERING
    # ----------------------------------------

    with st.spinner("Calculating Alpha Features..."):
        df_feat = add_features(df_regime)
        st.success("✔ Features built")

    # ----------------------------------------
    # STEP 5: MODEL TRAINING (CONDITIONAL)
    # ----------------------------------------

    if training_mode == "Retrain & Save New Models":
        with st.spinner(
            "Training new models on historical data..."
        ):
            train_df = df_feat[df_feat["DATE"] <= DECISION_DATE]
            train_and_save_models(train_df)
            st.success(
                "✔ Models retrained & saved to disk"
            )
    else:
        st.info("ℹ Using existing models from disk")

    # ----------------------------------------
    # STEP 6: LIVE INFERENCE (TRADES)
    # ----------------------------------------

    with st.spinner(
        "Running Inference (Champion/Challenger)..."
    ):
        try:
            trade_sheet = live_trade_decision(df_feat)
            st.success("🚀 LIVE trades generated")

            st.subheader("📌 LIVE TRADES")
            st.dataframe(
                trade_sheet,
                use_container_width=True,
            )

            trade_path = (
                f"{RESULTS_DIR}/LIVE_TRADES_{ENTRY_DATE.date()}.csv"
            )
            trade_sheet.to_csv(
                trade_path,
                index=False,
            )
            st.info(f"Saved → {trade_path}")

        except Exception as e:
            st.error(f"✖ Trade Generation Failed: {e}")

# ============================================================
# RUN WEEKLY PERFORMANCE
# ============================================================

if run_performance:
    st.subheader("📊 Weekly Performance Check")

    try:
        summary, model_returns = run_weekly_performance_check(
            exit_date=ENTRY_DATE + pd.Timedelta(days=4),
        )

        st.subheader("📈 Performance Summary")
        st.dataframe(
            summary,
            use_container_width=True,
        )

        st.subheader("📉 Model Stock Returns")
        st.dataframe(
            model_returns,
            use_container_width=True,
        )

        st.success("✔ Weekly evaluation completed")
    except Exception as e:
        st.error(f"Performance Check Failed: {e}")

# ============================================================
# MASTER DATA PREVIEW
# ============================================================

st.divider()
st.subheader("📂 Master FNO Data (Latest Snapshot)")

try:
    df_master = pd.read_csv(
        MASTER_CSV,
        low_memory=False,
    )
    st.dataframe(
        df_master.tail(100),
        use_container_width=True,
    )
except Exception:
    st.warning("Master CSV not found yet")
