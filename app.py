# ============================================================
# app.py
# Streamlit Quant Monitoring Platform (ORCHESTRATION ONLY)
# ============================================================

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from config import (
    DEFAULT_DECISION_DATE, # Only used as a default UI value
    MASTER_CSV,
    REGIME_TABLE,
    MACRO_MAP,
    RESULTS_DIR
)

from data_pipeline import (
    run_daily_data_pipeline, 
    get_master_date_range, 
    fetch_monitoring_data
)
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
# TABS ARCHITECTURE
# ============================================================

tab_strategy, tab_monitor = st.tabs([
    "🚀 Strategy Pipeline", 
    "⏱ Realtime Monitoring (4-Weeks)"
])

# ============================================================
# TAB 1: STRATEGY PIPELINE
# ============================================================

with tab_strategy:
    
    # --- SIDEBAR FOR TAB 1 ---
    st.sidebar.header("📅 Strategy Controls")
    
    # 1. Master Data Info
    current_start, current_end = get_master_date_range()
    st.sidebar.markdown(f"**Master Range:** `{current_start}` to `{current_end}`")
    st.sidebar.divider()

    # 2. Update Toggle
    update_data_mode = st.sidebar.checkbox("Download & Append New Data?", value=True, key="chk_update")
    
    if update_data_mode:
        # Default dates for download convenience
        def_start = (datetime.now() - timedelta(days=14)).strftime("%d-%b-%Y")
        def_end = datetime.now().strftime("%d-%b-%Y")

        download_start_date = st.sidebar.text_input("Download From", def_start, key="dl_start")
        download_end_date = st.sidebar.text_input("Download To", def_end, key="dl_end")
    else:
        download_start_date = None
        download_end_date = None

    st.sidebar.divider()

    # 3. Decision Date (CRITICAL: TAKEN FROM UI)
    st.sidebar.subheader("⚙ Strategy Settings")
    
    ui_decision_date = st.sidebar.date_input(
        "Decision Date (After Market Close)", 
        value=DEFAULT_DECISION_DATE.date(), 
        key="ui_dec_date"
    )
    
    # Convert UI date to Timestamp for backend
    ts_decision_date = pd.Timestamp(ui_decision_date)
    
    # Calculate Entry Date (Next BDay)
    ts_entry_date = ts_decision_date + pd.tseries.offsets.BDay(1)
    
    st.sidebar.info(f"Auto Entry Date: {ts_entry_date.date()}")
    st.sidebar.divider()

    # 4. Training Mode
    training_mode = st.sidebar.radio(
        "Model Strategy:",
        ("Use Existing Pre-Trained Models", "Retrain & Save New Models"),
        index=0,
        key="rad_train"
    )

    st.sidebar.divider()
    
    # 5. Action Buttons
    btn_run_pipeline = st.sidebar.button("▶ Run Strategy Pipeline")
    btn_perf_check = st.sidebar.button("📊 Run Performance Check")

    # --- MAIN CONTENT FOR TAB 1 ---

    if btn_run_pipeline:
        st.subheader(f"🔄 Running Pipeline for Decision Date: {ui_decision_date}")

        # 1. Download
        if update_data_mode:
            with st.spinner(f"Fetching {download_start_date} to {download_end_date}..."):
                try:
                    run_daily_data_pipeline(download_start_date, download_end_date)
                    st.success("✔ Data downloaded & appended")
                except Exception as e:
                    st.error(f"Pipeline Error: {e}")
                    st.stop()
        
        # 2. Clean
        with st.spinner("Cleaning Data..."):
            try:
                df_clean = clean_corporate_events(MASTER_CSV)
                st.success("✔ Cleaned")
            except:
                st.error("Master CSV missing."); st.stop()

        # 3. Regime (Pass UI Date)
        with st.spinner("Integrating Regimes..."):
            df_regime = integrate_regimes(
                df_clean, 
                REGIME_TABLE, 
                MACRO_MAP, 
                cutoff_date=ts_decision_date # <--- UI Date Used
            )
            st.success("✔ Regimes Done")

        # 4. Features
        with st.spinner("Building Features..."):
            df_feat = add_features(df_regime)
            st.success("✔ Features Done")

        # 5. Train (Pass UI Date)
        if training_mode == "Retrain & Save New Models":
            with st.spinner("Retraining..."):
                train_df = df_feat[df_feat["DATE"] <= ts_decision_date] # <--- UI Date Used
                train_and_save_models(train_df)
                st.success("✔ Retrained")
        
        # 6. Trade (Pass UI Date)
        with st.spinner("Generating Trades..."):
            try:
                trade_sheet = live_trade_decision(
                    df_feat, 
                    decision_date=ts_decision_date, # <--- UI Date Used
                    entry_date=ts_entry_date        # <--- UI Date Used
                )
                st.success("🚀 Trades Generated")
                st.dataframe(trade_sheet, use_container_width=True)
                
                out_file = f"{RESULTS_DIR}/LIVE_TRADES_{ts_entry_date.date()}.csv"
                trade_sheet.to_csv(out_file, index=False)
                st.info(f"Saved: {out_file}")
            except Exception as e:
                st.error(f"Trade Error: {e}")

    if btn_perf_check:
        st.subheader("📊 Performance Check")
        try:
            # Pass UI Date to performance engine
            summary, model_returns = run_weekly_performance_check(ts_decision_date)
            st.dataframe(summary, use_container_width=True)
            st.dataframe(model_returns, use_container_width=True)
        except Exception as e:
            st.error(f"Performance Error: {e}")

    # --- STOCK VISUALIZER (MASTER DATA) ---
    st.divider()
    st.markdown("#### 📂 Master Data Visualizer")
    try:
        df_m = pd.read_csv(MASTER_CSV, low_memory=False)
        if "DATE1" in df_m.columns: df_m["D"] = pd.to_datetime(df_m["DATE1"])
        else: df_m["D"] = pd.to_datetime(df_m["DATE"])
        
        syms = sorted(df_m["SYMBOL"].unique())
        col1, col2 = st.columns([1,3])
        with col1:
            sel = st.selectbox("Select Symbol (Master Data)", syms, key="sel_master")
        
        if sel:
            d_sub = df_m[df_m["SYMBOL"]==sel].sort_values("D").set_index("D")["CLOSE_PRICE"]
            st.line_chart(d_sub)
    except:
        st.warning("Master CSV not available.")

# ============================================================
# TAB 2: REALTIME MONITORING (4-WEEKS)
# ============================================================

with tab_monitor:
    st.header("⏱ Realtime 4-Week Monitor")
    st.markdown("Fetch the last 4 weeks of data on-demand (independent of Master CSV) and visualize.")

    col_date, col_btn = st.columns([1, 2])
    
    with col_date:
        # User selects "Today" or any reference date
        monitor_date = st.date_input("Select Reference Date (End Date)", datetime.now(), key="mon_date")
    
    with col_btn:
        st.write("") # Spacer
        st.write("")
        fetch_btn = st.button("📥 Fetch Last 4 Weeks & Load", key="btn_fetch_mon")

    # Initialize Session State for Data persistence
    if "monitor_df" not in st.session_state:
        st.session_state.monitor_df = None

    if fetch_btn:
        ts_monitor = pd.Timestamp(monitor_date)
        with st.spinner(f"Downloading data for 4 weeks ending {monitor_date}..."):
            try:
                # Calls data_pipeline logic to fetch 28 days back from selected date
                df_mon = fetch_monitoring_data(ts_monitor)
                
                if not df_mon.empty:
                    st.session_state.monitor_df = df_mon
                    st.success(f"✔ Loaded {len(df_mon)} rows from {df_mon['DATE'].min().date()} to {df_mon['DATE'].max().date()}")
                else:
                    st.warning("No data found for this range.")
            except Exception as e:
                st.error(f"Fetch failed: {e}")

    st.divider()

    # Visualizer for Monitor Data
    if st.session_state.monitor_df is not None:
        df_real = st.session_state.monitor_df
        
        all_syms_real = sorted(df_real["SYMBOL"].unique().tolist())
        
        col_m1, col_m2 = st.columns([1,3])
        with col_m1:
            sel_sym_real = st.selectbox(
                "🔍 Select Stock Symbol (Realtime Data)", 
                all_syms_real, 
                key="sel_sym_real"
            )
        
        if sel_sym_real:
            # Filter
            subset = df_real[df_real["SYMBOL"] == sel_sym_real].sort_values("DATE")
            
            # Plot
            st.subheader(f"Price Trend: {sel_sym_real}")
            
            chart_data = subset.set_index("DATE")["CLOSE_PRICE"]
            st.line_chart(chart_data)
            
            with st.expander("View Raw Data"):
                st.dataframe(subset, use_container_width=True)
    else:
        st.info("👈 Select a date and click Fetch to start monitoring.")