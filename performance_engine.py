# ============================================================
# performance_engine.py
# Model vs Universe performance evaluation (NO LOGIC CHANGES)
# ============================================================

import pandas as pd
import numpy as np

from config import (
    HOLDING_DAYS,
    MASTER_CSV,
    LIVE_TRADES_FILE,
    WEEKLY_ALPHA_FILE,
    MODEL_RETURNS_FILE
)

# ============================================================
# Helper: canonical symbol
# ============================================================

def extract_base_symbol(sym: str) -> str:
    if isinstance(sym, str):
        if sym.endswith("_POST"):
            return sym.replace("_POST", "")
        if sym.endswith("_PRE"):
            return sym.replace("_PRE", "")
    return sym

# ============================================================
# RUN PERFORMANCE CHECK
# ============================================================

def run_weekly_performance_check(decision_date):
    """
    Calculates performance relative to a specific Decision Date.
    It automatically finds the entry (T+1) and exit (T+5) from the data.
    """
    
    decision_date = pd.to_datetime(decision_date)

    # --------------------------------------------------------
    # Load data
    # --------------------------------------------------------

    df = pd.read_csv(MASTER_CSV, low_memory=False)
    if "DATE1" in df.columns:
        df["DATE1"] = pd.to_datetime(df["DATE1"])
    else:
        df["DATE1"] = pd.to_datetime(df["DATE"])

    # Load trades generated for this specific Entry Date
    # Note: Trade file naming convention usually uses Entry Date
    # We need to calculate Entry Date first to find the file
    
    dates = np.sort(df["DATE1"].unique())
    decision_idx = np.where(dates == np.datetime64(decision_date))[0]

    if len(decision_idx) == 0:
        raise RuntimeError(f"Decision date {decision_date.date()} not found in Master Data")

    # Calculate Entry and Exit based on Trading Calendar
    try:
        entry_ts = dates[decision_idx[0] + 1]
        exit_ts = dates[decision_idx[0] + HOLDING_DAYS]
    except IndexError:
        raise RuntimeError("Not enough future data points for Entry/Exit calculation")

    entry_date = pd.Timestamp(entry_ts)
    exit_date = pd.Timestamp(exit_ts)

    # Load Trades
    trade_file = LIVE_TRADES_FILE(entry_date.date())
    try:
        trades = pd.read_csv(trade_file)
    except FileNotFoundError:
        raise RuntimeError(f"Trade file not found: {trade_file}")

    symbols_model = trades["SYMBOL"].unique()

    # --------------------------------------------------------
    # Entry prices (AVG_PRICE)
    # --------------------------------------------------------

    entry_prices = (
        df[df["DATE1"] == entry_date]
        [["SYMBOL", "AVG_PRICE"]]
        .copy()
    )

    entry_prices["BASE_SYMBOL"] = entry_prices["SYMBOL"].apply(
        extract_base_symbol
    )

    entry_prices = (
        entry_prices
        .groupby("BASE_SYMBOL", as_index=False)
        .agg(entry_price=("AVG_PRICE", "mean"))
    )

    # --------------------------------------------------------
    # Exit prices (AVG_PRICE)
    # --------------------------------------------------------

    exit_prices = (
        df[df["DATE1"] == exit_date]
        [["SYMBOL", "AVG_PRICE"]]
        .copy()
    )

    exit_prices["BASE_SYMBOL"] = exit_prices["SYMBOL"].apply(
        extract_base_symbol
    )

    exit_prices = (
        exit_prices
        .groupby("BASE_SYMBOL", as_index=False)
        .agg(exit_price=("AVG_PRICE", "mean"))
    )

    # --------------------------------------------------------
    # Compute returns
    # --------------------------------------------------------

    returns = (
        entry_prices
        .merge(exit_prices, on="BASE_SYMBOL", how="inner")
    )

    returns["return_5d"] = (
        returns["exit_price"] / returns["entry_price"] - 1
    )

    # --------------------------------------------------------
    # Model vs Universe
    # --------------------------------------------------------

    symbols_model_base = {
        extract_base_symbol(s) for s in symbols_model
    }

    model_returns = returns[
        returns["BASE_SYMBOL"].isin(symbols_model_base)
    ].copy()

    universe_returns = returns.copy()

    # --------------------------------------------------------
    # Summary metrics
    # --------------------------------------------------------

    summary = pd.DataFrame({
        "Metric": [
            "Entry Date",
            "Exit Date",
            "Universe Mean Return",
            "Universe Median Return",
            "Model Mean Return",
            "Model Median Return",
            "Alpha (Mean)",
            "Alpha (Median)",
            "Universe Win Rate",
            "Model Win Rate"
        ],
        "Value": [
            str(entry_date.date()),
            str(exit_date.date()),
            universe_returns["return_5d"].mean(),
            universe_returns["return_5d"].median(),
            model_returns["return_5d"].mean(),
            model_returns["return_5d"].median(),
            model_returns["return_5d"].mean()
            - universe_returns["return_5d"].mean(),
            model_returns["return_5d"].median()
            - universe_returns["return_5d"].median(),
            (universe_returns["return_5d"] > 0).mean(),
            (model_returns["return_5d"] > 0).mean()
        ]
    })

    # --------------------------------------------------------
    # Save outputs
    # --------------------------------------------------------

    summary.to_csv(
        WEEKLY_ALPHA_FILE(exit_date.date()),
        index=False
    )

    model_returns.to_csv(
        MODEL_RETURNS_FILE(exit_date.date()),
        index=False
    )

    print("✅ Weekly performance evaluation completed")

    return summary, model_returns