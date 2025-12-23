# ============================================================
# performance_engine.py
# Model vs Universe performance evaluation (NO LOGIC CHANGES)
# ============================================================

import pandas as pd
import numpy as np

from config import (
    DECISION_DATE,
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
    if sym.endswith("_POST"):
        return sym.replace("_POST", "")
    if sym.endswith("_PRE"):
        return sym.replace("_PRE", "")
    return sym

# ============================================================
# RUN PERFORMANCE CHECK
# ============================================================

def run_weekly_performance_check(exit_date):

    # --------------------------------------------------------
    # Load data
    # --------------------------------------------------------

    df = pd.read_csv(MASTER_CSV, low_memory=False)
    df["DATE1"] = pd.to_datetime(df["DATE1"])

    trades = pd.read_csv(
        LIVE_TRADES_FILE(exit_date - pd.Timedelta(days=HOLDING_DAYS))
    )

    symbols_model = trades["SYMBOL"].unique()

    # --------------------------------------------------------
    # Trading calendar
    # --------------------------------------------------------

    dates = np.sort(df["DATE1"].unique())

    decision_idx = np.where(
        dates == np.datetime64(DECISION_DATE)
    )[0]

    if len(decision_idx) == 0:
        raise RuntimeError("Decision date not found")

    entry_date = pd.Timestamp(dates[decision_idx[0] + 1])
    exit_date = pd.Timestamp(dates[decision_idx[0] + HOLDING_DAYS])

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
            "Universe Mean Return",
            "Universe Median Return",
            "Model Mean Return",
            "Model Median Return",
            "Alpha (Mean)",
            "Alpha (Median)",
            "Universe Win Rate",
            "Model Win Rate",
            "Model Rank Percentile"
        ],
        "Value": [
            universe_returns["return_5d"].mean(),
            universe_returns["return_5d"].median(),
            model_returns["return_5d"].mean(),
            model_returns["return_5d"].median(),
            model_returns["return_5d"].mean()
            - universe_returns["return_5d"].mean(),
            model_returns["return_5d"].median()
            - universe_returns["return_5d"].median(),
            (universe_returns["return_5d"] > 0).mean(),
            (model_returns["return_5d"] > 0).mean(),
            (universe_returns["return_5d"]
             < model_returns["return_5d"].mean()).mean()
        ]
    })

    summary["Value"] = summary["Value"].round(4)

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
