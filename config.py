# ============================================================
# config.py
# Central configuration for Quant Monitoring System
# ============================================================

import os
import pandas as pd
from datetime import datetime

# ------------------------------------------------------------
# PROJECT ROOT
# ------------------------------------------------------------

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# ------------------------------------------------------------
# UI DEFAULTS (Placeholders Only)
# ------------------------------------------------------------
# These are ONLY used to set the default value of the Date Picker in the UI.
# The actual logic will use whatever the user selects in the App.
DEFAULT_DECISION_DATE = pd.Timestamp(datetime.now().date())

# ------------------------------------------------------------
# STRATEGY CONSTANTS
# ------------------------------------------------------------

HOLDING_DAYS = 5
TOP_K = 5
MIN_TRAIN_ROWS = 3000
INITIAL_CAPITAL = 100_000

FEATURES = [
    "ret_1", "ret_3", "ret_5", "ret_10",
    "vol_5", "vol_10",
    "range", "vol_z", "deliv_z"
]

# ------------------------------------------------------------
# DATA PATHS
# ------------------------------------------------------------

DATA_DIR = os.path.join(PROJECT_ROOT, "realtime", "data")

MASTER_CSV = os.path.join(
    DATA_DIR, "FNO_Combined_Updated_recent_1.csv"
)

CONSOLIDATED_BHAVCOPY = os.path.join(
    DATA_DIR, "consolidated_bhavcopy.csv"
)

LIVE_BHAVCOPY_DIR = os.path.join(
    DATA_DIR, "live_bhavcopy"
)

# Temp folder specifically for the Realtime Monitoring Tab
MONITOR_DIR = os.path.join(
    DATA_DIR, "monitor_temp"
)

# ------------------------------------------------------------
# REGIME FILES
# ------------------------------------------------------------

REGIME_TABLE = os.path.join(
    PROJECT_ROOT, "regimes_from_breakpoints.csv"
)

MACRO_MAP = os.path.join(
    PROJECT_ROOT, "regime_to_macro_mapping.csv"
)

# ------------------------------------------------------------
# MODEL PATHS
# ------------------------------------------------------------

MODEL_DIR = os.path.join(PROJECT_ROOT, "models")
os.makedirs(MODEL_DIR, exist_ok=True)

# ------------------------------------------------------------
# OUTPUT / RESULTS
# ------------------------------------------------------------

RESULTS_DIR = os.path.join(PROJECT_ROOT, "results")
os.makedirs(RESULTS_DIR, exist_ok=True)

LIVE_TRADES_FILE = lambda d: os.path.join(
    RESULTS_DIR, f"LIVE_TRADES_{d}.csv"
)

WEEKLY_ALPHA_FILE = lambda d: os.path.join(
    RESULTS_DIR, f"weekly_alpha_check_{d}.csv"
)

MODEL_RETURNS_FILE = lambda d: os.path.join(
    RESULTS_DIR, f"model_stock_returns_{d}.csv"
)

# ------------------------------------------------------------
# LOGS
# ------------------------------------------------------------

LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "daily_run.log")