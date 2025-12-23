# ============================================================
# model_engine.py
# Train & save macro-wise models (NO LOGIC CHANGES)
# ============================================================

import os
import joblib
import xgboost as xgb
import pandas as pd

from config import (
    FEATURES,
    HOLDING_DAYS,
    MIN_TRAIN_ROWS,
    MODEL_DIR
)

# ============================================================
# TRAIN & SAVE MODELS
# ============================================================

def train_and_save_models(train_df: pd.DataFrame):

    df = train_df.copy()

    # --------------------------------------------------------
    # Target: H-day forward return
    # --------------------------------------------------------

    df["target"] = (
        df.groupby("SYMBOL")["CLOSE_PRICE"].shift(-HOLDING_DAYS)
        / df["CLOSE_PRICE"] - 1
    )

    # --------------------------------------------------------
    # Drop unsafe rows
    # --------------------------------------------------------

    df = df.dropna(
        subset=FEATURES + ["target", "macro_group"]
    )

    # --------------------------------------------------------
    # Train per macro group
    # --------------------------------------------------------

    for macro in sorted(df["macro_group"].unique()):

        d = df[df["macro_group"] == macro]

        if len(d) < MIN_TRAIN_ROWS:
            continue

        model = xgb.XGBRegressor(
            n_estimators=300,
            max_depth=6,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            objective="reg:squarederror",
            random_state=42,
            n_jobs=1,
            tree_method="exact"
        )

        model.fit(d[FEATURES], d["target"])

        model_path = os.path.join(
            MODEL_DIR, f"macro_{float(macro)}.joblib"
        )

        joblib.dump(model, model_path)

    print("✅ Models trained & saved successfully")
