# ============================================================
# trade_engine.py
# Live trade generation (Champion–Challenger)
# ============================================================

import os
import joblib
import pandas as pd

from config import (
    FEATURES,
    TOP_K,
    MODEL_DIR
)

# ============================================================
# LIVE TRADE DECISION
# ============================================================

def live_trade_decision(df_feat: pd.DataFrame, decision_date, entry_date) -> pd.DataFrame:

    # Ensure timestamps
    decision_date = pd.to_datetime(decision_date)
    entry_date = pd.to_datetime(entry_date)

    # --------------------------------------------------------
    # Snapshot at SPECIFIC decision date (Dynamic)
    # --------------------------------------------------------

    snap = (
        df_feat[df_feat["DATE"] == decision_date]
        .dropna(subset=FEATURES)
        .copy()
    )

    if snap.empty:
        raise RuntimeError(f"❌ No data available on Selected Decision Date: {decision_date.date()}")

    # --------------------------------------------------------
    # Load all models
    # --------------------------------------------------------

    models = {}

    if not os.path.exists(MODEL_DIR):
        raise RuntimeError(f"❌ Model directory not found: {MODEL_DIR}")

    for f in os.listdir(MODEL_DIR):
        if not f.endswith(".joblib"):
            continue

        try:
            macro_id = int(float(
                f.replace("macro_", "").replace(".joblib", "")
            ))

            models[macro_id] = joblib.load(
                os.path.join(MODEL_DIR, f)
            )
        except:
            continue

    if not models:
        raise RuntimeError("❌ No trained models found. Please train models first.")

    # --------------------------------------------------------
    # Champion–Challenger
    # --------------------------------------------------------

    cluster_scores = {}
    cluster_topk = {}

    for macro, model in models.items():

        tmp = snap.copy()
        try:
            tmp["pred"] = model.predict(tmp[FEATURES])
            
            topk = (
                tmp
                .sort_values(["pred", "SYMBOL"], ascending=[False, True])
                .head(TOP_K)
            )

            cluster_scores[macro] = topk["pred"].mean()
            cluster_topk[macro] = topk
        except Exception as e:
            print(f"Skipping macro {macro}: {e}")

    if not cluster_scores:
        raise RuntimeError("❌ Failed to generate predictions for any macro group.")

    champion = max(cluster_scores, key=cluster_scores.get)

    # --------------------------------------------------------
    # Final trade sheet
    # --------------------------------------------------------

    trade = cluster_topk[champion].copy()

    trade.loc[:, "weight"] = 1 / TOP_K
    trade.loc[:, "entry_date"] = entry_date
    trade.loc[:, "cluster"] = champion

    return trade.sort_values("pred", ascending=False)