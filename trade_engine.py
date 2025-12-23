# ============================================================
# trade_engine.py
# Live trade generation (Champion–Challenger)
# ============================================================

import os
import joblib
import pandas as pd

from config import (
    DECISION_DATE,
    ENTRY_DATE,
    FEATURES,
    TOP_K,
    MODEL_DIR
)

# ============================================================
# LIVE TRADE DECISION
# ============================================================

def live_trade_decision(df_feat: pd.DataFrame) -> pd.DataFrame:

    # --------------------------------------------------------
    # Snapshot at decision date
    # --------------------------------------------------------

    snap = (
        df_feat[df_feat["DATE"] == DECISION_DATE]
        .dropna(subset=FEATURES)
        .copy()
    )

    if snap.empty:
        raise RuntimeError("❌ No data available on DECISION_DATE")

    # --------------------------------------------------------
    # Load all models
    # --------------------------------------------------------

    models = {}

    for f in os.listdir(MODEL_DIR):
        if not f.endswith(".joblib"):
            continue

        macro_id = int(float(
            f.replace("macro_", "").replace(".joblib", "")
        ))

        models[macro_id] = joblib.load(
            os.path.join(MODEL_DIR, f)
        )

    if not models:
        raise RuntimeError("❌ No trained models found")

    # --------------------------------------------------------
    # Champion–Challenger
    # --------------------------------------------------------

    cluster_scores = {}
    cluster_topk = {}

    for macro, model in models.items():

        tmp = snap.copy()
        tmp["pred"] = model.predict(tmp[FEATURES])

        topk = (
            tmp
            .sort_values(["pred", "SYMBOL"], ascending=[False, True])
            .head(TOP_K)
        )

        cluster_scores[macro] = topk["pred"].mean()
        cluster_topk[macro] = topk

    champion = max(cluster_scores, key=cluster_scores.get)

    # --------------------------------------------------------
    # Final trade sheet
    # --------------------------------------------------------

    trade = cluster_topk[champion].copy()

    trade.loc[:, "weight"] = 1 / TOP_K
    trade.loc[:, "entry_date"] = ENTRY_DATE
    trade.loc[:, "cluster"] = champion

    return trade.sort_values("pred", ascending=False)
