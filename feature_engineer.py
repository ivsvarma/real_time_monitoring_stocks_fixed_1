# ============================================================
# feature_engineer.py
# Feature engineering (NO LOGIC CHANGES)
# ============================================================

import pandas as pd

# ============================================================
# ADD FEATURES
# ============================================================

def add_features(df: pd.DataFrame) -> pd.DataFrame:

    def build_features(g):

        g = g.sort_values("DATE")

        # ----------------------------
        # Returns
        # ----------------------------
        g["ret_1"]  = g["CLOSE_PRICE"].pct_change(1)
        g["ret_3"]  = g["CLOSE_PRICE"].pct_change(3)
        g["ret_5"]  = g["CLOSE_PRICE"].pct_change(5)
        g["ret_10"] = g["CLOSE_PRICE"].pct_change(10)

        # ----------------------------
        # Volatility
        # ----------------------------
        g["vol_5"]  = g["CLOSE_PRICE"].pct_change().rolling(5).std()
        g["vol_10"] = g["CLOSE_PRICE"].pct_change().rolling(10).std()

        # ----------------------------
        # Price range
        # ----------------------------
        g["range"] = (
            (g["HIGH_PRICE"] - g["LOW_PRICE"]) / g["CLOSE_PRICE"]
        )

        # ----------------------------
        # Z-scores
        # ----------------------------
        g["vol_z"] = (
            (g["TTL_TRD_QNTY"] - g["TTL_TRD_QNTY"].rolling(20).mean()) /
            g["TTL_TRD_QNTY"].rolling(20).std()
        )

        g["deliv_z"] = (
            (g["DELIV_PER"] - g["DELIV_PER"].rolling(20).mean()) /
            g["DELIV_PER"].rolling(20).std()
        )

        return g

    return (
        df
        .groupby("SYMBOL", group_keys=False)
        .apply(build_features)
        .reset_index(drop=True)
    )
