import pandas as pd
import numpy as np

def integrate_regimes(df, regime_csv, macro_csv, cutoff_date):
    """
    Integrates regime data. 
    cutoff_date: The specific decision date (pd.Timestamp) from the UI.
    """
    reg = pd.read_csv(regime_csv)
    mac = pd.read_csv(macro_csv)
    reg["start_date"] = pd.to_datetime(reg["start_date"])
    reg["end_date"] = pd.to_datetime(reg["end_date"])

    # Ensure cutoff is timestamp
    cutoff_date = pd.to_datetime(cutoff_date)

    def map_reg(d):
        # Use the passed argument, not the config constant
        if d > cutoff_date:
            return np.nan
        r = reg[(reg.start_date <= d) & (reg.end_date >= d)]
        return r.regime_id.iloc[0] if len(r) else np.nan

    df["regime"] = df["DATE"].apply(map_reg)
    df["macro_group"] = df["regime"].map(mac.set_index("regime_id")["macro_group"])
    return df