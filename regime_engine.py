import pandas as pd
import numpy as np
from config import DECISION_DATE

def integrate_regimes(df, regime_csv, macro_csv):
    reg = pd.read_csv(regime_csv)
    mac = pd.read_csv(macro_csv)
    reg["start_date"] = pd.to_datetime(reg["start_date"])
    reg["end_date"] = pd.to_datetime(reg["end_date"])

    def map_reg(d):
        if d > DECISION_DATE:
            return np.nan
        r = reg[(reg.start_date<=d)&(reg.end_date>=d)]
        return r.regime_id.iloc[0] if len(r) else np.nan

    df["regime"] = df["DATE"].apply(map_reg)
    df["macro_group"] = df["regime"].map(mac.set_index("regime_id")["macro_group"])
    return df
