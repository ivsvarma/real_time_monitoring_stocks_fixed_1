# ============================================================
# corporate_cleaner.py
# Corporate action & bad tick cleaning (NO LOGIC CHANGES)
# ============================================================

import numpy as np
import pandas as pd

# ============================================================
# CLEAN CORPORATE EVENTS
# ============================================================

def clean_corporate_events(master_csv: str) -> pd.DataFrame:

    df = pd.read_csv(master_csv, low_memory=False)
    df.columns = df.columns.str.strip()
    df["DATE"] = pd.to_datetime(df["DATE1"])

    NON_NUMERIC = ["SYMBOL", "DATE"]

    for col in df.columns:
        if col not in NON_NUMERIC:
            df[col] = (
                df[col].astype(str)
                .str.replace(",", "", regex=False)
                .str.replace("%", "", regex=False)
                .str.strip()
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values(["SYMBOL", "DATE"]).reset_index(drop=True)

    # --------------------------------------------------------
    # RETURNS
    # --------------------------------------------------------

    df["ret_1d"] = df.groupby("SYMBOL")["CLOSE_PRICE"].pct_change()
    df["ret_2d"] = df.groupby("SYMBOL")["CLOSE_PRICE"].pct_change(2)

    # --------------------------------------------------------
    # ABNORMAL JUMPS
    # --------------------------------------------------------

    df["abnormal_jump"] = (
        (df["ret_1d"].abs() > 0.40) |
        (df["ret_2d"].abs() > 0.60)
    )

    df["post_stable"] = False

    for sym, sub in df.groupby("SYMBOL"):
        sub = sub.reset_index()
        for i in sub.index[sub["abnormal_jump"]]:
            if i + 5 >= len(sub):
                continue
            base = sub.loc[i + 1, "CLOSE_PRICE"]
            fut = sub.loc[i + 1:i + 5, "CLOSE_PRICE"]
            if np.all(np.abs(fut / base - 1) < 0.10):
                df.at[sub.loc[i, "index"], "post_stable"] = True

    # --------------------------------------------------------
    # CLASSIFY EVENTS
    # --------------------------------------------------------

    df["suspected_corp_event"] = df["abnormal_jump"] & df["post_stable"]

    df["suspected_bad_tick"] = (
        (df["ret_1d"].abs() > 0.8) &
        (df.groupby("SYMBOL")["ret_1d"].shift(-1).abs() < 0.1)
    )

    GAP_DAYS = 10
    EXCLUDE_SPLIT = ["BRITANNIA"]

    final = []

    # --------------------------------------------------------
    # SPLIT PRE / POST
    # --------------------------------------------------------

    for sym, sub in df.groupby("SYMBOL"):
        sub = sub[~sub["suspected_bad_tick"]].reset_index(drop=True)

        if sym in EXCLUDE_SPLIT:
            final.append(sub.copy())
            continue

        events = sub.index[sub["suspected_corp_event"]]

        if len(events) == 0:
            final.append(sub.copy())
            continue

        idx = events[0]

        pre = sub.iloc[:max(0, idx - GAP_DAYS)].copy()
        post = sub.iloc[min(len(sub), idx + GAP_DAYS + 1):].copy()

        if len(pre):
            pre.loc[:, "SYMBOL"] = f"{sym}_PRE"
            final.append(pre)

        if len(post):
            post.loc[:, "SYMBOL"] = f"{sym}_POST"
            final.append(post)

    df = pd.concat(final, ignore_index=True)

    return df[
        [
            "SYMBOL", "DATE",
            "OPEN_PRICE", "HIGH_PRICE", "LOW_PRICE",
            "LAST_PRICE", "CLOSE_PRICE", "AVG_PRICE",
            "TTL_TRD_QNTY", "TURNOVER_LACS",
            "NO_OF_TRADES", "DELIV_QTY", "DELIV_PER"
        ]
    ]
