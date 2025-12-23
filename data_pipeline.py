# ============================================================
# data_pipeline.py
# Bhavcopy download, merge, append & dedupe (FNO ONLY)
# ============================================================

import os
import glob
import zipfile
import requests
import pandas as pd
from datetime import datetime, timedelta

from config import (
    DATA_DIR,
    MASTER_CSV,
    CONSOLIDATED_BHAVCOPY,
    LIVE_BHAVCOPY_DIR
)

# ------------------------------------------------------------
# NSE HEADERS
# ------------------------------------------------------------

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/all-reports",
}

# ============================================================
# HELPER: GET CURRENT MASTER RANGE
# ============================================================

def get_master_date_range():
    """Returns formatted start and end date of the current Master CSV."""
    if not os.path.exists(MASTER_CSV):
        return "N/A", "N/A"
    
    try:
        df = pd.read_csv(MASTER_CSV, usecols=["DATE1"])
        df["DATE1"] = pd.to_datetime(df["DATE1"])
        
        min_date = df["DATE1"].min().strftime("%d-%b-%Y")
        max_date = df["DATE1"].max().strftime("%d-%b-%Y")
        return min_date, max_date
    except:
        return "Error", "Error"

# ============================================================
# DOWNLOAD DAILY BHAVCOPY
# ============================================================

def download_bhavcopy_for_date(date_str: str):

    API_URL = (
        "https://www.nseindia.com/api/reports?"
        "archives=[{\"name\":\"Full%20Bhavcopy%20and%20Security%20Deliverable%20data\","
        "\"type\":\"daily-reports\",\"category\":\"capital-market\","
        "\"section\":\"equities\"}]&"
        f"date={date_str}&type=equities&mode=single"
    )

    os.makedirs(LIVE_BHAVCOPY_DIR, exist_ok=True)

    with requests.Session() as s:
        s.headers.update(HEADERS)

        try:
            s.get("https://www.nseindia.com/", timeout=10)
        except:
            pass

        try:
            resp = s.get(API_URL, stream=True, timeout=30)
            resp.raise_for_status()

            fname = resp.headers.get(
                "Content-Disposition", "bhav.zip"
            ).split("filename=")[-1].replace('"', "")

            save_path = os.path.join(
                LIVE_BHAVCOPY_DIR, f"{date_str}_{fname}"
            )

            with open(save_path, "wb") as f:
                for chunk in resp.iter_content(1024):
                    f.write(chunk)

            print(f"✔ Downloaded bhavcopy: {date_str}")

        except:
            print(f"✖ Failed bhavcopy: {date_str}")

# ============================================================
# MERGE ALL BHAVCOPIES
# ============================================================

def merge_all_bhavcopies():

    all_data = []

    for zip_file in glob.glob(os.path.join(LIVE_BHAVCOPY_DIR, "*.zip")):
        try:
            with zipfile.ZipFile(zip_file, "r") as z:
                csvs = [f for f in z.namelist() if f.endswith(".csv")]
                if csvs:
                    df = pd.read_csv(z.open(csvs[0]), low_memory=False)
                    all_data.append(df)
        except:
            pass

    for csv_file in glob.glob(os.path.join(LIVE_BHAVCOPY_DIR, "*.csv")):
        try:
            df = pd.read_csv(csv_file, low_memory=False)
            all_data.append(df)
        except:
            pass

    if not all_data:
        print("ℹ No bhavcopies found")
        return

    final_df = pd.concat(all_data, ignore_index=True)
    final_df.to_csv(CONSOLIDATED_BHAVCOPY, index=False)

    print(f"✔ Consolidated bhavcopy saved")

# ============================================================
# APPEND TO FNO MASTER (SAFE)
# ============================================================

def append_consolidated_bhavcopy_fno_only():

    if not os.path.exists(MASTER_CSV):
        print(f"⚠ Master CSV not found at {MASTER_CSV}. Creating new from consolidated.")
        if os.path.exists(CONSOLIDATED_BHAVCOPY):
            bhav = pd.read_csv(CONSOLIDATED_BHAVCOPY, low_memory=False)
            bhav.columns = bhav.columns.str.strip().str.upper()
            
            # Handle date normalization
            if "DATE1" in bhav.columns:
                bhav["DATE1"] = pd.to_datetime(bhav["DATE1"])
            elif "DATE" in bhav.columns:
                bhav["DATE1"] = pd.to_datetime(bhav["DATE"])
                
            bhav.to_csv(MASTER_CSV, index=False)
            return

    master = pd.read_csv(MASTER_CSV, low_memory=False)
    bhav = pd.read_csv(CONSOLIDATED_BHAVCOPY, low_memory=False)

    # 🔧 NORMALIZE BHAVCOPY COLUMNS
    bhav.columns = bhav.columns.str.strip().str.upper()

    # Handle date column safely
    if "DATE1" in bhav.columns:
        bhav["DATE1"] = pd.to_datetime(bhav["DATE1"])
    elif "DATE" in bhav.columns:
        bhav["DATE1"] = pd.to_datetime(bhav["DATE"])
    else:
        raise RuntimeError(
            f"No recognizable date column in bhavcopy: {bhav.columns.tolist()}"
        )

    master["DATE1"] = pd.to_datetime(master["DATE1"])

    # FNO universe is source of truth
    fno_symbols = set(master["SYMBOL"].unique())

    bhav = bhav[master.columns.tolist()]

    # Filter to FNO symbols only
    bhav = bhav[bhav["SYMBOL"].isin(fno_symbols)]

    if bhav.empty:
        print("ℹ No FNO symbols to append")
        return

    key_cols = ["SYMBOL", "DATE1"]

    bhav = bhav[
        ~bhav.set_index(key_cols).index.isin(
            master.set_index(key_cols).index
        )
    ]

    if bhav.empty:
        print("ℹ No new rows to append")
        return

    final = pd.concat([master, bhav], ignore_index=True)
    final = final.sort_values(key_cols).reset_index(drop=True)

    final.to_csv(MASTER_CSV, index=False)

    print(
        f"✔ Appended {len(bhav)} rows "
        f"({bhav['DATE1'].min().date()} → {bhav['DATE1'].max().date()})"
    )

# ============================================================
# DAILY DATA UPDATE PIPELINE
# ============================================================

def run_daily_data_pipeline(start_date: str, end_date: str):

    s = datetime.strptime(start_date, "%d-%b-%Y")
    e = datetime.strptime(end_date, "%d-%b-%Y")

    while s <= e:
        download_bhavcopy_for_date(s.strftime("%d-%b-%Y"))
        s += timedelta(days=1)

    merge_all_bhavcopies()
    append_consolidated_bhavcopy_fno_only()