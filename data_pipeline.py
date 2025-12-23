# ============================================================
# data_pipeline.py
# Bhavcopy download, merge, append & dedupe
# ============================================================

import os
import glob
import zipfile
import requests
import pandas as pd
import shutil
from datetime import datetime, timedelta

from config import (
    DATA_DIR,
    MASTER_CSV,
    CONSOLIDATED_BHAVCOPY,
    LIVE_BHAVCOPY_DIR,
    MONITOR_DIR
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
# CORE DOWNLOADER
# ============================================================

def download_bhavcopy_to_dir(date_str: str, output_dir: str):
    """Downloads a single bhavcopy to a specific directory."""
    API_URL = (
        "https://www.nseindia.com/api/reports?"
        "archives=[{\"name\":\"Full%20Bhavcopy%20and%20Security%20Deliverable%20data\","
        "\"type\":\"daily-reports\",\"category\":\"capital-market\","
        "\"section\":\"equities\"}]&"
        f"date={date_str}&type=equities&mode=single"
    )

    os.makedirs(output_dir, exist_ok=True)

    with requests.Session() as s:
        s.headers.update(HEADERS)
        try:
            s.get("https://www.nseindia.com/", timeout=10)
        except:
            pass

        try:
            resp = s.get(API_URL, stream=True, timeout=30)
            resp.raise_for_status()

            fname = resp.headers.get("Content-Disposition", "bhav.zip").split("filename=")[-1].replace('"', "")
            save_path = os.path.join(output_dir, f"{date_str}_{fname}")

            with open(save_path, "wb") as f:
                for chunk in resp.iter_content(1024):
                    f.write(chunk)
            return True
        except:
            return False

# ============================================================
# STRATEGY PIPELINE FUNCTIONS
# ============================================================

def run_daily_data_pipeline(start_date: str, end_date: str):
    """Downloads data for Master CSV update"""
    s = datetime.strptime(start_date, "%d-%b-%Y")
    e = datetime.strptime(end_date, "%d-%b-%Y")

    while s <= e:
        download_bhavcopy_to_dir(s.strftime("%d-%b-%Y"), LIVE_BHAVCOPY_DIR)
        s += timedelta(days=1)

    # Merge logic (simplified for brevity, logic remains same as before)
    all_data = []
    for zip_file in glob.glob(os.path.join(LIVE_BHAVCOPY_DIR, "*.zip")):
        try:
            with zipfile.ZipFile(zip_file, "r") as z:
                csvs = [f for f in z.namelist() if f.endswith(".csv")]
                if csvs:
                    df = pd.read_csv(z.open(csvs[0]), low_memory=False)
                    all_data.append(df)
        except: pass
    
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(CONSOLIDATED_BHAVCOPY, index=False)
        append_consolidated_bhavcopy_fno_only()

def append_consolidated_bhavcopy_fno_only():
    # ... (Same logic as provided in previous turns) ...
    # Re-implementing briefly for completeness in this file context
    if not os.path.exists(MASTER_CSV) and os.path.exists(CONSOLIDATED_BHAVCOPY):
        shutil.copy(CONSOLIDATED_BHAVCOPY, MASTER_CSV)
        return

    master = pd.read_csv(MASTER_CSV, low_memory=False)
    bhav = pd.read_csv(CONSOLIDATED_BHAVCOPY, low_memory=False)
    bhav.columns = bhav.columns.str.strip().str.upper()
    
    if "DATE1" in bhav.columns: bhav["DATE1"] = pd.to_datetime(bhav["DATE1"])
    elif "DATE" in bhav.columns: bhav["DATE1"] = pd.to_datetime(bhav["DATE"])
    
    master["DATE1"] = pd.to_datetime(master["DATE1"])
    fno = set(master["SYMBOL"].unique())
    bhav = bhav[bhav["SYMBOL"].isin(fno)]
    
    key = ["SYMBOL", "DATE1"]
    bhav = bhav[~bhav.set_index(key).index.isin(master.set_index(key).index)]
    
    if not bhav.empty:
        pd.concat([master, bhav], ignore_index=True).sort_values(key).to_csv(MASTER_CSV, index=False)

# ============================================================
# REALTIME MONITORING FETCH
# ============================================================

def fetch_monitoring_data(end_date: pd.Timestamp):
    """
    Downloads last 4 weeks of data ending on 'end_date' to a temp folder.
    Returns a DataFrame of that data.
    """
    start_date = end_date - timedelta(weeks=4)
    current = start_date
    
    # 1. Prepare Temp Directory
    if os.path.exists(MONITOR_DIR):
        shutil.rmtree(MONITOR_DIR) # Clear old cache
    os.makedirs(MONITOR_DIR, exist_ok=True)

    print(f"Fetching monitor data: {start_date.date()} to {end_date.date()}")
    
    # 2. Download Loop
    while current <= end_date:
        d_str = current.strftime("%d-%b-%Y")
        download_bhavcopy_to_dir(d_str, MONITOR_DIR)
        current += timedelta(days=1)

    # 3. Load & Merge
    all_dfs = []
    
    for zf in glob.glob(os.path.join(MONITOR_DIR, "*.zip")):
        try:
            with zipfile.ZipFile(zf, "r") as z:
                csvs = [f for f in z.namelist() if f.endswith(".csv")]
                if csvs:
                    df = pd.read_csv(z.open(csvs[0]), low_memory=False)
                    all_dfs.append(df)
        except: pass
        
    if not all_dfs:
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    
    # Normalize Columns
    combined.columns = combined.columns.str.strip().str.upper()
    
    # Standardize Date Column
    if "DATE1" in combined.columns:
        combined["DATE"] = pd.to_datetime(combined["DATE1"])
    elif "DATE" in combined.columns:
        combined["DATE"] = pd.to_datetime(combined["DATE"])
        
    return combined.sort_values("DATE")