# paymeter_app.py
# -*- coding: utf-8 -*-
"""
Paymeter Pro – Fancy UI + Clear Instructions + Custom Logo
All reports → ONE timestamped Excel
Run: streamlit run paymeter_app.py
"""

import csv
import re
import os
import shutil
import tempfile
import base64
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime, date

import pandas as pd
import streamlit as st

# -----------------------------
# Config / defaults
# -----------------------------
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DEFAULT_DISTRICT = DATA_DIR / "district.csv"
DEFAULT_KCG = DATA_DIR / "KCG.csv"
DEFAULT_DISTRICT_INFO = DATA_DIR / "district_acct_number.csv"
LOGO_PATH = DATA_DIR / "logo.png"

# === YOUR LOGO – Base64 (placeholder for a simple 1x1 transparent pixel; replace with your full base64) ===
EMBEDDED_LOGO_BASE64 = """
iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==
"""  # ← **YOUR LOGO** – keep the whole string, no extra spaces. This is a placeholder; generate your own as per instructions.

# ----------------------------------------------------------------------
# Helper: robust CSV reader
# ----------------------------------------------------------------------
def safe_read_csv(path: Path) -> pd.DataFrame:
    with open(path, 'r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f)
        rows = list(reader)
    if not rows:
        return pd.DataFrame()
    header = rows[0]
    data = rows[1:]
    max_cols = max(len(row) for row in rows)
    for row in data:
        if len(row) < max_cols:
            row.extend([''] * (max_cols - len(row)))
    if len(header) < max_cols:
        header = header + [f'Unnamed_{i}' for i in range(len(header), max_cols)]
    df = pd.DataFrame(data, columns=header)
    return df.astype(str)

# ----------------------------------------------------------------------
# Helper: make DataFrame columns unique
# ----------------------------------------------------------------------
def make_columns_unique(df: pd.DataFrame) -> pd.DataFrame:
    cols = df.columns.tolist()
    seen = set()
    unique_cols = []
    for c in cols:
        if c in seen:
            i = 1
            new_c = f"{c}_{i}"
            while new_c in seen:
                i += 1
                new_c = f"{c}_{i}"
            unique_cols.append(new_c)
            seen.add(new_c)
        else:
            unique_cols.append(c)
            seen.add(c)
    df.columns = unique_cols
    return df

# -----------------------------
# Utility helpers
# -----------------------------
_amount_re = re.compile(r"""^\s*[-+]?(?:\d{1,3}(?:,\d{3})*|\d+)(?:\.\d+)?\s*$""")

def _norm(s: Optional[str]) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

def is_amount(val: Optional[str]) -> bool:
    if val is None:
        return False
    s = str(val).strip()
    if not s:
        return False
    return bool(_amount_re.match(s))

def find_col_index(header: List[str], candidates: List[str]) -> Optional[int]:
    H = [_norm(h) for h in header]
    C = [_norm(c) for c in candidates]
    for i, h in enumerate(H):
        if h in C:
            return i
    return None

def normalize_acct(x: Optional[str]) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return re.sub(r"\D", "", s)

def calculate_commission(x) -> float:
    try:
        v = float(x)
        return 100000.0 if v >= 4_000_000 else v * 0.025
    except Exception:
        return 0.0

def pick_kcg_column(df: pd.DataFrame) -> str:
    cols = list(df.columns)
    if not cols:
        raise ValueError("KCG file has no columns")
    def score(c):
        lc = c.lower()
        return (
            ("kcg" in lc) * 4 + ("account" in lc) * 2 + ("number" in lc) * 1,
            -len(c)
        )
    preferred = sorted(cols, key=score, reverse=True)
    for c in preferred:
        sample = df[c].astype(str).head(200).apply(normalize_acct)
        if (sample.str.len() >= 6).mean() >= 0.5:
            return c
    return cols[0]

# -----------------------------
# Step A: Repair address spill
# -----------------------------
def repair_address_spill(
    input_file: str,
    output_file: str,
    address_candidates: Optional[List[str]] = None,
    txn_amt_candidates: Optional[List[str]] = None,
    preview_limit: int = 8
) -> Tuple[int, List[Dict]]:
    address_candidates = address_candidates or [
        "address", "customer address", "service address", "customeraddress", "serviceaddress"
    ]
    txn_amt_candidates = txn_amt_candidates or [
        "transaction amount", "txn amount", "amount", "amt", "transactionamount", "txnamount"
    ]

    with open(input_file, "r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("Input CSV is empty.")
    expected_cols = len(header)

    addr_idx = find_col_index(header, address_candidates)
    txn_idx = find_col_index(header, txn_amt_candidates)
    if addr_idx is None:
        raise ValueError("Could not find Address column.")
    if txn_idx is None:
        raise ValueError("Could not find Transaction Amount column.")

    repaired_examples = []
    fixed_count = 0
    row_num = 1

    with open(input_file, "r", encoding="utf-8", newline="") as fin, \
         open(output_file, "w", encoding="utf-8", newline="") as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        header_row = next(reader)
        writer.writerow(header_row)
        row_num += 1

        for row in reader:
            original = row[:]
            this_row_num = row_num
            row_num += 1

            if len(row) <= txn_idx:
                row = row + [""] * (txn_idx + 1 - len(row))

            if txn_idx < len(row) and is_amount(row[txn_idx]):
                if len(row) > expected_cols:
                    row = row[:expected_cols]
                elif len(row) < expected_cols:
                    row = row + [""] * (expected_cols - len(row))
                writer.writerow(row)
                continue

            j = txn_idx
            spill = []
            while j < len(row) and not is_amount(row[j]):
                spill.append(row[j])
                j += 1

            if j >= len(row) and not spill:
                if len(row) > expected_cols:
                    row = row[:expected_cols]
                elif len(row) < expected_cols:
                    row = row + [""] * (expected_cols - len(row))
                writer.writerow(row)
                continue

            if not spill:
                if len(row) > expected_cols:
                    row = row[:expected_cols]
                elif len(row) < expected_cols:
                    row = row + [""] * (expected_cols - len(row))
                writer.writerow(row)
                continue

            address_val = (row[addr_idx] if addr_idx < len(row) else "").strip()
            spill_text = ", ".join([s.strip() for s in spill if str(s).strip()])
            new_address = (f"{address_val}, {spill_text}" if address_val else spill_text).strip().strip(",")

            new_row = row[:]
            if addr_idx < len(new_row):
                new_row[addr_idx] = new_address
            else:
                new_row += [""] * (addr_idx + 1 - len(new_row))
                new_row[addr_idx] = new_address

            del new_row[txn_idx:j]

            if len(new_row) > expected_cols:
                new_row = new_row[:expected_cols]
            elif len(new_row) < expected_cols:
                new_row += [""] * (expected_cols - len(new_row))

            writer.writerow(new_row)
            fixed_count += 1

            if len(repaired_examples) < preview_limit:
                show_from = max(0, addr_idx - 2)
                show_to = min(expected_cols, txn_idx + 6)
                repaired_examples.append({
                    "line": this_row_num,
                    "before": original[show_from:show_to] + (["…"] if len(original) > show_to else []),
                    "after": new_row[show_from:show_to]
                })

    return fixed_count, repaired_examples

# -----------------------------
# Step B: Merge district lookup
# -----------------------------
def merge_districts(paymeter_cleaned: str, district_path: str, out_path: str) -> pd.DataFrame:
    paymeter = safe_read_csv(Path(paymeter_cleaned))
    if district_path and os.path.exists(district_path):
        district = safe_read_csv(Path(district_path))
        if 'paymeter Account Number' in district.columns and 'DISTRICT BY ADDRESS' in district.columns:
            district = district[['paymeter Account Number', 'DISTRICT BY ADDRESS']].drop_duplicates(subset=['paymeter Account Number'])
            district.rename(columns={'paymeter Account Number': 'Account Number'}, inplace=True)
        else:
            acct_col = None
            dist_col = None
            for c in district.columns:
                lc = c.lower()
                if acct_col is None and ('account' in lc or 'acct' in lc or 'no' in lc):
                    acct_col = c
                if dist_col is None and ('district' in lc or 'dist' in lc):
                    dist_col = c
            if acct_col and dist_col:
                district = district[[acct_col, dist_col]].drop_duplicates(subset=[acct_col])
                district.columns = ['Account Number', 'DISTRICT BY ADDRESS']
            else:
                paymeter = make_columns_unique(paymeter)
                paymeter.to_csv(out_path, index=False)
                return paymeter

        merged = paymeter.merge(district, on='Account Number', how='left')
        merged['District Name'] = merged['DISTRICT BY ADDRESS'].combine_first(merged.get('District Name', pd.Series(dtype=str)))
        merged = make_columns_unique(merged)
        merged.to_csv(out_path, index=False)
        return merged
    else:
        paymeter = make_columns_unique(paymeter)
        paymeter.to_csv(out_path, index=False)
        return paymeter

# -----------------------------
# Step C: Merge Eko & Analyze → ONE Excel
# -----------------------------

def merge_and_analyze(
    eko_path: str,
    trans_path: str,
    district_info_path: Optional[str],
    kcg_path: Optional[str],
    out_detail: str,
    out_excel: str
) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    eko   = safe_read_csv(Path(eko_path))
    trans = safe_read_csv(Path(trans_path))

    # drop conflicting columns across sources defensively
    eko = eko.drop(columns=[c for c in ['Reference','Created At','Account Number','Transaction Amount'] if c in eko.columns], errors='ignore')
    trans = trans.drop(columns=[c for c in ['Request ID','Transaction Date','Account Number','Total Amount'] if c in trans.columns], errors='ignore')

    # create refs
    eko['ref']   = eko['Request ID'].astype(str).str.strip() if 'Request ID' in eko.columns else eko.index.astype(str)
    trans['ref'] = trans['Reference'].astype(str).str.strip() if 'Reference' in trans.columns else trans.index.astype(str)

    eko['source'] = 'eko'
    trans['source'] = 'paymeter'

    merged = pd.merge(eko, trans, on='ref', how='outer', suffixes=('_eko', '_trans'))

    # --- Parse/ensure a Created At column for monthly grouping ---
    created_candidates = ['Created At', 'Created_At', 'createdat', 'created_at', 'CreatedAt']
    created_col = next((c for c in created_candidates if c in merged.columns), None)
    txn_candidates_dates = ['Transaction Date', 'TransactionDate', 'transactiondate', 'Transaction Date_eko', 'transaction_date']
    txn_date_col = next((c for c in txn_candidates_dates if c in merged.columns), None)

    if created_col:
        merged['Created At'] = pd.to_datetime(merged[created_col], errors='coerce')
    elif txn_date_col:
        merged['Created At'] = pd.to_datetime(merged[txn_date_col], errors='coerce')
    else:
        merged['Created At'] = pd.NaT

    # --- District ---
    src_col = next((c for c in ['District Name', 'DISTRICT BY ADDRESS', 'District', 'district', 'DISTRICT'] if c in merged.columns and merged[c].notna().any()), None)
    merged['District'] = merged[src_col].astype(str).replace({'nan': None}).fillna('empty').astype(str).str.strip() if src_col else 'empty'

    # --- Amount detection (existing logic) ---
    def pick_amount(col_list):
        for c in col_list:
            if c in merged.columns:
                s = merged[c].astype(str).str.replace(r'[,\s₦$]', '', regex=True).str.strip()
                if s.replace('', '0').ne('').any():
                    return c
        return None

    txn_col = pick_amount(['Transaction Amount_trans','Transaction Amount','Transaction Amount_eko','Txn Amount','txn amount','Amount','amount','amt'])
    total_col = pick_amount(['Total Amount','Total Amount_eko','Total','total'])
    merged['Transaction Amount'] = pd.to_numeric(merged[txn_col].astype(str).str.replace(r'[,\s₦$]', '', regex=True), errors='coerce').fillna(0.0) if txn_col else 0.0
    merged['Total Amount'] = pd.to_numeric(merged[total_col].astype(str).str.replace(r'[,\s₦$]', '', regex=True), errors='coerce').fillna(0.0) if total_col else 0.0

    merged['fig_dif'] = merged['Total Amount'] - merged['Transaction Amount']
    merged['amt_less_vat'] = merged['Transaction Amount'] / 1.075
    merged['commission'] = merged['amt_less_vat'].apply(calculate_commission)

    # --- Load KCG anchor (fallback to DEFAULT_KCG) ---
    kcg_list = []
    if (kcg_path and os.path.exists(kcg_path)) or (not kcg_path and DEFAULT_KCG.exists()):
        actual = kcg_path if (kcg_path and os.path.exists(kcg_path)) else str(DEFAULT_KCG)
        try:
            kcg_df = safe_read_csv(Path(actual))
            kcg_col = pick_kcg_column(kcg_df)
            kcg_norm = kcg_df[kcg_col].astype(str).apply(normalize_acct).unique().tolist()
            kcg_list = [x for x in kcg_norm if x and len(x) >= 1]
        except Exception:
            kcg_list = []
    kcg_set = set(kcg_list)

    # --- Find candidate account-like columns (permissive) ---
    candidate_account_columns = []
    for c in merged.columns:
        lc = c.lower()
        if 'account' in lc or 'acct' in lc or 'accountnumber' in lc.replace(' ',''):
            candidate_account_columns.append(c)
        else:
            # sample some values and if many contain digits consider it
            try:
                sample = merged[c].astype(str).head(100).str.replace(r'\D','',regex=True)
                if sample.str.len().ge(4).mean() >= 0.3:
                    candidate_account_columns.append(c)
            except Exception:
                pass

    if not candidate_account_columns:
        merged['Account Number'] = merged.index.astype(str)
        candidate_account_columns = ['Account Number']

    # --- Build normalized tokens per row and attempt matching ---
    debug_rows = []
    matched_mask = pd.Series(False, index=merged.index)
    matched_rule = pd.Series("", index=merged.index)
    matched_kcg_value = pd.Series("", index=merged.index)

    def try_match(norm_val: str):
        if not norm_val:
            return (None, None)
        # exact
        if norm_val in kcg_set:
            return (norm_val, "exact")
        # suffix/endswith
        for k in kcg_list:
            if k and (norm_val.endswith(k) or k.endswith(norm_val)):
                return (k, "suffix")
        # last6
        x6 = norm_val[-6:] if len(norm_val) >= 6 else norm_val
        for k in kcg_list:
            if k and (k.endswith(x6) or x6.endswith(k[-6:])):
                return (k, "last6")
        # last4
        x4 = norm_val[-4:] if len(norm_val) >= 4 else norm_val
        for k in kcg_list:
            if k and (k.endswith(x4) or x4.endswith(k[-4:])):
                return (k, "last4")
        # substring (permissive)
        for k in kcg_list:
            if k and (k in norm_val or norm_val in k):
                return (k, "substring")
        return (None, None)

    # iterate candidate columns and populate debug and match attempts (use .items())
    for col in candidate_account_columns:
        try:
            norm_series = merged[col].astype(str).apply(normalize_acct)
        except Exception:
            norm_series = merged[col].astype(str).apply(normalize_acct) if col in merged.columns else pd.Series([""]*len(merged), index=merged.index)

        merged[f"{col}_norm"] = norm_series
        merged[f"{col}_last6"] = norm_series.apply(lambda s: s[-6:] if s and len(s) >= 6 else s)
        merged[f"{col}_last4"] = norm_series.apply(lambda s: s[-4:] if s and len(s) >= 4 else s)

        for idx, nv in norm_series.items():
            matched_val, rule = try_match(nv)
            debug_rows.append({
                "index": idx,
                "source_col": col,
                "raw_value": merged.at[idx, col] if col in merged.columns else "",
                "norm_value": nv,
                "last6": merged.at[idx, f"{col}_last6"],
                "last4": merged.at[idx, f"{col}_last4"],
                "matched_kcg": matched_val or "",
                "match_rule": rule or ""
            })
            if matched_val:
                matched_mask.at[idx] = True
                if not matched_rule.at[idx]:
                    matched_rule.at[idx] = rule or ""
                    matched_kcg_value.at[idx] = matched_val

    # textual flags
    text_flag = pd.Series(False, index=merged.index)
    for flag_col in ("Disco Commission Type","DiscoCommissionType","Commission Type","CommissionType","Remarks","Note"):
        if flag_col in merged.columns:
            try:
                text_flag = text_flag | merged[flag_col].fillna('').astype(str).str.contains('kcg', case=False, na=False)
            except Exception:
                pass

    merged['Is_KCG'] = (matched_mask | text_flag).fillna(False)
    merged['KCG_Matched_Value'] = matched_kcg_value
    merged['KCG_Match_Rule'] = matched_rule

    # split
    kcg_rows = merged.loc[merged['Is_KCG']].copy()
    non_kcg_rows = merged.loc[~merged['Is_KCG']].copy()

    # try to fill missing dates
    def fill_missing_dates(df):
        if df is None or df.empty:
            return df
        missing = df['Created At'].isna()
        if not missing.any():
            return df
        date_cols = [c for c in df.columns if 'transaction date' in c.lower() or 'transactiondate' in c.lower() or 'created' in c.lower()]
        for dc in date_cols:
            try:
                cand = pd.to_datetime(df[dc], errors='coerce')
                if cand.notna().any():
                    df.loc[missing, 'Created At'] = cand.loc[missing]
                    break
            except Exception:
                continue
        return df

    kcg_rows = fill_missing_dates(kcg_rows)
    non_kcg_rows = fill_missing_dates(non_kcg_rows)

    # --- Summaries & ranges ---
    main_summary = pd.DataFrame([
        {"Category":"All Accounts","Count":len(merged),"Transaction Amount":merged['Transaction Amount'].sum(),"Commission":merged['commission'].sum()},
        {"Category":"KCG Accounts","Count":len(kcg_rows),"Transaction Amount":kcg_rows['Transaction Amount'].sum(),"Commission":kcg_rows['commission'].sum()},
        {"Category":"Non-KCG Accounts","Count":len(non_kcg_rows),"Transaction Amount":non_kcg_rows['Transaction Amount'].sum(),"Commission":non_kcg_rows['commission'].sum()}
    ])

    bins = [0,10000,20000,40000,60000,80000,100000,200000,300000,500000,1000000,float("inf")]
    labels = ["0 - 10,000","10,001 - 20,000","20,001 - 40,000","40,001 - 60,000","60,001 - 80,000","80,001 - 100,000","100,001 - 200,000","200,001 - 300,000","300,001 - 500,000","500,001 - 1,000,000","1,000,001 and above"]
    if not non_kcg_rows.empty:
        non_kcg_rows = non_kcg_rows.assign(Amount_Range = pd.cut(non_kcg_rows['Transaction Amount'], bins=bins, labels=labels, right=True))
        non_kcg_ranges = non_kcg_rows.groupby('Amount_Range', observed=False).agg(Transaction_Count=('Transaction Amount','size'), Total_Amount=('Transaction Amount','sum'), Total_Commission=('commission','sum')).reset_index()
    else:
        non_kcg_ranges = pd.DataFrame(columns=['Amount_Range','Transaction_Count','Total_Amount','Total_Commission'])

    # account summary
    account_col_candidates = [c for c in merged.columns if 'account' in c.lower()]
    acct_col = account_col_candidates[0] if account_col_candidates else 'Account Number_trans'
    if acct_col not in merged.columns:
        merged[acct_col] = merged.get('Account Number_trans', merged.index.astype(str))
    merged[acct_col] = merged[acct_col].astype(str).apply(normalize_acct)
    cust_col = next((c for c in ['Customer Name','CustomerName','Name','Customer'] if c in merged.columns), None)
    if cust_col is None:
        merged['Customer Name'] = merged.get('Customer Name','')
        cust_col = 'Customer Name'
    account_summary = merged.groupby([acct_col,cust_col], as_index=False).agg(Transaction_Count=('Transaction Amount','size'), Total_Amount=('Transaction Amount','sum'), Total_Commission=('commission','sum'))
    top20 = account_summary.sort_values(by=['Transaction_Count','Total_Amount'], ascending=[False,False]).head(20)
    scenario_no_kcg = pd.DataFrame([{"Category":"Scenario: Non-KCG Only","Count":len(non_kcg_rows),"Transaction Amount":non_kcg_rows['Transaction Amount'].sum(),"Commission":non_kcg_rows['commission'].sum()}])

    # --- Monthly aggregates; ensure 'Unknown' included when Created At missing ---
    def produce_month_col(df):
        if df is None or df.empty:
            return pd.DataFrame(columns=['Month','Count','Transaction_Amount','Commission'])
        df_local = df.copy()
        df_local['Month'] = pd.to_datetime(df_local['Created At'], errors='coerce').dt.to_period('M')
        no_date = df_local['Month'].isna()
        if no_date.any():
            df_local.loc[no_date,'Month'] = 'Unknown'
        df_local['Month'] = df_local['Month'].astype(str)
        grouped = df_local.groupby('Month', observed=False).agg(Count=('Transaction Amount','size'), Transaction_Amount=('Transaction Amount','sum'), Commission=('commission','sum')).reset_index()
        return grouped

    monthly_non_kcg = produce_month_col(non_kcg_rows)
    monthly_kcg = produce_month_col(kcg_rows)
    monthly_all = produce_month_col(merged)

    # combine monthly trends
    def rename_cols(df,prefix):
        if df is None or df.empty:
            return pd.DataFrame(columns=['Month', f'{prefix}_Count', f'{prefix}_Transaction_Amount', f'{prefix}_Commission'])
        return df.rename(columns={'Count':f'{prefix}_Count','Transaction_Amount':f'{prefix}_Transaction_Amount','Commission':f'{prefix}_Commission'})

    ma = rename_cols(monthly_all,'All')
    mk = rename_cols(monthly_kcg,'KCG')
    mn = rename_cols(monthly_non_kcg,'NonKCG')
    monthly_trends_combined = pd.merge(ma,mk,on='Month',how='outer').merge(mn,on='Month',how='outer').fillna(0)

    # --- District summary with KCG breakdowns ---
    report = pd.DataFrame(merged['District'].unique(), columns=['District'])
    district_trans_totals = merged.groupby('District', dropna=False)['Transaction Amount'].sum().reset_index().rename(columns={'Transaction Amount':'paymeter_total'})
    district_eko_totals = merged.groupby('District', dropna=False)['Total Amount'].sum().reset_index().rename(columns={'Total Amount':'eko_total'})
    district_commission = merged.groupby('District', dropna=False)['commission'].sum().reset_index().rename(columns={'commission':'district_commission'})

    district_kcg_counts = merged.loc[merged['Is_KCG']].groupby('District', dropna=False).size().reset_index(name='kcg_count')
    district_kcg_pay = merged.loc[merged['Is_KCG']].groupby('District', dropna=False)['Transaction Amount'].sum().reset_index(name='kcg_paymeter_total')
    district_kcg_eko = merged.loc[merged['Is_KCG']].groupby('District', dropna=False)['Total Amount'].sum().reset_index(name='kcg_eko_total')
    district_kcg_comm = merged.loc[merged['Is_KCG']].groupby('District', dropna=False)['commission'].sum().reset_index(name='kcg_commission')

    district_non_kcg_counts = merged.loc[~merged['Is_KCG']].groupby('District', dropna=False).size().reset_index(name='nonkcg_count')
    district_non_kcg_pay = merged.loc[~merged['Is_KCG']].groupby('District', dropna=False)['Transaction Amount'].sum().reset_index(name='nonkcg_paymeter_total')
    district_non_kcg_eko = merged.loc[~merged['Is_KCG']].groupby('District', dropna=False)['Total Amount'].sum().reset_index(name='nonkcg_eko_total')
    district_non_kcg_comm = merged.loc[~merged['Is_KCG']].groupby('District', dropna=False)['commission'].sum().reset_index(name='nonkcg_commission')

    report = report.merge(district_trans_totals, on='District', how='left')\
                   .merge(district_eko_totals, on='District', how='left')\
                   .merge(district_commission, on='District', how='left')\
                   .merge(district_kcg_counts, on='District', how='left')\
                   .merge(district_kcg_pay, on='District', how='left')\
                   .merge(district_kcg_eko, on='District', how='left')\
                   .merge(district_kcg_comm, on='District', how='left')\
                   .merge(district_non_kcg_counts, on='District', how='left')\
                   .merge(district_non_kcg_pay, on='District', how='left')\
                   .merge(district_non_kcg_eko, on='District', how='left')\
                   .merge(district_non_kcg_comm, on='District', how='left')

    num_cols = ['paymeter_total','eko_total','district_commission','kcg_count','kcg_paymeter_total','kcg_eko_total','kcg_commission','nonkcg_count','nonkcg_paymeter_total','nonkcg_eko_total','nonkcg_commission']
    for c in num_cols:
        if c in report.columns:
            report[c] = pd.to_numeric(report[c], errors='coerce').fillna(0)

    report['difference'] = report['eko_total'] - report['paymeter_total']
    report['kcg_share_pct'] = report.apply(lambda r: (r['kcg_paymeter_total']/r['paymeter_total']*100.0) if r['paymeter_total'] else 0.0, axis=1)

    # --- Build DistrictSummaryTotal: include bank details from district_info if present ---
    # Prepare default empty bank columns
    district_summary_total = report[['District','paymeter_total','eko_total','district_commission','difference']].copy()
    district_summary_total['Bank name'] = ""
    district_summary_total['Bank acct name'] = ""
    district_summary_total['Bank acct number'] = ""

    # Try to load district_info (either provided path or DEFAULT)
    district_info_df = None
    if district_info_path and os.path.exists(district_info_path):
        try:
            district_info_df = safe_read_csv(Path(district_info_path))
        except Exception:
            district_info_df = None
    elif DEFAULT_DISTRICT_INFO.exists():
        try:
            district_info_df = safe_read_csv(DEFAULT_DISTRICT_INFO)
        except Exception:
            district_info_df = None

    if district_info_df is not None and not district_info_df.empty:
        # normalize district column name
        district_col = None
        for c in district_info_df.columns:
            if c.lower() in ('district','districtname','district_name','area'):
                district_col = c
                break
        if not district_col:
            # try find a column whose values match districts
            for c in district_info_df.columns:
                sample = district_info_df[c].astype(str).head(50)
                if sample.str.strip().str.len().gt(0).any():
                    # no strong guarantee — just pick the first non-empty column as district fallback
                    district_col = c
                    break

        # detect bank columns heuristically
        bank_name_col = None
        bank_acct_name_col = None
        bank_acct_num_col = None
        for c in district_info_df.columns:
            lc = c.lower().replace(' ','').replace('_','')
            if bank_name_col is None and any(k in lc for k in ('bankname','bank','bankname2','bankbranch')):
                bank_name_col = c
            if bank_acct_name_col is None and any(k in lc for k in ('accountname','acctname','beneficiary','beneficiaryname','accountname1','acctname1')):
                bank_acct_name_col = c
            if bank_acct_num_col is None and any(k in lc for k in ('accountnumber','acctnumber','acctno','accountno','account_number','acctnum')):
                bank_acct_num_col = c

        # normalize/rename to join
        join_df = district_info_df.copy()
        # ensure district_col exists
        if district_col is None:
            # nothing sensible to join on; attempt best-effort: if there's a column named 'district' in report, skip join
            district_col = None
        else:
            join_df = join_df[[c for c in [district_col, bank_name_col, bank_acct_name_col, bank_acct_num_col] if c in join_df.columns]]
            join_df = join_df.rename(columns={
                district_col: 'District',
                bank_name_col or '': 'Bank name' if bank_name_col else bank_name_col,
                bank_acct_name_col or '': 'Bank acct name' if bank_acct_name_col else bank_acct_name_col,
                bank_acct_num_col or '': 'Bank acct number' if bank_acct_num_col else bank_acct_num_col
            })
            # Clean columns: keep only those present
            cols_to_keep = [c for c in ['District','Bank name','Bank acct name','Bank acct number'] if c in join_df.columns]
            join_df = join_df[cols_to_keep]
            # Normalize District values
            join_df['District'] = join_df['District'].astype(str).str.strip()

            # Merge into district_summary_total on District
            district_summary_total = district_summary_total.merge(join_df, on='District', how='left')
            # If merge left duplicates existing columns, ensure final column names exist
            for col in ['Bank name','Bank acct name','Bank acct number']:
                if col not in district_summary_total.columns:
                    district_summary_total[col] = ""
            # Fill NaNs with empty strings
            for col in ['Bank name','Bank acct name','Bank acct number']:
                district_summary_total[col] = district_summary_total[col].fillna("")

    # Ensure numeric and ordering
    for c in ['paymeter_total','eko_total','district_commission','difference']:
        if c in district_summary_total.columns:
            district_summary_total[c] = pd.to_numeric(district_summary_total[c], errors='coerce').fillna(0.0)

    # Reorder columns exactly as requested
    desired_order = ['District','paymeter_total','eko_total','district_commission','difference','Bank name','Bank acct name','Bank acct number']
    for col in desired_order:
        if col not in district_summary_total.columns:
            # create missing columns as blank
            district_summary_total[col] = "" if col.startswith('Bank') else 0.0
    district_summary_total = district_summary_total[desired_order]

    # --- Build debug frames that will reveal why/what matched ---
    debug_df = pd.DataFrame(debug_rows)
    if debug_df.empty:
        debug_df = pd.DataFrame(columns=['index','source_col','raw_value','norm_value','last6','last4','matched_kcg','match_rule'])
    else:
        ctx_cols = [c for c in ['District','Transaction Amount','Total Amount','Created At','Is_KCG','KCG_Matched_Value','KCG_Match_Rule'] if c in merged.columns]
        ctx = merged[ctx_cols].reset_index().rename(columns={'index':'index'})
        debug_df = debug_df.merge(ctx, on='index', how='left')

    # KCG summary
    kcg_summary = pd.DataFrame([{
        "KCG_anchor_count": len(kcg_list),
        "Matched_rows_count": int(kcg_rows.shape[0]),
        "Matched_transaction_amount_total": float(kcg_rows['Transaction Amount'].sum()) if not kcg_rows.empty else 0.0,
        "Matched_commission_total": float(kcg_rows['commission'].sum()) if not kcg_rows.empty else 0.0
    }])

    # audit empty district rows
    audit_df = pd.DataFrame()
    try:
        empty_rows = merged[merged['District'].astype(str).str.lower() == 'empty']
        if not empty_rows.empty:
            audit_path = BASE_DIR / "Audit_empty_district_rows.csv"
            empty_rows.to_csv(audit_path, index=False)
            audit_df = empty_rows
            result['audit_empty_rows_path'] = str(audit_path)
    except Exception:
        pass

    # make columns unique before writing
    merged = make_columns_unique(merged)
    report = make_columns_unique(report)
    account_summary = make_columns_unique(account_summary)
    top20 = make_columns_unique(top20)
    main_summary = make_columns_unique(main_summary)
    non_kcg_ranges = make_columns_unique(non_kcg_ranges)
    scenario_no_kcg = make_columns_unique(scenario_no_kcg)
    monthly_non_kcg = make_columns_unique(monthly_non_kcg)
    monthly_kcg = make_columns_unique(monthly_kcg)
    monthly_trends_combined = make_columns_unique(monthly_trends_combined)
    audit_df = make_columns_unique(audit_df)
    debug_df = make_columns_unique(debug_df)
    kcg_summary = make_columns_unique(kcg_summary)
    district_summary_total = make_columns_unique(district_summary_total)

    try:
        with pd.ExcelWriter(out_excel, engine="openpyxl") as writer:
            main_summary.to_excel(writer, sheet_name="Main Summary", index=False)
            non_kcg_ranges.to_excel(writer, sheet_name="Non-KCG Ranges", index=False)
            account_summary.to_excel(writer, sheet_name="All Accounts Summary", index=False)
            top20.to_excel(writer, sheet_name="Top 20 Accounts", index=False)
            scenario_no_kcg.to_excel(writer, sheet_name="Scenario No KCG", index=False)
            monthly_non_kcg.to_excel(writer, sheet_name="Monthly Non-KCG", index=False)
            monthly_kcg.to_excel(writer, sheet_name="Monthly KCG", index=False)
            monthly_trends_combined.to_excel(writer, sheet_name="Monthly Trends (All)", index=False)
            report.to_excel(writer, sheet_name="District Summary", index=False)
            # new sheet
            district_summary_total.to_excel(writer, sheet_name="DistrictSummaryTotal", index=False)
            kcg_summary.to_excel(writer, sheet_name="KCG_Summary", index=False)
            if not debug_df.empty:
                debug_df.to_excel(writer, sheet_name="KCG_Debug", index=False)
            if not audit_df.empty:
                audit_df.to_excel(writer, sheet_name="Audit Empty District", index=False)
            merged.to_csv(out_detail, index=False)
    except Exception as e:
        raise RuntimeError(f"Error writing Excel: {e}")

    result.update({
        "merged_df": merged,
        "monthly_trends_combined": monthly_trends_combined,
        "top20": top20,
        "out_detail": out_detail,
        "out_excel": out_excel,
        "kcg_anchor_count": len(kcg_list),
        "kcg_matched_rows": int(kcg_rows.shape[0]),
        "kcg_matched_amount": float(kcg_rows['Transaction Amount'].sum()) if not kcg_rows.empty else 0.0
    })
    return result



# =============================================
# MODERN & FANCY STREAMLIT UI
# =============================================

st.set_page_config(page_title="Paymeter Pro", layout="wide", page_icon="lightning")

# === CUSTOM CSS (Fully Responsive + Code/Mobile Fixes) ===
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
    html, body, [class*="css"] {font-family: 'Inter', sans-serif;}
    
    .main > div {padding-top: 1rem;}
    .header-container {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 16px;
        color: white;
        box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        margin-bottom: 1.5rem;
        display: flex;
        align-items: center;
        gap: 1.5rem;
        height: 140px;
    }
    .header-logo {
        width: 120px;
        height: 120px;
        object-fit: contain;
        border-radius: 12px;
        background: transparent !important;
        box-shadow: none;
    }
    .header-text {
        flex: 1;
        display: flex;
        flex-direction: column;
        justify-content: center;
        text-align: center;
    }
    .header-title {font-size: 2.8rem; font-weight: 700; margin: 0;}
    .header-subtitle {font-size: 1.1rem; opacity: 0.9; margin-top: 0.5rem;}
    
    .big-button {
        background: linear-gradient(45deg, #FF6B6B, #FF8E53);
        color: white;
        font-size: 1.8rem !important;
        font-weight: 700;
        padding: 1.5rem 3rem !important;
        border: none;
        border-radius: 16px;
        box-shadow: 0 8px 25px rgba(255, 107, 107, 0.4);
        transition: all 0.3s ease;
        width: 100%;
        margin: 2rem 0;
    }
    .big-button:hover {
        transform: translateY(-5px);
        box-shadow: 0 12px 30px rgba(255, 107, 107, 0.6);
    }
    
    .card {
        background: rgba(255, 255, 255, 0.95);
        backdrop-filter: blur(10px);
        border-radius: 16px;
        padding: 1.5rem;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        border: 1px solid rgba(255,255,255,0.2);
        margin-bottom: 1.5rem;
    }
    .file-status {
        font-size: 0.9rem;
        display: flex;
        align-items: center;
        gap: 8px;
    }
    .stTabs [data-baseweb="tab-list"] {gap: 1rem;}
    .stTabs [data-baseweb="tab"] {
        background: #f0f2f6;
        border-radius: 12px;
        padding: 0.8rem 1.5rem;
        font-weight: 600;
        color: #555;
    }
    .stTabs [data-baseweb="tab"]:hover {background: #e0e6ed;}
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        background: linear-gradient(45deg, #667eea, #764ba2);
        color: white;
    }

    /* FIX: Readable text on small screens */
    @media (max-width: 768px) {
        .header-container {
            flex-direction: column;
            text-align: center;
            height: auto;
            padding: 1rem;
        }
        .header-logo {
            width: 80px;
            height: 80px;
        }
        .header-title {
            font-size: 2rem;
        }
        .header-subtitle {
            font-size: 1rem;
        }
        .big-button {
            font-size: 1.4rem !important;
            padding: 1rem 2rem !important;
        }
        .card {
            background: #ffffff !important;
            color: #212529 !important;
            border: 1px solid #dee2e6 !important;
        }
        .card h3, .card p, .card li, .card code, .card ol {
            color: #212529 !important;
        }
        /* FIX: Code blocks (e.g., paymeter_report.csv) visible on mobile */
        code, .st-emotion-cache-1trjexit code, [data-testid="stMarkdownContainer"] code {
            background-color: #f8f9fa !important;
            color: #000000 !important;
            border: 1px solid #e9ecef !important;
            padding: 0.2rem 0.4rem !important;
            border-radius: 4px !important;
            font-size: 0.9em !important;
        }
    }

    /* Global fix for code blocks (big screens too) */
    code, [data-testid="stMarkdownContainer"] code {
        background-color: #f8f9fa !important;
        color: #000000 !important;
        border: 1px solid #e9ecef !important;
        padding: 0.2rem 0.4rem !important;
        border-radius: 4px !important;
    }
</style>
""", unsafe_allow_html=True)

# === LOGO LOADING (File → Embedded Fallback) ===
logo_src = ""
logo_status = "Logo not loaded"

if LOGO_PATH.exists():
    try:
        with open(LOGO_PATH, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
            logo_src = f"data:image/png;base64,{b64}"
            logo_status = "Logo: Loaded from data/logo.png"
    except Exception as e:
        logo_status = f"File error: {e}"
else:
    logo_src = f"data:image/png;base64,{EMBEDDED_LOGO_BASE64.strip()}"
    logo_status = "Logo: Using embedded version"

st.markdown(f"""
<div class="header-container">
    <img src="{logo_src}" class="header-logo" alt="Logo">
    <div class="header-text">
        <h1 class="header-title">Paymeter Pro</h1>
        <p class="header-subtitle">Smart Repair • KCG Detection • One-Click Excel Report</p>
    </div>
</div>
""", unsafe_allow_html=True)

st.sidebar.info(logo_status)

# Initialize session state
if 'dates_checked' not in st.session_state:
    st.session_state.dates_checked = False
if 'pay_min' not in st.session_state:
    st.session_state.pay_min = None
if 'pay_max' not in st.session_state:
    st.session_state.pay_max = None
if 'eko_min' not in st.session_state:
    st.session_state.eko_min = None
if 'eko_max' not in st.session_state:
    st.session_state.eko_max = None

# === SIDEBAR ===
with st.sidebar:
    st.markdown("### Required Files")
    paymeter_file = st.file_uploader("**Paymeter Report CSV**", type=["csv"], key="paymeter")
    eko_file = st.file_uploader("**Eko Trans CSV**", type=["csv"], key="eko")

    st.markdown("---")
    st.markdown("### Optional Reference Files")
    district_upload = st.file_uploader("`district.csv`", type=["csv"], key="district")
    kcg_upload = st.file_uploader("`KCG.csv`", type=["csv"], key="kcg")
    district_info_upload = st.file_uploader("`district_acct_number.csv`", type=["csv"], key="distinfo")

    st.markdown("---")
    st.markdown("#### File Status")
    def status(path, upload, name):
        if upload:
            st.markdown(f"<div class='file-status'>Uploaded {name} <span style='color:green'>Uploaded</span></div>", unsafe_allow_html=True)
        elif path.exists():
            st.markdown(f"<div class='file-status'>Default {name} <span style='color:#4CAF50'>Loaded</span></div>", unsafe_allow_html=True)
        else:
            st.markdown(f"<div class='file-status'>Not loaded {name} <span style='color:#999'>Not loaded</span></div>", unsafe_allow_html=True)

    status(DEFAULT_DISTRICT, district_upload, "district.csv")
    status(DEFAULT_KCG, kcg_upload, "KCG.csv")
    status(DEFAULT_DISTRICT_INFO, district_info_upload, "district_acct_number.csv")

    st.markdown("---")
    preview_limit = st.slider("Preview repaired rows", 1, 20, 8)

    check_dates = st.button("Check Date Ranges", key="check_dates")

    default_start = st.session_state.pay_min if st.session_state.pay_min else date.today()
    default_end = st.session_state.pay_max if st.session_state.pay_max else date.today()
    date_range = st.date_input(
        "Select Report Date Range",
        value=(default_start, default_end),
        min_value=st.session_state.pay_min,
        max_value=st.session_state.pay_max,
        key="date_range"
    )

    run = st.button("GENERATE REPORT", key="run", help="Click to process and download full report")

# === TABS ===
tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Preview", "Results", "Logs"])

with tab1:
    st.markdown("""
    <div class="card">
        <h3>How to Use Paymeter Pro</h3>
        <ol>
            <li><strong>Upload Required Files</strong>: <code>paymeter_report.csv</code> and <code>Eko Trans.csv</code></li>
            <li><strong>Check Date Ranges</strong>: Click to view available dates</li>
            <li><strong>Select Date Range</strong>: Choose dates within the detected range</li>
            <li><strong>Click "GENERATE REPORT"</strong></li>
        </ol>
        <p><strong>Warning</strong>: Dates outside the data range will stop the report.</p>
    </div>
    """, unsafe_allow_html=True)

    if st.session_state.dates_checked:
        if st.session_state.pay_min and st.session_state.pay_max:
            st.info(f"Paymeter Date Range: {st.session_state.pay_min} to {st.session_state.pay_max}")
        else:
            st.warning("No 'Created At' column found in Paymeter report or invalid dates.")
        if st.session_state.eko_min and st.session_state.eko_max:
            st.info(f"Eko Date Range: {st.session_state.eko_min} to {st.session_state.eko_max}")
        else:
            st.warning("No 'Transaction Date' column found in Eko report or invalid dates.")

    col1, col2, col3 = st.columns(3)
    m1 = col1.empty()
    m2 = col2.empty()
    m3 = col3.empty()
    m1.metric("Rows Processed", "—")
    m2.metric("Rows Fixed", "—")
    m3.metric("Total Amount", "—")

with tab2: preview_area = st.empty()
with tab3: results_area = st.empty()
with tab4: log_area = st.empty()

# === CHECK DATE RANGES ===
if check_dates:
    if not paymeter_file or not eko_file:
        st.error("Please upload both required CSV files.")
    else:
        with tempfile.TemporaryDirectory() as tmpdirname:
            paymeter_path = Path(tmpdirname) / "paymeter_report.csv"
            eko_path = Path(tmpdirname) / "eko_trans.csv"
            with open(paymeter_path, "wb") as f: f.write(paymeter_file.getbuffer())
            with open(eko_path, "wb") as f: f.write(eko_file.getbuffer())

            pay_df = safe_read_csv(paymeter_path)
            eko_df = safe_read_csv(eko_path)

            try:
                pay_dates = pd.to_datetime(pay_df['Created At'], errors='coerce').dropna()
                if not pay_dates.empty:
                    st.session_state.pay_min = pay_dates.min().date()
                    st.session_state.pay_max = pay_dates.max().date()
                else:
                    st.session_state.pay_min = st.session_state.pay_max = None
            except KeyError:
                st.session_state.pay_min = st.session_state.pay_max = None

            try:
                eko_dates = pd.to_datetime(eko_df['Transaction Date'], errors='coerce').dropna()
                if not eko_dates.empty:
                    st.session_state.eko_min = eko_dates.min().date()
                    st.session_state.eko_max = eko_dates.max().date()
                else:
                    st.session_state.eko_min = st.session_state.eko_max = None
            except KeyError:
                st.session_state.eko_min = st.session_state.eko_max = None

            st.session_state.dates_checked = True
            st.rerun()

# === RUN PIPELINE ===
if run:
    if not st.session_state.dates_checked:
        st.error("Please check date ranges first.")
    elif not paymeter_file or not eko_file:
        st.error("Please upload both required CSV files.")
    else:
        work_dir = Path(tempfile.mkdtemp(prefix="paymeter_"))
        st.sidebar.success(f"Working: `{work_dir.name}`")

        fixed_count = 0
        out_detail = out_excel = None

        try:
            paymeter_path = work_dir / "paymeter_report.csv"
            eko_path = work_dir / "eko_trans.csv"
            with open(paymeter_path, "wb") as f: f.write(paymeter_file.getbuffer())
            with open(eko_path, "wb") as f: f.write(eko_file.getbuffer())

            district_path = DEFAULT_DISTRICT if DEFAULT_DISTRICT.exists() and not district_upload else None
            if district_upload:
                district_path = work_dir / "district.csv"
                with open(district_path, "wb") as f: f.write(district_upload.getbuffer())

            kcg_path = DEFAULT_KCG if DEFAULT_KCG.exists() and not kcg_upload else None
            if kcg_upload:
                kcg_path = work_dir / "kcg.csv"
                with open(kcg_path, "wb") as f: f.write(kcg_upload.getbuffer())

            district_info_path = DEFAULT_DISTRICT_INFO if DEFAULT_DISTRICT_INFO.exists() and not district_info_upload else None
            if district_info_upload:
                district_info_path = work_dir / "district_info.csv"
                with open(district_info_path, "wb") as f: f.write(district_info_upload.getbuffer())

            with st.spinner("Repairing address spills..."):
                cleaned = work_dir / "cleaned.csv"
                fixed_count, examples = repair_address_spill(str(paymeter_path), str(cleaned), preview_limit=preview_limit)

            with st.spinner("Merging district data..."):
                bydistrict = work_dir / "bydistrict.csv"
                if district_path:
                    merge_districts(str(cleaned), str(district_path), str(bydistrict))
                else:
                    shutil.copy2(cleaned, bydistrict)

            trans_df = safe_read_csv(bydistrict)
            eko_df   = safe_read_csv(eko_path)

            if 'Created At' in trans_df.columns:
                trans_df['Created At'] = pd.to_datetime(trans_df['Created At'], errors='coerce')
            if 'Transaction Date' in eko_df.columns:
                eko_df['Transaction Date'] = pd.to_datetime(eko_df['Transaction Date'], errors='coerce')

            start_date, end_date = date_range if isinstance(date_range, tuple) and len(date_range) == 2 else (None, None)
            if start_date and end_date:
                start_ts = pd.Timestamp(start_date)
                end_ts   = pd.Timestamp(end_date)

                pay_min = pd.Timestamp(st.session_state.pay_min) if st.session_state.pay_min else None
                pay_max = pd.Timestamp(st.session_state.pay_max) if st.session_state.pay_max else None
                eko_min = pd.Timestamp(st.session_state.eko_min) if st.session_state.eko_min else None
                eko_max = pd.Timestamp(st.session_state.eko_max) if st.session_state.eko_max else None

                errors = []
                if pay_min and pay_max:
                    if start_ts < pay_min:
                        errors.append(f"Start date ({start_date}) is before Paymeter data starts ({pay_min.date()})")
                    if end_ts > pay_max:
                        errors.append(f"End date ({end_date}) is after Paymeter data ends ({pay_max.date()})")
                if eko_min and eko_max:
                    if start_ts < eko_min:
                        errors.append(f"Start date ({start_date}) is before Eko data starts ({eko_min.date()})")
                    if end_ts > eko_max:
                        errors.append(f"End date ({end_date}) is after Eko data ends ({eko_max.date()})")

                if errors:
                    st.error("**Invalid Date Range Selected**")
                    for err in errors:
                        st.warning(f"Warning: {err}")
                    st.stop()

                with st.spinner("Filtering data by selected date range..."):
                    def _date_floor(s):
                        return pd.to_datetime(s).dt.floor('D')

                    if 'Created At' in trans_df.columns:
                        trans_mask = (_date_floor(trans_df['Created At']) >= start_ts) & (_date_floor(trans_df['Created At']) <= end_ts)
                        trans_df = trans_df[trans_mask]

                    if 'Transaction Date' in eko_df.columns:
                        eko_mask = (_date_floor(eko_df['Transaction Date']) >= start_ts) & (_date_floor(eko_df['Transaction Date']) <= end_ts)
                        eko_df = eko_df[eko_mask]

            filtered_trans = work_dir / "filtered_trans.csv"
            filtered_eko = work_dir / "filtered_eko.csv"
            trans_df.to_csv(filtered_trans, index=False)
            eko_df.to_csv(filtered_eko, index=False)

            with st.spinner("Generating final report..."):
                out_detail = work_dir / "detail.csv"
                timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                out_excel = work_dir / f"PaymeterReport_{timestamp}.xlsx"
                result = merge_and_analyze(
                    str(filtered_eko), str(filtered_trans),
                    str(district_info_path) if district_info_path else None,
                    str(kcg_path) if kcg_path else None,
                    str(out_detail), str(out_excel)
                )

            detail_df = safe_read_csv(Path(out_detail))
            txn_sum = pd.to_numeric(detail_df['Transaction Amount'].astype(str).str.replace(r'[,\s₦$]', '', regex=True), errors='coerce').fillna(0).sum()

            with tab1:
                m1.metric("Rows Processed", len(detail_df))
                m2.metric("Rows Fixed", fixed_count)
                m3.metric("Total Amount", f"₦{txn_sum:,.2f}")

            with tab2:
                if examples:
                    st.success(f"Fixed {fixed_count} rows")
                    for ex in examples:
                        st.markdown(f"**Line {ex['line']}**")
                        b, a = st.columns(2)
                        b.code(" → ".join(ex['before']))
                        a.code(" → ".join(ex['after']))
                st.dataframe(detail_df.head(10))

            with tab3:
                st.balloons()
                st.success("Report Generated!")
                c1, c2 = st.columns(2)
                with c1:
                    with open(cleaned, "rb") as f:
                        st.download_button("Cleaned Paymeter (Full)", f.read(), "cleaned.csv", "text/csv")
                    with open(out_detail, "rb") as f:
                        st.download_button("Detailed Merged (Filtered)", f.read(), "detail.csv", "text/csv")
                with c2:
                    with open(out_excel, "rb") as f:
                        st.download_button(
                            "DOWNLOAD FULL REPORT (Excel)",
                            f.read(),
                            out_excel.name,
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        )
                st.info(f"**{out_excel.name}** includes **12+ sheets**")

            with tab4:
                log_area.code(f"Fixed: {fixed_count}\nDetail: {out_detail}\nExcel: {out_excel}")

        except Exception as e:
            st.error(f"Error: {e}")
            log_area.text(str(e))
