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
import base64  # For logo encoding
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime

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
LOGO_PATH = DATA_DIR / "Logo.png"

# -----------------------------
# Utility helpers (unchanged)
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
    paymeter = pd.read_csv(paymeter_cleaned, dtype=str, keep_default_na=False)
    if district_path and os.path.exists(district_path):
        district = pd.read_csv(district_path, dtype=str, keep_default_na=False)
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
                paymeter.to_csv(out_path, index=False)
                return paymeter

        merged = paymeter.merge(district, on='Account Number', how='left')
        merged['District Name'] = merged['DISTRICT BY ADDRESS'].combine_first(merged.get('District Name', pd.Series(dtype=str)))
        merged.to_csv(out_path, index=False)
        return merged
    else:
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
    eko = pd.read_csv(eko_path, dtype=str, keep_default_na=False)
    trans = pd.read_csv(trans_path, dtype=str, keep_default_na=False)

    if 'Request ID' in eko.columns:
        eko['ref'] = eko['Request ID'].astype(str).str.strip()
    elif 'ref' in eko.columns:
        eko['ref'] = eko['ref'].astype(str).str.strip()
    else:
        eko['ref'] = eko.index.astype(str)

    if 'Reference' in trans.columns:
        trans['ref'] = trans['Reference'].astype(str).str.strip()
    elif 'ref' in trans.columns:
        trans['ref'] = trans['ref'].astype(str).str.strip()
    else:
        trans['ref'] = trans.index.astype(str)

    eko['source'] = 'eko'
    trans['source'] = 'paymeter'

    merged = pd.merge(eko, trans, on='ref', how='outer', suffixes=('_eko', '_trans'))

    src_col = None
    for candidate in ['District Name', 'DISTRICT BY ADDRESS', 'District', 'district', 'DISTRICT']:
        if candidate in merged.columns and merged[candidate].notna().any():
            src_col = candidate
            break
    merged['District'] = merged[src_col].astype(str).replace({'nan': None}).fillna('empty').astype(str).str.strip() if src_col else 'empty'

    def pick_amount(col_list: List[str]) -> Optional[str]:
        for c in col_list:
            if c in merged.columns:
                s = merged[c].astype(str).str.replace(r'[,\s₦$]', '', regex=True).str.strip()
                if s.replace('', '0').ne('').any():
                    return c
        return None

    txn_candidates = ['Transaction Amount_trans', 'Transaction Amount', 'Transaction Amount_eko', 'Txn Amount', 'txn amount', 'Amount', 'amount', 'amt']
    total_candidates = ['Total Amount', 'Total Amount_eko', 'Total', 'total']
    txn_col = pick_amount(txn_candidates)
    total_col = pick_amount(total_candidates)

    if txn_col:
        merged['Transaction Amount'] = pd.to_numeric(merged[txn_col].astype(str).str.replace(r'[,\s₦$]', '', regex=True), errors='coerce').fillna(0.0)
    else:
        merged['Transaction Amount'] = 0.0

    if total_col:
        merged['Total Amount'] = pd.to_numeric(merged[total_col].astype(str).str.replace(r'[,\s₦$]', '', regex=True), errors='coerce').fillna(0.0)
    else:
        merged['Total Amount'] = 0.0

    merged['fig_dif'] = merged['Total Amount'] - merged['Transaction Amount']
    merged['amt_less_vat'] = merged['Transaction Amount'] / 1.075
    merged['commission'] = merged['amt_less_vat'].apply(calculate_commission)

    kcg_accounts = set()
    if kcg_path and os.path.exists(kcg_path):
        try:
            kcg_df = pd.read_csv(kcg_path, dtype=str, keep_default_na=False)
            kcg_col = pick_kcg_column(kcg_df)
            kcg_accounts = set(kcg_df[kcg_col].astype(str).apply(normalize_acct))
        except Exception:
            pass

    possible_account_columns = []
    for c in merged.columns:
        lc = c.lower()
        if 'account' in lc or 'acct' in lc or 'accountnumber' in lc.replace(' ', ''):
            possible_account_columns.append(c)
    for candidate in ['Account Number', 'Account Number_trans', 'Account Number_eko', 'account', 'account_no', 'Acct', 'acct_no']:
        if candidate in merged.columns and candidate not in possible_account_columns:
            possible_account_columns.append(candidate)

    matched_any = pd.Series(False, index=merged.index)
    for col in possible_account_columns:
        try:
            norm_series = merged[col].astype(str).apply(normalize_acct)
            merged[f"{col}_norm"] = norm_series
            if kcg_accounts:
                matched = norm_series.isin(kcg_accounts)
                matched_any = matched_any | matched
        except Exception:
            continue

    text_flag = pd.Series(False, index=merged.index)
    for flag_col in ("Disco Commission Type", "DiscoCommissionType", "Commission Type", "CommissionType", "Remarks", "Note"):
        if flag_col in merged.columns:
            try:
                text_flag = text_flag | merged[flag_col].fillna('').astype(str).str.contains('kcg', case=False, na=False)
            except Exception:
                pass

    merged['Is_KCG'] = (matched_any | text_flag).fillna(False)

    kcg_rows = merged.loc[merged['Is_KCG']].copy()
    non_kcg_rows = merged.loc[~merged['Is_KCG']].copy()

    main_summary = pd.DataFrame([
        {"Category": "All Accounts", "Count": len(merged),
         "Transaction Amount": merged['Transaction Amount'].sum(),
         "Commission": merged['commission'].sum()},
        {"Category": "KCG Accounts", "Count": len(kcg_rows),
         "Transaction Amount": kcg_rows['Transaction Amount'].sum(),
         "Commission": kcg_rows['commission'].sum()},
        {"Category": "Non-KCG Accounts", "Count": len(non_kcg_rows),
         "Transaction Amount": non_kcg_rows['Transaction Amount'].sum(),
         "Commission": non_kcg_rows['commission'].sum()}
    ])

    bins = [0, 10000, 20000, 40000, 60000, 80000, 100000, 200000, 300000, 500000, 1000000, float("inf")]
    labels = [
        "0 - 10,000", "10,001 - 20,000", "20,001 - 40,000", "40,001 - 60,000",
        "60,001 - 80,000", "80,001 - 100,000", "100,001 - 200,000",
        "200,001 - 300,000", "300,001 - 500,000", "500,001 - 1,000,000",
        "1,000,001 and above"
    ]
    if not non_kcg_rows.empty:
        non_kcg_rows = non_kcg_rows.assign(Amount_Range=pd.cut(non_kcg_rows['Transaction Amount'], bins=bins, labels=labels, right=True))
        non_kcg_ranges = non_kcg_rows.groupby('Amount_Range', observed=False).agg(
            Transaction_Count=('Transaction Amount', 'size'),
            Total_Amount=('Transaction Amount', 'sum'),
            Total_Commission=('commission', 'sum')
        ).reset_index()
    else:
        non_kcg_ranges = pd.DataFrame(columns=['Amount_Range','Transaction_Count','Total_Amount','Total_Commission'])

    account_col_candidates = [c for c in merged.columns if 'account' in c.lower()]
    acct_col = account_col_candidates[0] if account_col_candidates else 'Account Number_trans'
    if acct_col not in merged.columns:
        merged[acct_col] = merged.get('Account Number_trans', merged.index.astype(str))
    merged[acct_col] = merged[acct_col].astype(str).apply(normalize_acct)

    cust_col = None
    for c in ['Customer Name', 'CustomerName', 'Name', 'Customer']:
        if c in merged.columns:
            cust_col = c
            break
    if cust_col is None:
        merged['Customer Name'] = merged.get('Customer Name', '')
        cust_col = 'Customer Name'

    account_summary = merged.groupby([acct_col, cust_col], as_index=False).agg(
        Transaction_Count=('Transaction Amount','size'),
        Total_Amount=('Transaction Amount','sum'),
        Total_Commission=('commission','sum')
    )

    top20 = account_summary.sort_values(by=['Transaction_Count','Total_Amount'], ascending=[False, False]).head(20)

    scenario_no_kcg = pd.DataFrame([{
        "Category": "Scenario: Non-KCG Only",
        "Count": len(non_kcg_rows),
        "Transaction Amount": non_kcg_rows['Transaction Amount'].sum(),
        "Commission": non_kcg_rows['commission'].sum()
    }])

    created_candidates = ['Created At', 'Created_At', 'createdat', 'created_at', 'CreatedAt']
    created_col = None
    for c in created_candidates:
        if c in merged.columns:
            created_col = c
            break
    if created_col:
        merged['Created At'] = pd.to_datetime(merged[created_col], errors='coerce')
        kcg_rows['Created At'] = pd.to_datetime(kcg_rows.get(created_col, kcg_rows.get('Created At', pd.Series(dtype=str))), errors='coerce')
        non_kcg_rows['Created At'] = pd.to_datetime(non_kcg_rows.get(created_col, non_kcg_rows.get('Created At', pd.Series(dtype=str))), errors='coerce')
    else:
        merged['Created At'] = pd.NaT
        kcg_rows['Created At'] = pd.NaT
        non_kcg_rows['Created At'] = pd.NaT

    monthly_non_kcg = non_kcg_rows.assign(Month=non_kcg_rows['Created At'].dt.to_period('M')).groupby('Month', observed=False).agg(
        Count=('Transaction Amount','size'),
        Transaction_Amount=('Transaction Amount','sum'),
        Commission=('commission','sum')
    ).reset_index()
    monthly_kcg = kcg_rows.assign(Month=kcg_rows['Created At'].dt.to_period('M')).groupby('Month', observed=False).agg(
        Count=('Transaction Amount','size'),
        Transaction_Amount=('Transaction Amount','sum'),
        Commission=('commission','sum')
    ).reset_index()
    monthly_all = merged.assign(Month=merged['Created At'].dt.to_period('M')).groupby('Month', observed=False).agg(
        All_Count=('Transaction Amount','size'),
        All_Transaction_Amount=('Transaction Amount','sum'),
        All_Commission=('commission','sum')
    ).reset_index()

    monthly_trends_combined = monthly_all.copy()
    if not monthly_trends_combined.empty:
        monthly_trends_combined = monthly_trends_combined.merge(monthly_kcg, on='Month', how='outer', suffixes=('', '_KCG'))
        monthly_trends_combined = monthly_trends_combined.merge(monthly_non_kcg, on='Month', how='outer', suffixes=('', '_NonKCG')).fillna(0)
    else:
        monthly_trends_combined = pd.DataFrame(columns=[
            'Month', 'All_Count', 'All_Transaction_Amount', 'All_Commission',
            'Count', 'Transaction_Amount', 'Commission'
        ])

    def build_projection(monthly_df):
        proj = pd.DataFrame()
        if monthly_df is not None and not monthly_df.empty:
            window = min(3, len(monthly_df))
            recent = monthly_df.tail(window)
            last_month = monthly_df['Month'].iloc[-1]
            try:
                next_month = str(last_month + 1)
            except Exception:
                next_month = "proj"
            proj = pd.DataFrame([{
                "Month": next_month,
                "Projected_Count": round(recent['Count'].mean(), 0) if 'Count' in recent.columns else 0,
                "Projected_Amount": round(recent['Transaction_Amount'].mean(), 2) if 'Transaction_Amount' in recent.columns else 0.0,
                "Projected_Commission": round(recent['Commission'].mean(), 2) if 'Commission' in recent.columns else 0.0,
                "Basis": f"Average of last {window} month(s)"
            }])
        return proj

    projection_non_kcg = build_projection(monthly_non_kcg)
    projection_kcg = build_projection(monthly_kcg)

    # District Summary
    report = pd.DataFrame(merged['District'].unique(), columns=['District'])
    district_trans_totals = merged.groupby('District', dropna=False)['Transaction Amount'].sum().reset_index().rename(columns={'Transaction Amount': 'paymeter_total'})
    district_eko_totals = merged.groupby('District', dropna=False)['Total Amount'].sum().reset_index().rename(columns={'Total Amount': 'eko_total'})
    district_commission = merged.groupby('District', dropna=False)['commission'].sum().reset_index().rename(columns={'commission': 'district_commission'})
    report = report.merge(district_trans_totals, on='District', how='left')\
                   .merge(district_eko_totals, on='District', how='left')\
                   .merge(district_commission, on='District', how='left')
    for c in ['paymeter_total','eko_total','district_commission']:
        if c in report.columns:
            report[c] = report[c].fillna(0.0)
    report['difference'] = report['eko_total'] - report['paymeter_total']

    if district_info_path and os.path.exists(district_info_path):
        try:
            district_info = pd.read_csv(district_info_path, dtype=str, keep_default_na=False)
            if 'district' in district_info.columns:
                district_info = district_info.rename(columns={'district': 'District'})
            report = report.merge(district_info, on='District', how='left')
        except Exception:
            pass

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

    # WRITE ONE EXCEL
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
            (projection_non_kcg if not projection_non_kcg.empty else pd.DataFrame(columns=["Month","Projected_Count","Projected_Amount","Projected_Commission","Basis"])).to_excel(writer, sheet_name="Projection Non-KCG", index=False)
            (projection_kcg if not projection_kcg.empty else pd.DataFrame(columns=["Month","Projected_Count","Projected_Amount","Projected_Commission","Basis"])).to_excel(writer, sheet_name="Projection KCG", index=False)
            report.to_excel(writer, sheet_name="District Summary", index=False)
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
        "out_excel": out_excel
    })
    return result

# =============================================
# MODERN & FANCY STREAMLIT UI
# =============================================

st.set_page_config(page_title="Paymeter Pro", layout="wide", page_icon="lightning")

# === CUSTOM CSS ===
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
        height: 140px;  /* Fixed height for logo fit */
    }
    .header-logo {
        width: 120px;
        height: 120px;
        object-fit: contain;
        border-radius: 12px;
        background: transparent !important; /* Blend to gradient */
        box-shadow: none; /* Remove shadow for better blend */
    }
    .header-text {
        flex: 1;
        text-align: left;
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
</style>
""", unsafe_allow_html=True)

# === HEADER WITH GRADIENT UNDER LOGO ===
logo_src = ""
logo_status = ""
if LOGO_PATH.exists():
    try:
        with open(LOGO_PATH, "rb") as logo_file:
            logo_bytes = logo_file.read()
            logo_base64 = base64.b64encode(logo_bytes).decode("utf-8")
            logo_src = f"data:image/png;base64,{logo_base64}"
            logo_status = "Logo loaded successfully!"
    except Exception as e:
        logo_status = f"Error loading logo: {e}"
else:
    logo_status = "Logo.png not found in data/ — no logo shown."

st.markdown(f"""
<div class="header-container">
    <img src="{logo_src}" class="header-logo" alt="Logo">
    <div class="header-text">
        <h1 class="header-title">Paymeter Pro</h1>
        <p class="header-subtitle">Smart Repair • KCG Detection • One-Click Excel Report</p>
    </div>
</div>
""", unsafe_allow_html=True)

# Debug status (remove after testing)
st.sidebar.info(logo_status)

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

    # Show status
    st.markdown("---")
    st.markdown("#### File Status")
    def status(path, upload, name):
        if upload:
            st.markdown(f"<div class='file-status'>✅ {name} <span style='color:green'>Uploaded</span></div>", unsafe_allow_html=True)
        elif path.exists():
            st.markdown(f"<div class='file-status'>✅ {name} <span style='color:#4CAF50'>Default loaded</span></div>", unsafe_allow_html=True)
        else:
            st.markdown(f"<div class='file-status'>❌ {name} <span style='color:#999'>Not loaded</span></div>", unsafe_allow_html=True)

    status(DEFAULT_DISTRICT, district_upload, "district.csv")
    status(DEFAULT_KCG, kcg_upload, "KCG.csv")
    status(DEFAULT_DISTRICT_INFO, district_info_upload, "district_acct_number.csv")

    st.markdown("---")
    preview_limit = st.slider("Preview repaired rows", 1, 20, 8)

    # BIG BUTTON
    run = st.button("GENERATE REPORT", key="run", help="Click to process and download full report")

# === TABS ===
tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Preview", "Results", "Logs"])

# === OVERVIEW TAB WITH INSTRUCTIONS ===
with tab1:
    st.markdown("""
    <div class="card">
        <h3>How to Use Paymeter Pro</h3>
        <ol>
            <li><strong>Upload Required Files</strong>: <code>paymeter_report.csv</code> and <code>Eko Trans.csv</code></li>
            <li><strong>Optional Files</strong>: Upload or use defaults in <code>data/</code> folder:
                <ul>
                    <li><code>district.csv</code> → Maps account to district</li>
                    <li><code>KCG.csv</code> → List of KCG accounts</li>
                    <li><code>district_acct_number.csv</code> → Extra info</li>
                </ul>
            </li>
            <li><strong>Click "GENERATE REPORT"</strong> → Wait for magic</li>
            <li><strong>Download</strong> the timestamped Excel with <strong>12+ sheets</strong></li>
        </ol>
        <p><strong>Tip</strong>: Test with small files first!</p>
    </div>
    """, unsafe_allow_html=True)

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

# === RUN PIPELINE ===
if run:
    if not paymeter_file or not eko_file:
        st.error("Please upload both required CSV files.")
    else:
        work_dir = Path(tempfile.mkdtemp(prefix="paymeter_"))
        st.sidebar.success(f"Working: `{work_dir.name}`")

        fixed_count = 0
        out_detail = out_excel = None

        try:
            # Save files
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

            # Step 1
            with st.spinner("Repairing address spills..."):
                cleaned = work_dir / "cleaned.csv"
                fixed_count, examples = repair_address_spill(str(paymeter_path), str(cleaned), preview_limit=preview_limit)

            # Step 2
            with st.spinner("Merging district data..."):
                bydistrict = work_dir / "bydistrict.csv"
                if district_path:
                    merge_districts(str(cleaned), str(district_path), str(bydistrict))
                else:
                    shutil.copy2(cleaned, bydistrict)

            # Step 3
            with st.spinner("Generating final report..."):
                out_detail = work_dir / "detail.csv"
                timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                out_excel = work_dir / f"PaymeterReport_{timestamp}.xlsx"
                result = merge_and_analyze(
                    str(eko_path), str(bydistrict),
                    str(district_info_path) if district_info_path else None,
                    str(kcg_path) if kcg_path else None,
                    str(out_detail), str(out_excel)
                )

            detail_df = pd.read_csv(out_detail, dtype=str, keep_default_na=False)
            txn_sum = pd.to_numeric(detail_df['Transaction Amount'].astype(str).str.replace(r'[,\s₦$]', '', regex=True), errors='coerce').fillna(0).sum()

            # === UPDATE UI ===
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
                        st.download_button("Cleaned Paymeter", f.read(), "cleaned.csv", "text/csv")
                    with open(out_detail, "rb") as f:
                        st.download_button("Detailed Merged", f.read(), "detail.csv", "text/csv")
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
