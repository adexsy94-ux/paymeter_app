# merged_recon_app.py
# -*- coding: utf-8 -*-
"""
Merged Reconciliation App
Combines:
- EKO vs Paymeter (from paymeter_app.py)
- Providus vs VPS (from Providus_recon.py)
- VPS-Providus vs Paymeter (from VPS_Paymeter.py)

Run: streamlit run merged_recon_app.py
"""

import csv
import re
import os
import shutil
import tempfile
import base64
import io
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any
from datetime import datetime, date

import pandas as pd
import numpy as np
import streamlit as st
import streamlit.components.v1 as components
from openpyxl import load_workbook

# -----------------------------
# AUTO-INSTALL xlrd (for .xls)
# -----------------------------
try:
    import xlrd  # noqa: F401
except ImportError:
    st.warning("Installing `xlrd` for .xls support...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "xlrd"])
    import xlrd

# -----------------------------
# Config / defaults (shared)
# -----------------------------
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)
DEFAULT_DISTRICT = DATA_DIR / "district.csv"
DEFAULT_KCG = DATA_DIR / "KCG.csv"
DEFAULT_DISTRICT_INFO = DATA_DIR / "district_acct_number.csv"
LOGO_PATH = DATA_DIR / "logo.png"

# Placeholder base64 logo
EMBEDDED_LOGO_BASE64 = """
iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==
"""

# -----------------------------
# Shared Helpers
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

# Universal file reader (merged from all apps)
def read_file_any(uploaded_file, local_path=None, sheet_name=None, dtype=str):
    def _read_df(source, suffix, engine=None):
        if suffix == ".csv":
            return pd.read_csv(source, dtype=dtype)
        else:
            return pd.read_excel(source, sheet_name=sheet_name, engine=engine, dtype=dtype)

    if uploaded_file is not None:
        try:
            name = uploaded_file.name if hasattr(uploaded_file, "name") else ""
            suffix = Path(name).suffix.lower()
            engine = "openpyxl" if suffix == ".xlsx" else "xlrd" if suffix == ".xls" else None
            df = _read_df(uploaded_file, suffix, engine)
            return make_columns_unique(df)
        except Exception as e:
            st.error(f"Failed to read {name}: {e}")
            return None

    if local_path and Path(local_path).exists():
        suffix = Path(local_path).suffix.lower()
        engine = "openpyxl" if suffix == ".xlsx" else "xlrd" if suffix == ".xls" else None
        df = _read_df(local_path, suffix, engine)
        return make_columns_unique(df)
    return None

# Repair address spill (from first app)
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
                new_row += [""] * (expected_cols - len(row))

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

# Merge districts (from first app)
def merge_districts(paymeter_cleaned: str, district_path: str, out_path: str) -> pd.DataFrame:
    paymeter = read_file_any(None, paymeter_cleaned)
    if district_path and os.path.exists(district_path):
        district = read_file_any(None, district_path)
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

# Merge and analyze (from first app)
def merge_and_analyze(
    eko_path: str,
    trans_path: str,
    district_info_path: Optional[str],
    kcg_path: Optional[str],
    out_detail: str,
    out_excel: str
) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    eko   = read_file_any(None, eko_path)
    trans = read_file_any(None, trans_path)

    eko_keep = ['Request ID', 'Transaction Date', 'Account Number', 'Total Amount']
    trans_keep = ['Reference', 'Created At', 'Account Number', 'Transaction Amount']

    eko = eko.drop(columns=[c for c in trans_keep if c in eko.columns], errors='ignore')
    trans = trans.drop(columns=[c for c in eko_keep if c in trans.columns], errors='ignore')

    if 'Request ID' in eko.columns:
        eko['ref'] = eko['Request ID'].astype(str).str.strip()
    else:
        eko['ref'] = eko.index.astype(str)

    if 'Reference' in trans.columns:
        trans['ref'] = trans['Reference'].astype(str).str.strip()
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
            kcg_df = read_file_any(None, kcg_path)
            kcg_col = pick_kcg_column(kcg_df)
            kcg_accounts = set(kcg_df[kcg_col].astype(str).apply(normalize_acct))
        except Exception:
            pass

    possible_account_columns = []
    for c in merged.columns:
        lc = c.lower()
        if 'account' in lc or 'acct' in lc or 'accountnumber' in lc.replace(' ', ''):
            possible_account_columns.append(c)

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
            district_info = read_file_any(None, district_info_path)
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
    projection_non_kcg = make_columns_unique(projection_non_kcg)
    projection_kcg = make_columns_unique(projection_kcg)
    audit_df = make_columns_unique(audit_df)

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
            projection_non_kcg.to_excel(writer, sheet_name="Projection Non-KCG", index=False)
            projection_kcg.to_excel(writer, sheet_name="Projection KCG", index=False)
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

# From second app: clean_numeric_text_col, parse_vps_date, parse_prv_date
def clean_numeric_text_col(col):
    if col is None: return col
    s = col.astype(str).astype("string")
    s = s.str.replace(r"[^\d\.\-]", "", regex=True)
    return pd.to_numeric(s, errors="coerce")

def parse_vps_date(series):
    s = series.astype(str).replace({"nan": None})
    parsed_utc = pd.to_datetime(s, errors="coerce", utc=True)
    mask_fail = parsed_utc.isna()
    if mask_fail.any():
        fallback = pd.to_datetime(series[mask_fail], errors='coerce', dayfirst=True)
        fallback_utc = pd.to_datetime(fallback, errors="coerce", utc=True)
        parsed_utc.loc[mask_fail] = fallback_utc
    try:
        parsed_local = parsed_utc.dt.tz_convert("Africa/Lagos").dt.normalize()
    except Exception:
        parsed_utc2 = pd.to_datetime(parsed_utc.dt.tz_localize("UTC", ambiguous="NaT", nonexistent="NaT"), errors="coerce", utc=True)
        parsed_local = parsed_utc2.dt.tz_convert("Africa/Lagos").dt.normalize()
    return pd.to_datetime(parsed_local.dt.tz_localize(None), errors="coerce")

def parse_prv_date(series):
    s = series.astype(str).replace({"nan": None})
    parsed = pd.to_datetime(s, errors="coerce", dayfirst=True)
    parsed = pd.to_datetime(parsed, errors="coerce")
    mask_valid = parsed.notna()
    if mask_valid.any():
        try:
            parsed_loc = parsed.copy()
            parsed_loc.loc[mask_valid] = parsed_loc.loc[mask_valid].dt.tz_localize("Africa/Lagos", ambiguous="NaT", nonexistent="NaT")
            parsed_loc = parsed_loc.dt.tz_convert("Africa/Lagos").dt.normalize()
            return pd.to_datetime(parsed_loc.dt.tz_localize(None), errors="coerce")
        except Exception:
            return pd.to_datetime(parsed.dt.normalize(), errors="coerce")
    return parsed

# Run VPS recon enhanced (from second app)
def run_vps_recon_enhanced(prv_df, vps_df, opts, date_tolerance_days=3, progress_callback=None):
    prv = prv_df.copy()
    vps = vps_df.copy()

    prv.columns = prv.columns.astype(str).str.strip()
    vps.columns = vps.columns.astype(str).str.strip()

    PRV_COL_DATE = opts.get("PRV_COL_DATE", "Transaction Date")
    PRV_COL_CREDIT = opts.get("PRV_COL_CREDIT", "Credit Amount")
    PRV_NARRATION_COL = opts.get("PRV_NARRATION_COL", "Transaction Details")
    PRV_COL_DEBIT = opts.get("PRV_COL_DEBIT", "Debit Amount")
    VPS_COL_DATE = opts.get("VPS_COL_DATE", "created_at")
    VPS_COL_SETTLED = opts.get("VPS_COL_SETTLED", "settled_amount_minor")
    VPS_COL_CHARGE = opts.get("VPS_COL_CHARGE", "charge_amount_minor")

    if PRV_COL_CREDIT not in prv.columns:
        raise KeyError(f"PROVIDUS missing column '{PRV_COL_CREDIT}'")
    for c in (VPS_COL_DATE, VPS_COL_SETTLED, VPS_COL_CHARGE):
        if c not in vps.columns:
            raise KeyError(f"VPS missing column '{c}'")

    if PRV_COL_DEBIT in prv.columns:
        prv = prv.drop(columns=[PRV_COL_DEBIT])

    prv[PRV_COL_CREDIT] = clean_numeric_text_col(prv[PRV_COL_CREDIT])
    vps["_raw_settled_clean"] = clean_numeric_text_col(vps[VPS_COL_SETTLED])
    vps[VPS_COL_CHARGE] = clean_numeric_text_col(vps[VPS_COL_CHARGE])

    before = len(prv)
    prv = prv[prv[PRV_COL_CREDIT].notna()].copy()
    prv = prv.dropna(how="all").reset_index(drop=True)

    prv["_parsed_date"] = parse_prv_date(prv[PRV_COL_DATE])
    vps["_parsed_date"] = parse_vps_date(vps[VPS_COL_DATE])

    prv["_credit_main"] = prv[PRV_COL_CREDIT].astype(float)
    vps["_settled_numeric"] = vps["_raw_settled_clean"].astype(float)

    vps["_used"] = False

    ref_to_idx = {}
    possible_ref_cols = ["settlement_ref", "session_id", "account_ref_code", "settlement_notification_retry_batch_id"]
    for c in possible_ref_cols:
        if c in vps.columns:
            for idx, val in vps[c].dropna().astype(str).items():
                key = val.strip()
                if key:
                    ref_to_idx.setdefault(key, []).append(idx)

    vps_valid = vps.dropna(subset=["_parsed_date"])
    vps_by_date_idx = {d: list(g.index) for d, g in vps_valid.groupby("_parsed_date")}

    prv["vps_settled_amount"] = pd.NA
    prv["vps_charge_amount"] = pd.NA
    prv["vps_matched"] = False
    prv["vps_match_reason"] = pd.NA
    prv["vps_matched_vps_index"] = pd.NA

    narration_col = PRV_NARRATION_COL if PRV_NARRATION_COL in prv.columns else None
    if narration_col:
        prv["_tran_details_lower"] = prv[narration_col].astype(str).str.lower()

    matched = 0
    total_rows = len(prv)

    for prv_idx, prv_row in prv.iterrows():
        if progress_callback:
            progress_callback(prv_idx + 1, total_rows)

        if prv_row.get("vps_matched", False):
            continue

        p_amount = float(prv_row["_credit_main"]) if pd.notna(prv_row["_credit_main"]) else None
        p_date = prv_row["_parsed_date"]

        # 1. Reference token match
        if opts.get("ref_matching", True) and narration_col:
            details = prv_row["_tran_details_lower"] or ""
            for ref_key, idx_list in ref_to_idx.items():
                if not ref_key or ref_key.lower() not in details:
                    continue
                candidate_indices = [i for i in idx_list if not vps.at[i, "_used"]]
                if candidate_indices:
                    chosen_idx = candidate_indices[0]
                    vps.at[chosen_idx, "_used"] = True
                    found = vps.loc[chosen_idx]
                    prv.at[prv_idx, "vps_settled_amount"] = found.get(VPS_COL_SETTLED, found["_raw_settled_clean"])
                    prv.at[prv_idx, "vps_charge_amount"] = found.get(VPS_COL_CHARGE, pd.NA)
                    prv.at[prv_idx, "vps_matched"] = True
                    prv.at[prv_idx, "vps_match_reason"] = f"matched by ref token '{ref_key}'"
                    prv.at[prv_idx, "vps_matched_vps_index"] = int(chosen_idx)
                    matched += 1
                    break
            if prv.at[prv_idx, "vps_matched"]:
                continue

        # 2. Same date + amount
        if p_date is not None:
            cand_idx = [i for i in vps_by_date_idx.get(p_date, []) if not vps.at[i, "_used"]]
            if cand_idx:
                cand_df = vps.loc[cand_idx].copy()
                diffs = np.abs(cand_df["_settled_numeric"].astype(float) - p_amount)
                mask = diffs <= 0.005
                if mask.any():
                    found = cand_df[mask].iloc[0]
                    found_idx = found.name
                    vps.at[found_idx, "_used"] = True
                    prv.at[prv_idx, "vps_settled_amount"] = found[VPS_COL_SETTLED]
                    prv.at[prv_idx, "vps_charge_amount"] = found[VPS_COL_CHARGE]
                    prv.at[prv_idx, "vps_matched"] = True
                    prv.at[prv_idx, "vps_match_reason"] = "date & amount match"
                    prv.at[prv_idx, "vps_matched_vps_index"] = int(found_idx)
                    matched += 1
                    continue
                credit_x100 = p_amount * 100.0
                diffs2 = np.abs(cand_df["_settled_numeric"].astype(float) - credit_x100)
                mask2 = diffs2 <= 0.5
                if mask2.any():
                    found = cand_df[mask2].iloc[0]
                    found_idx = found.name
                    vps.at[found_idx, "_used"] = True
                    prv.at[prv_idx, "vps_settled_amount"] = found[VPS_COL_SETTLED]
                    prv.at[prv_idx, "vps_charge_amount"] = found[VPS_COL_CHARGE]
                    prv.at[prv_idx, "vps_matched"] = True
                    prv.at[prv_idx, "vps_match_reason"] = "date match & settled==credit*100"
                    prv.at[prv_idx, "vps_matched_vps_index"] = int(found_idx)
                    matched += 1
                    continue

        # 3. ±N days
        if p_date is not None and opts.get("plus_minus_N_days", True) and date_tolerance_days > 0:
            outer_break = False
            for delta in range(1, date_tolerance_days + 1):
                for sign in (-1, 1):
                    alt_date = p_date + pd.Timedelta(days=sign * delta)
                    alt_idx_list = vps_by_date_idx.get(alt_date, [])
                    alt_idx_list = [i for i in alt_idx_list if not vps.at[i, "_used"]]
                    if not alt_idx_list:
                        continue
                    alt_df = vps.loc[alt_idx_list].copy()
                    diffs_alt = np.abs(alt_df["_settled_numeric"].astype(float) - p_amount)
                    mask_alt = diffs_alt <= 0.005
                    if mask_alt.any():
                        found = alt_df[mask_alt].iloc[0]
                        found_idx = found.name
                        vps.at[found_idx, "_used"] = True
                        prv.at[prv_idx, "vps_settled_amount"] = found[VPS_COL_SETTLED]
                        prv.at[prv_idx, "vps_charge_amount"] = found[VPS_COL_CHARGE]
                        prv.at[prv_idx, "vps_matched"] = True
                        prv.at[prv_idx, "vps_match_reason"] = f"amount match on {alt_date.date()} (±{date_tolerance_days}d)"
                        prv.at[prv_idx, "vps_matched_vps_index"] = int(found_idx)
                        matched += 1
                        outer_break = True
                        break
                    diffs_alt2 = np.abs(alt_df["_settled_numeric"].astype(float) - (p_amount * 100.0))
                    mask_alt2 = diffs_alt2 <= 0.5
                    if mask_alt2.any():
                        found = alt_df[mask_alt2].iloc[0]
                        found_idx = found.name
                        vps.at[found_idx, "_used"] = True
                        prv.at[prv_idx, "vps_settled_amount"] = found[VPS_COL_SETTLED]
                        prv.at[prv_idx, "vps_charge_amount"] = found[VPS_COL_CHARGE]
                        prv.at[prv_idx, "vps_matched"] = True
                        prv.at[prv_idx, "vps_match_reason"] = f"credit*100 match on {alt_date.date()} (±{date_tolerance_days}d)"
                        prv.at[prv_idx, "vps_matched_vps_index"] = int(found_idx)
                        matched += 1
                        outer_break = True
                        break
                if outer_break:
                    break

        # 4. Amount-only fallback
        if not prv.at[prv_idx, "vps_matched"] and opts.get("amount_only_fallback", False):
            global_avail = vps[(vps["_used"] == False) & vps["_settled_numeric"].notna()].copy()
            if not global_avail.empty:
                diffs_g = np.abs(global_avail["_settled_numeric"].astype(float) - p_amount)
                mask_g = diffs_g <= 0.005
                if mask_g.any():
                    found = global_avail[mask_g].iloc[0]
                    found_idx = found.name
                    vps.at[found_idx, "_used"] = True
                    prv.at[prv_idx, "vps_settled_amount"] = found[VPS_COL_SETTLED]
                    prv.at[prv_idx, "vps_charge_amount"] = found[VPS_COL_CHARGE]
                    prv.at[prv_idx, "vps_matched"] = True
                    prv.at[prv_idx, "vps_match_reason"] = "amount-only fallback"
                    prv.at[prv_idx, "vps_matched_vps_index"] = int(found_idx)
                    matched += 1
                    continue
                diffs_g2 = np.abs(global_avail["_settled_numeric"].astype(float) - (p_amount * 100.0))
                mask_g2 = diffs_g2 <= 0.5
                if mask_g2.any():
                    found = global_avail[mask_g2].iloc[0]
                    found_idx = found.name
                    vps.at[found_idx, "_used"] = True
                    prv.at[prv_idx, "vps_settled_amount"] = found[VPS_COL_SETTLED]
                    prv.at[prv_idx, "vps_charge_amount"] = found[VPS_COL_CHARGE]
                    prv.at[prv_idx, "vps_matched"] = True
                    prv.at[prv_idx, "vps_match_reason"] = "amount*100 fallback"
                    prv.at[prv_idx, "vps_matched_vps_index"] = int(found_idx)
                    matched += 1
                    continue

    vps_unmatched = vps[vps["_used"] != True].copy()

    # Merge VPS fields
    matched_vps = vps[vps["_used"] == True].copy()
    rename_map = {
        "id": "vps_id", "session_id": "vps_session_id", "settlement_ref": "vps_settlement_ref",
        "transaction_amount_minor": "vps_transaction_amount_minor", "source_acct_name": "vps_source_acct_name",
        "source_acct_no": "vps_source_acct_no", "virtual_acct_no": "vps_virtual_acct_no",
        "created_at": "vps_created_at", "reversal_session_id": "vps_reversal_session_id",
        "settlement_notification_retry_batch_id": "vps_settlement_notification_retry_batch_id"
    }
    matched_vps = matched_vps.rename(columns=rename_map)
    vps_merge_cols = [v for k, v in rename_map.items() if k in vps.columns]
    matched_vps = matched_vps[vps_merge_cols]

    out_prv = prv.merge(matched_vps, left_on="vps_matched_vps_index", right_index=True, how="left")

    # === Excel Report ===
    helper_cols = ["_parsed_date", "_credit_main", "_tran_details_lower"]
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        out_prv.drop(columns=[c for c in helper_cols if c in out_prv.columns], errors="ignore") \
               .to_excel(writer, sheet_name="Cleaned_PROVIDUS", index=False)
        log_cols = [
            PRV_COL_DATE, PRV_COL_CREDIT, "vps_matched", "vps_match_reason",
            "vps_settled_amount", "vps_charge_amount", "vps_id", "vps_session_id"
        ]
        out_prv[[c for c in log_cols if c in out_prv.columns]].to_excel(writer, sheet_name="Match_Log", index=False)
        out_prv[out_prv["vps_matched"] != True].to_excel(writer, sheet_name="Unmatched_PROVIDUS", index=False)
        vps_unmatched.reset_index(drop=True).to_excel(writer, sheet_name="Unmatched_VPS", index=False)
        vps.reset_index(drop=True).to_excel(writer, sheet_name="All_VPS_Input", index=False)
    excel_buffer.seek(0)

    # === CSV Buffers ===
    csv_buffers = {}
    for name in ["Cleaned_PROVIDUS", "Match_Log", "Unmatched_PROVIDUS", "Unmatched_VPS", "All_VPS_Input"]:
        if name == "Cleaned_PROVIDUS":
            csv_buffers[name] = out_prv.drop(columns=[c for c in helper_cols if c in out_prv.columns], errors="ignore").to_csv(index=False)
        elif name == "Match_Log":
            csv_buffers[name] = out_prv[[c for c in log_cols if c in out_prv.columns]].to_csv(index=False)
        elif name == "Unmatched_PROVIDUS":
            csv_buffers[name] = out_prv[out_prv["vps_matched"] != True].to_csv(index=False)
        elif name == "Unmatched_VPS":
            csv_buffers[name] = vps_unmatched.reset_index(drop=True).to_csv(index=False)
        else:
            csv_buffers[name] = vps.reset_index(drop=True).to_csv(index=False)

    stats = {
        "prv_before": before,
        "prv_after": len(out_prv),
        "vps_matched": matched,
        "unmatched_prv": len(out_prv) - matched,
        "unmatched_vps": len(vps_unmatched)
    }

    return out_prv, vps_unmatched, excel_buffer, csv_buffers, stats, vps

# From third app: find_col_by_norm, find_sheet_case_insensitive, repair_csv_bytes
def find_col_by_norm(columns, target):
    if target is None:
        return None
    tnorm = _norm(target)
    for c in columns:
        if _norm(c) == tnorm:
            return c
    tokens = re.findall(r"[a-z0-9]+", tnorm)
    if not tokens:
        return None
    for c in columns:
        cnorm = _norm(c)
        if all(tok in cnorm for tok in tokens):
            return c
    return None

def find_sheet_case_insensitive(xls, target_name="Cleaned_PROVIDUS"):
    target_norm = _norm(target_name)
    for name in xls.sheet_names:
        if _norm(name) == target_norm:
            return name
    for name in xls.sheet_names:
        n = _norm(name)
        if "cleaned" in n and "providus" in n:
            return name
    return None

def repair_csv_bytes(b: bytes):
    txt = None
    for enc in ("utf-8", "latin1", "cp1252"):
        try:
            txt = b.decode(enc)
            break
        except Exception:
            txt = None
    if txt is None:
        raise ValueError("Could not decode CSV file using utf-8/latin1/cp1252.")
    lines = txt.splitlines()
    if not lines:
        raise ValueError("CSV file appears empty.")
    try:
        header_row = next(csv.reader([lines[0]]))
    except Exception as e:
        raise ValueError(f"Could not parse header row: {e}")
    expected_cols = len(header_row)
    ADDRESS_CANDS = ["address", "customer address", "service address", "customeraddress", "serviceaddress", "addr"]
    TXN_AMT_CANDS = ["transaction amount", "txn amount", "amount", "amt", "transactionamount", "txnamount"]
    H = [_norm(h) for h in header_row]
    addr_idx = find_col_index(header_row, ADDRESS_CANDS)
    txn_idx = find_col_index(header_row, TXN_AMT_CANDS)
    if addr_idx is None:
        raise ValueError("Could not find Address column.")
    if txn_idx is None:
        raise ValueError("Could not find Transaction Amount column.")
    repaired_lines = []
    repaired_lines.append(",".join(header_row))
    reader = csv.reader(lines)
    row_num = 0
    fixed_count = 0
    examples = []
    for row in reader:
        row_num += 1
        if row_num == 1:
            continue
        original = row[:]
        if len(row) <= txn_idx:
            row = row + [""] * (txn_idx + 1 - len(row))
        if txn_idx < len(row) and is_amount(row[txn_idx]):
            if len(row) > expected_cols:
                row = row[:expected_cols]
            elif len(row) < expected_cols:
                row = row + [""] * (expected_cols - len(row))
            repaired_lines.append(",".join(['"{}"'.format(x.replace('"','""')) if ("," in str(x) or '"' in str(x)) else str(x) for x in row]))
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
            repaired_lines.append(",".join(['"{}"'.format(x.replace('"','""')) if ("," in str(x) or '"' in str(x)) else str(x) for x in row]))
            continue
        if not spill:
            if len(row) > expected_cols:
                row = row[:expected_cols]
            elif len(row) < expected_cols:
                row = row + [""] * (expected_cols - len(row))
            repaired_lines.append(",".join(['"{}"'.format(x.replace('"','""')) if ("," in str(x) or '"' in str(x)) else str(x) for x in row]))
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
        repaired_lines.append(",".join(['"{}"'.format(x.replace('"','""')) if ("," in str(x) or '"' in str(x)) else str(x) for x in new_row]))
        fixed_count += 1
        if len(examples) < 6:
            examples.append({"line": row_num, "before": original, "after": new_row})
    cleaned_text = "\n".join(repaired_lines)
    return cleaned_text, {"fixed_count": fixed_count, "examples": examples}

# Read VPS (from third app)
def read_vps_bytes(uploaded_bytes):
    try:
        uploaded_bytes.seek(0)
    except Exception:
        pass
    try:
        xls = pd.ExcelFile(uploaded_bytes)
        sheet = find_sheet_case_insensitive(xls, "Cleaned_PROVIDUS")
        if sheet is None:
            st.error(f"Uploaded VPS workbook does not contain a sheet named like 'Cleaned_PROVIDUS'. Available sheets: {xls.sheet_names}")
            return None
        df = pd.read_excel(xls, sheet_name=sheet, dtype=str)
        df.columns = make_columns_unique(df.columns)
        return df
    except Exception as e:
        st.error(f"Error reading VPS workbook: {e}")
        return None

# Read Paymeter (from third app)
def read_paymeter_bytes(uploaded_bytes):
    lower = getattr(uploaded_bytes, "name", "").lower() if hasattr(uploaded_bytes, "name") else ""
    try:
        uploaded_bytes.seek(0)
    except Exception:
        pass
    # Excel
    if lower.endswith(".xls") or lower.endswith(".xlsx"):
        try:
            xls = pd.ExcelFile(uploaded_bytes)
            sheet = xls.sheet_names[0]
            df = pd.read_excel(xls, sheet_name=sheet, dtype=str)
            df.columns = make_columns_unique(df.columns)
            return df
        except Exception as e:
            st.error(f"Error reading Paymeter Excel file: {e}")
            return None
    # CSV
    try:
        uploaded_bytes.seek(0)
        try:
            df = pd.read_csv(uploaded_bytes, dtype=str)
            df.columns = make_columns_unique(df.columns)
            return df
        except Exception:
            st.warning("Initial CSV parse failed — attempting automated repair...")
            uploaded_bytes.seek(0)
            b = uploaded_bytes.read()
            cleaned_text, meta = repair_csv_bytes(b)
            st.info(f"CSV repair completed — rows adjusted: {meta.get('fixed_count', 0)}")
            df = pd.read_csv(io.StringIO(cleaned_text), dtype=str)
            df.columns = make_columns_unique(df.columns)
            return df
    except Exception as e:
        st.error(f"Error reading Paymeter CSV file: {e}")
        return None

# VPS Paymeter recon (from third app)
def run_vps_paymeter_recon(vps_df, paymeter_df, opts):
    prefix_len = opts.get("prefix_len", 15)
    rrn_column_input = opts.get("rrn_column_input", "RRN")
    vps_ref_column_input = opts.get("vps_ref_column_input", "vps_settlement_ref")
    vps_virtual_acct_col_input = opts.get("vps_virtual_acct_col_input", "vps_virtual_acct_no")
    paymeter_account_col_input = opts.get("paymeter_account_col_input", "Account Number")
    pm_cols_chosen = opts.get("pm_cols_chosen", ["RRN", "Account Number", "Transaction Amount", "Transaction ID", "Customer Name"])
    include_json = opts.get("include_json", False)
    clean_debit_col = opts.get("clean_debit_col", True)
    drop_empty_rows = opts.get("drop_empty_rows", True)

    vps_ref_col_actual = find_col_by_norm(vps_df.columns, vps_ref_column_input) or vps_ref_column_input
    vps_virtual_col_actual = find_col_by_norm(vps_df.columns, vps_virtual_acct_col_input) or vps_virtual_acct_col_input
    pay_rrn_col_actual = find_col_by_norm(paymeter_df.columns, rrn_column_input) or rrn_column_input
    pay_account_col_actual = find_col_by_norm(paymeter_df.columns, paymeter_account_col_input) or paymeter_account_col_input

    missing = []
    if vps_ref_col_actual not in vps_df.columns:
        missing.append(f"VPS ref ({vps_ref_column_input})")
    if vps_virtual_col_actual not in vps_df.columns:
        missing.append(f"VPS virtual acct ({vps_virtual_acct_col_input})")
    if pay_rrn_col_actual not in paymeter_df.columns:
        missing.append(f"Paymeter RRN ({rrn_column_input})")
    if pay_account_col_actual not in paymeter_df.columns:
        missing.append(f"Paymeter Account Number ({paymeter_account_col_input})")
    if missing:
        raise ValueError("Missing required columns: " + "; ".join(missing))

    if clean_debit_col:
        debit_col = find_col_by_norm(vps_df.columns, "Debit Amount")
        if debit_col and debit_col in vps_df.columns:
            vps_df[debit_col] = ""

    if drop_empty_rows:
        vps_df.replace("", pd.NA, inplace=True)
        vps_df.dropna(axis=0, how="all", inplace=True)
        vps_df.fillna("", inplace=True)

    vps_df[vps_ref_col_actual] = vps_df[vps_ref_col_actual].astype(str).fillna("")
    vps_df[vps_virtual_col_actual] = vps_df[vps_virtual_col_actual].astype(str).fillna("")
    paymeter_df[pay_rrn_col_actual] = paymeter_df[pay_rrn_col_actual].astype(str).fillna("")
    paymeter_df[pay_account_col_actual] = paymeter_df[pay_account_col_actual].astype(str).fillna("")

    vps_df = vps_df.reset_index(drop=True)
    vps_df["_vps_idx"] = range(len(vps_df))
    vps_df["_lookup_key"] = vps_df[vps_ref_col_actual].str.slice(0, prefix_len)
    vps_df["_vps_virtual_acct"] = vps_df[vps_virtual_col_actual].astype(str)

    paymeter_df = paymeter_df.reset_index(drop=True)
    paymeter_df["_pm_row_id"] = range(len(paymeter_df))
    paymeter_df["_pm_key"] = paymeter_df[pay_rrn_col_actual].str.slice(0, prefix_len)
    paymeter_df["_pm_account"] = paymeter_df[pay_account_col_actual].astype(str)

    lookup_keys = pd.Index(vps_df["_lookup_key"].unique())
    lookup_accounts = pd.Index(vps_df["_vps_virtual_acct"].unique())

    mask_key = paymeter_df["_pm_key"].isin(lookup_keys)
    mask_acct = paymeter_df["_pm_account"].isin(lookup_accounts)
    prefilter_mask = mask_key & mask_acct

    filtered_count = int(prefilter_mask.sum())

    keep_pm_cols = ["_pm_row_id", "_pm_key", "_pm_account"]
    for col_choice in pm_cols_chosen:
        real = find_col_by_norm(paymeter_df.columns, col_choice)
        if real and real not in keep_pm_cols:
            keep_pm_cols.append(real)
    if pay_rrn_col_actual not in keep_pm_cols:
        keep_pm_cols.append(pay_rrn_col_actual)
    if pay_account_col_actual not in keep_pm_cols:
        keep_pm_cols.append(pay_account_col_actual)

    if include_json:
        keep_pm_cols = list(paymeter_df.columns)

    paymeter_small = paymeter_df.loc[prefilter_mask, keep_pm_cols].copy()

    if len(paymeter_small) > 0:
        paymeter_small["_pm_key"] = paymeter_small["_pm_key"].astype("category")
        paymeter_small["_pm_account"] = paymeter_small["_pm_account"].astype("category")
        vps_df["_lookup_key"] = vps_df["_lookup_key"].astype("category")
        vps_df["_vps_virtual_acct"] = vps_df["_vps_virtual_acct"].astype("category")

    pm_rename = {}
    for c in paymeter_small.columns:
        if c in ("_pm_row_id", "_pm_key", "_pm_account"):
            pm_rename[c] = c
        else:
            pm_rename[c] = f"PM_{c}"
    paymeter_small = paymeter_small.rename(columns=pm_rename)

    merged = vps_df.merge(
        paymeter_small,
        left_on=["_lookup_key", "_vps_virtual_acct"],
        right_on=["_pm_key", "_pm_account"],
        how="left",
        sort=False
    )

    pm_row_id_col = "_pm_row_id"
    merged["_is_matched"] = merged[pm_row_id_col].notna()
    matched_counts = merged.groupby("_vps_idx")["_is_matched"].sum().astype(int).rename("matched_count")
    merged = merged.merge(matched_counts, left_on="_vps_idx", right_index=True, how="left")
    merged["matched_count"] = merged["matched_count"].fillna(0).astype(int)

    pm_rrn_col = f"PM_{pay_rrn_col_actual}" if f"PM_{pay_rrn_col_actual}" in merged.columns else None
    if pm_rrn_col:
        rrn_lists = merged.groupby("_vps_idx")[pm_rrn_col].apply(lambda s: ",".join([str(x) for x in s.dropna().astype(str) if str(x).strip()!=""])).rename("matched_rrn_list")
        merged = merged.merge(rrn_lists, left_on="_vps_idx", right_index=True, how="left")
        merged["matched_rrn_list"] = merged["matched_rrn_list"].fillna("")
    else:
        merged["matched_rrn_list"] = ""

    if include_json:
        paymeter_group = paymeter_small.groupby(["_pm_key", "_pm_account"]).apply(lambda df: df.drop(columns=[c for c in ["_pm_row_id", "_pm_key", "_pm_account"] if c in df.columns], errors='ignore').fillna("").to_dict(orient="records")).rename("pm_matches_list")
        merged = merged.merge(paymeter_group, left_on=["_lookup_key", "_vps_virtual_acct"], right_index=True, how="left")
        merged["pm_matches_list"] = merged["pm_matches_list"].apply(lambda x: x if isinstance(x, list) else [])
        merged["PM_all_matches_json"] = merged["pm_matches_list"].apply(lambda x: json.dumps(x, ensure_ascii=False))
    else:
        merged["PM_all_matches_json"] = ""

    original_vps_cols = [c for c in vps_df.columns if c not in ("_vps_idx", "_lookup_key", "_vps_virtual_acct")]
    merged = merged.sort_values(by=["_vps_idx", pm_row_id_col], na_position="last").reset_index(drop=True)
    merged["_match_order"] = merged.groupby("_vps_idx").cumcount() + 1
    mask_extra = merged["_match_order"] > 1
    if mask_extra.any():
        for col in original_vps_cols:
            if col in merged.columns:
                merged.loc[mask_extra, col] = ""

    merged["match_index"] = merged["_match_order"]
    merged["continued"] = merged["match_index"] > 1

    final_cols = []
    for c in original_vps_cols:
        if c in merged.columns:
            final_cols.append(c)
    if vps_virtual_col_actual in merged.columns and vps_virtual_col_actual not in final_cols:
        final_cols.append(vps_virtual_col_actual)
    for c in ["matched_count", "matched_rrn_list"]:
        final_cols.append(c)
    if include_json:
        final_cols.append("PM_all_matches_json")
    final_cols += ["match_index", "continued"]
    pm_cols_after = [c for c in merged.columns if c.startswith("PM_")]
    final_cols += pm_cols_after
    final_cols = [c for c in final_cols if c in merged.columns]

    vps_updated_df = merged[final_cols].copy()

    consumed_pm_ids = merged[pm_row_id_col].dropna().astype(int).unique().tolist()
    matched_df = paymeter_df[paymeter_df["_pm_row_id"].isin(consumed_pm_ids)].copy() if len(consumed_pm_ids)>0 else pd.DataFrame(columns=paymeter_df.columns)
    paymeter_remaining_df = paymeter_df[~paymeter_df["_pm_row_id"].isin(consumed_pm_ids)].drop(columns=["_pm_row_id", "_pm_key", "_pm_account"], errors='ignore').copy()

    first_per_vps = merged.groupby("_vps_idx").first().reset_index()
    not_consumed_df = first_per_vps[first_per_vps["matched_count"] == 0].copy()
    not_consumed_df = not_consumed_df[original_vps_cols] if original_vps_cols else not_consumed_df

    stats = {
        "VPS_rows_input": int(len(vps_df)),
        "Paymeter_rows_before_prefilter": int(len(paymeter_df)),
        "Paymeter_rows_after_prefilter": filtered_count,
        "VPS_output_rows": int(len(vps_updated_df)),
        "VPS_rows_with_matches": int((vps_updated_df.get("matched_count", 0) > 0).sum()),
        "Total_matched_paymeter_rows": int(len(matched_df)),
        "Total_paymeter_rows_remaining": int(len(paymeter_remaining_df))
    }

    return vps_updated_df, matched_df, paymeter_remaining_df, not_consumed_df, stats

# CSS (merged from second app for dark mode, adjustable)
def get_css(dark_mode):
    light = """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
    * { font-family: 'Inter', sans-serif; }
    .stApp { background: linear-gradient(135deg, #f8faff 0%, #ffffff 60%); color: #1e293b; }
    .glass-card { background: rgba(255,255,255,0.92); backdrop-filter: blur(12px); border-radius: 16px; padding: 20px; box-shadow: 0 8px 32px rgba(15,30,70,0.08); }
    .header-card {
        background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%);
        color: #ffffff !important;
        backdrop-filter: blur(12px);
        border-radius: 16px;
        padding: 20px;
        box-shadow: 0 8px 32px rgba(15,30,70,0.12);
        border: 1px solid rgba(255,255,255,0.2);
    }
    .metric-card { background: linear-gradient(145deg, #ffffff, #f0f4ff); border-radius: 14px; padding: 16px; box-shadow: 0 6px 20px rgba(15,30,70,0.06); }
    .metric-title { font-weight: 600; color: #64748b; font-size: 0.875rem; text-transform: uppercase; }
    .metric-value { font-size: 1.75rem; font-weight: 800; color: #1e293b; }
    .step { background: #e0e7ff; border-left: 4px solid #6366f1; padding: 12px 16px; border-radius: 0 8px 8px 0; margin: 12px 0; }
    .stButton>button { border-radius: 12px !important; font-weight: 600 !important; }
    section[data-testid="stSidebar"] { background: linear-gradient(180deg, #f8faff 0%, #f1f5ff 100%); }
    </style>
    """
    dark = """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
    * { font-family: 'Inter', sans-serif; }
    .stApp { background: linear-gradient(135deg, #0f172a 0%, #1e293b 60%); color: #f1f5f9; }
    .glass-card { background: rgba(30,41,59,0.9); backdrop-filter: blur(12px); border-radius: 16px; padding: 20px; box-shadow: 0 8px 32px rgba(0,0,0,0.3); border: 1px solid rgba(255,255,255,0.1); }
    .header-card {
        background: linear-gradient(135deg, #5d5fe8 0%, #8b5cf6 100%);
        color: #ffffff !important;
        backdrop-filter: blur(12px);
        border-radius: 16px;
        padding: 20px;
        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
        border: 1px solid rgba(255,255,255,0.2);
    }
    .metric-card { background: linear-gradient(145deg, #1e293b, #334155); border-radius: 14px; padding: 16px; box-shadow: 0 6px 20px rgba(0,0,0,0.2); }
    .metric-title { color: #94a3b8; }
    .metric-value { color: #f1f5f9; }
    .step { background: #1e293b; border-left: 4px solid #8b5cf6; padding: 12px 16px; border-radius: 0 8px 8px 0; margin: 12px 0; color: #e2e8f0; }
    .stButton>button { background: #5d5fe8 !important; color: white !important; }
    section[data-testid="stSidebar"] { background: linear-gradient(180deg, #1e293b 0%, #0f172a 100%); }
    </style>
    """
    return dark if dark_mode else light

# Main UI
st.set_page_config(page_title="Merged Recon App", layout="wide", page_icon="⚡")

if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = False

st.markdown(get_css(st.session_state.dark_mode), unsafe_allow_html=True)

logo_src = f"data:image/png;base64,{base64.b64encode(open(LOGO_PATH, 'rb').read()).decode()}" if LOGO_PATH.exists() else f"data:image/png;base64,{EMBEDDED_LOGO_BASE64.strip()}"

header_html = f"""
<div class="header-card" style="display:flex;align-items:center;gap:20px;">
  <div>{f'<img src="{logo_src}" style="width:80px;height:80px;border-radius:16px;">'}</div>
  <div style="flex:1;">
    <div style="font-size:1.5rem;font-weight:800;">Merged Reconciliation App</div>
    <div style="font-size:0.925rem;opacity:0.9;">EKO-Paymeter • Providus-VPS • VPS-Paymeter</div>
  </div>
  <div style="text-align:right;">
    <div style="background:#10b981;padding:8px 16px;border-radius:12px;color:white;font-weight:700;font-size:0.875rem;">Live</div>
    <div style="margin-top:6px;font-size:0.75rem;opacity:0.8;">Merged v1.0 • {datetime.now().strftime('%b %d')}</div>
  </div>
</div>
"""
components.html(header_html, height=130)

with st.sidebar:
    st.session_state.dark_mode = st.toggle("Dark Mode", value=st.session_state.dark_mode)
    st.markdown(get_css(st.session_state.dark_mode), unsafe_allow_html=True)  # reapply on toggle

# Add full pipeline chaining
st.header("Full Pipeline Chaining")

paymeter_file_full = st.file_uploader("Paymeter Report CSV (shared)", type=["csv"], key="pay_full")
eko_file_full = st.file_uploader("Eko Trans CSV", type=["csv"], key="eko_full")
providus_file_full = st.file_uploader("PROVIDUS file", type=["csv", "xlsx", "xls"], key="providus_full")
vps_file_full = st.file_uploader("VPS file", type=["csv", "xlsx", "xls"], key="vps_full")
district_upload_full = st.file_uploader("district.csv (optional)", type=["csv"], key="district_full")
kcg_upload_full = st.file_uploader("KCG.csv (optional)", type=["csv"], key="kcg_full")
district_info_upload_full = st.file_uploader("district_acct_number.csv (optional)", type=["csv"], key="distinfo_full")

preview_limit_full = st.slider("Preview repaired rows", 1, 20, 8, key="preview_full")

# For Providus-VPS opts
PRV_COL_DATE_full = st.text_input("PROVIDUS Date", value="Transaction Date", key="prv_date_full")
PRV_COL_CREDIT_full = st.text_input("PROVIDUS Credit", value="Credit Amount", key="prv_credit_full")
PRV_NARRATION_COL_full = st.text_input("PROVIDUS Narration", value="Transaction Details", key="prv_narr_full")
PRV_COL_DEBIT_full = st.text_input("PROVIDUS Debit (drop)", value="Debit Amount", key="prv_debit_full")
VPS_COL_DATE_full = st.text_input("VPS Date", value="created_at", key="vps_date_full")
VPS_COL_SETTLED_full = st.text_input("VPS Settled", value="settled_amount_minor", key="vps_settled_full")
VPS_COL_CHARGE_full = st.text_input("VPS Charge", value="charge_amount_minor", key="vps_charge_full")
date_tolerance_days_full = st.slider("Date tolerance (± days)", 0, 7, 3, key="date_tol_full")
enable_amount_only_fallback_full = st.checkbox("Amount-only fallback", value=False, key="amt_fallback_full")
enable_ref_matching_full = st.checkbox("Reference token matching", value=True, key="ref_match_full")

# For VPS-Paymeter opts
prefix_len_full = st.number_input("Chars from vps_settlement_ref", 1, 64, 15, key="prefix_full")
rrn_column_input_full = st.text_input("Paymeter RRN col", "RRN", key="rrn_full")
vps_ref_column_input_full = st.text_input("VPS ref col", "vps_settlement_ref", key="vps_ref_full")
vps_virtual_acct_col_input_full = st.text_input("VPS virtual acct col", "vps_virtual_acct_no", key="vps_virt_full")
paymeter_account_col_input_full = st.text_input("Paymeter account col", "Account Number", key="pm_acct_full")
pm_cols_chosen_full = st.multiselect("Paymeter columns to include", options=["RRN", "Account Number", "Transaction Amount", "Transaction ID", "Customer Name", "Input Amount","Meter Number","Phone Number","Status","Reference","Created At"], default=["RRN", "Account Number", "Transaction Amount", "Transaction ID", "Customer Name"], key="pm_cols_full")
include_json_full = st.checkbox("Include PM_all_matches_json (slower)", value=False, key="json_full")
clean_debit_col_full = st.checkbox("Clear 'Debit Amount' in VPS", value=True, key="clean_debit_full")
drop_empty_rows_full = st.checkbox("Drop empty VPS rows", value=True, key="drop_empty_full")

date_range_full = st.date_input("Select Report Date Range", value=(date.today(), date.today()), key="date_range_full")

run_full = st.button("Run Full Pipeline")

def _date_floor(s):
    return pd.to_datetime(s).dt.floor('D')

if run_full:
    if not all([paymeter_file_full, eko_file_full, providus_file_full, vps_file_full]):
        st.error("Upload all required files.")
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            
            # Step 1: EKO vs Paymeter
            paymeter_path = work_dir / "paymeter_full.csv"
            eko_path = work_dir / "eko_full.csv"
            paymeter_path.write_bytes(paymeter_file_full.getvalue())
            eko_path.write_bytes(eko_file_full.getvalue())
            
            district_path = work_dir / "district_full.csv" if district_upload_full else (DEFAULT_DISTRICT if DEFAULT_DISTRICT.exists() else None)
            if district_upload_full:
                district_path.write_bytes(district_upload_full.getvalue())
            kcg_path = work_dir / "kcg_full.csv" if kcg_upload_full else (DEFAULT_KCG if DEFAULT_KCG.exists() else None)
            if kcg_upload_full:
                kcg_path.write_bytes(kcg_upload_full.getvalue())
            district_info_path = work_dir / "distinfo_full.csv" if district_info_upload_full else (DEFAULT_DISTRICT_INFO if DEFAULT_DISTRICT_INFO.exists() else None)
            if district_info_upload_full:
                district_info_path.write_bytes(district_info_upload_full.getvalue())
            
            cleaned = work_dir / "cleaned_full.csv"
            fixed_count, examples = repair_address_spill(str(paymeter_path), str(cleaned), preview_limit=preview_limit_full)
            
            bydistrict = work_dir / "bydistrict_full.csv"
            merge_districts(str(cleaned), str(district_path) if district_path else None, str(bydistrict))
            
            trans_df = read_file_any(None, str(bydistrict))
            eko_df = read_file_any(None, str(eko_path))
            
            if 'Created At' in trans_df.columns:
                trans_df['Created At'] = pd.to_datetime(trans_df['Created At'], errors='coerce')
            if 'Transaction Date' in eko_df.columns:
                eko_df['Transaction Date'] = pd.to_datetime(eko_df['Transaction Date'], errors='coerce')
            
            start_date, end_date = date_range_full
            if start_date and end_date:
                start_ts = pd.Timestamp(start_date)
                end_ts = pd.Timestamp(end_date)
                if 'Created At' in trans_df.columns:
                    trans_mask = (_date_floor(trans_df['Created At']) >= start_ts) & (_date_floor(trans_df['Created At']) <= end_ts)
                    trans_df = trans_df[trans_mask]
                if 'Transaction Date' in eko_df.columns:
                    eko_mask = (_date_floor(eko_df['Transaction Date']) >= start_ts) & (_date_floor(eko_df['Transaction Date']) <= end_ts)
                    eko_df = eko_df[eko_mask]
            
            filtered_trans = work_dir / "filtered_trans_full.csv"
            filtered_eko = work_dir / "filtered_eko_full.csv"
            trans_df.to_csv(filtered_trans, index=False)
            eko_df.to_csv(filtered_eko, index=False)
            
            out_detail1 = work_dir / "detail1.csv"
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            out_excel1 = work_dir / f"EKO_Paymeter_{timestamp}.xlsx"
            result1 = merge_and_analyze(
                str(filtered_eko), str(filtered_trans),
                str(district_info_path) if district_info_path else None,
                str(kcg_path) if kcg_path else None,
                str(out_detail1), str(out_excel1)
            )
            
            # Step 2: Providus vs VPS
            providus_path = work_dir / "providus_full." + providus_file_full.name.split('.')[-1]
            vps_path = work_dir / "vps_full." + vps_file_full.name.split('.')[-1]
            providus_path.write_bytes(providus_file_full.getvalue())
            vps_path.write_bytes(vps_file_full.getvalue())
            
            prv_df = read_file_any(None, str(providus_path))
            vps_df = read_file_any(None, str(vps_path))
            
            opts2 = {
                "PRV_COL_DATE": PRV_COL_DATE_full,
                "PRV_COL_CREDIT": PRV_COL_CREDIT_full,
                "PRV_NARRATION_COL": PRV_NARRATION_COL_full,
                "PRV_COL_DEBIT": PRV_COL_DEBIT_full,
                "VPS_COL_DATE": VPS_COL_DATE_full,
                "VPS_COL_SETTLED": VPS_COL_SETTLED_full,
                "VPS_COL_CHARGE": VPS_COL_CHARGE_full,
                "ref_matching": enable_ref_matching_full,
                "plus_minus_N_days": date_tolerance_days_full > 0,
                "amount_only_fallback": enable_amount_only_fallback_full
            }
            
            out_prv, vps_unmatched, excel_buf2, csv_bufs, stats2, vps = run_vps_recon_enhanced(
                prv_df, vps_df, opts2, date_tolerance_days_full
            )
            
            out_excel2 = work_dir / f"Providus_VPS_{timestamp}.xlsx"
            with open(out_excel2, "wb") as f:
                f.write(excel_buf2.getvalue())
            
            # Step 3: VPS vs Paymeter
            vps_df_step3 = read_file_any(io.BytesIO(excel_buf2.getvalue()), sheet_name="Cleaned_PROVIDUS")
            paymeter_df_step3 = read_paymeter_bytes(io.BytesIO(paymeter_file_full.getvalue()))
            
            opts3 = {
                "prefix_len": prefix_len_full,
                "rrn_column_input": rrn_column_input_full,
                "vps_ref_column_input": vps_ref_column_input_full,
                "vps_virtual_acct_col_input": vps_virtual_acct_col_input_full,
                "paymeter_account_col_input": paymeter_account_col_input_full,
                "pm_cols_chosen": pm_cols_chosen_full,
                "include_json": include_json_full,
                "clean_debit_col": clean_debit_col_full,
                "drop_empty_rows": drop_empty_rows_full
            }
            
            vps_updated_df, matched_df, paymeter_remaining_df, not_consumed_df, stats3 = run_vps_paymeter_recon(vps_df_step3, paymeter_df_step3, opts3)
            
            out_excel3 = io.BytesIO()
            with pd.ExcelWriter(out_excel3, engine="openpyxl") as writer:
                vps_updated_df.to_excel(writer, sheet_name="Cleaned_Providus_updated", index=False)
                matched_df.to_excel(writer, sheet_name="Matched_From_Paymeter", index=False)
                paymeter_remaining_df.to_excel(writer, sheet_name="Paymeter_Remaining", index=False)
                not_consumed_df.to_excel(writer, sheet_name="Not_Consumed", index=False)
            out_excel3.seek(0)
            out_filename3 = f"VPS_Paymeter_{timestamp}.xlsx"
            
            # Downloads
            st.success("Full pipeline complete.")
            with open(out_excel1, "rb") as f:
                st.download_button("Download EKO-Paymeter", f, out_excel1.name)
            with open(out_excel2, "rb") as f:
                st.download_button("Download Providus-VPS", f, out_excel2.name)
            st.download_button("Download VPS-Paymeter", out_excel3.getvalue(), out_filename3)

tab1, tab2, tab3 = st.tabs(["EKO vs Paymeter", "Providus vs VPS", "VPS vs Paymeter"])

with tab1:
    st.header("EKO vs Paymeter Reconciliation")

    paymeter_file = st.file_uploader("Paymeter Report CSV", type=["csv"], key="paymeter1")
    eko_file = st.file_uploader("Eko Trans CSV", type=["csv"], key="eko")
    district_upload = st.file_uploader("district.csv", type=["csv"], key="district")
    kcg_upload = st.file_uploader("KCG.csv", type=["csv"], key="kcg")
    district_info_upload = st.file_uploader("district_acct_number.csv", type=["csv"], key="distinfo")
    preview_limit = st.slider("Preview repaired rows", 1, 20, 8)
    check_dates = st.button("Check Date Ranges", key="check_dates1")
    date_range = st.date_input("Select Report Date Range", value=(date.today(), date.today()), key="date_range1")
    run1 = st.button("Generate Report", key="run1")

    if "dates_checked1" not in st.session_state:
        st.session_state.dates_checked1 = False
    if check_dates:
        if not paymeter_file or not eko_file:
            st.error("Upload both files.")
        else:
            with tempfile.TemporaryDirectory() as tmp:
                pay_path = Path(tmp) / "pay.csv"
                eko_path = Path(tmp) / "eko.csv"
                pay_path.write_bytes(paymeter_file.getvalue())
                eko_path.write_bytes(eko_file.getvalue())
                pay_df = read_file_any(None, str(pay_path))
                eko_df = read_file_any(None, str(eko_path))
                pay_dates = pd.to_datetime(pay_df.get('Created At', pd.Series()), errors='coerce').dropna()
                eko_dates = pd.to_datetime(eko_df.get('Transaction Date', pd.Series()), errors='coerce').dropna()
                st.session_state.pay_min1 = pay_dates.min().date() if not pay_dates.empty else None
                st.session_state.pay_max1 = pay_dates.max().date() if not pay_dates.empty else None
                st.session_state.eko_min1 = eko_dates.min().date() if not eko_dates.empty else None
                st.session_state.eko_max1 = eko_dates.max().date() if not eko_dates.empty else None
                st.session_state.dates_checked1 = True

    if run1:
        if not st.session_state.dates_checked1:
            st.error("Check dates first.")
        elif not paymeter_file or not eko_file:
            st.error("Upload both files.")
        else:
            work_dir = Path(tempfile.mkdtemp())
            paymeter_path = work_dir / "paymeter.csv"
            eko_path = work_dir / "eko.csv"
            paymeter_path.write_bytes(paymeter_file.getvalue())
            eko_path.write_bytes(eko_file.getvalue())

            district_path = DEFAULT_DISTRICT if not district_upload else work_dir / "district.csv"
            if district_upload:
                district_path.write_bytes(district_upload.getvalue())
            kcg_path = DEFAULT_KCG if not kcg_upload else work_dir / "kcg.csv"
            if kcg_upload:
                kcg_path.write_bytes(kcg_upload.getvalue())
            district_info_path = DEFAULT_DISTRICT_INFO if not district_info_upload else work_dir / "distinfo.csv"
            if district_info_upload:
                district_info_path.write_bytes(district_info_upload.getvalue())

            cleaned = work_dir / "cleaned.csv"
            fixed_count, examples = repair_address_spill(str(paymeter_path), str(cleaned), preview_limit=preview_limit)

            bydistrict = work_dir / "bydistrict.csv"
            merge_districts(str(cleaned), str(district_path) if district_path else None, str(bydistrict))

            trans_df = read_file_any(None, str(bydistrict))
            eko_df = read_file_any(None, str(eko_path))

            if 'Created At' in trans_df.columns:
                trans_df['Created At'] = pd.to_datetime(trans_df['Created At'], errors='coerce')
            if 'Transaction Date' in eko_df.columns:
                eko_df['Transaction Date'] = pd.to_datetime(eko_df['Transaction Date'], errors='coerce')

            start_date, end_date = date_range
            if start_date and end_date:
                start_ts = pd.Timestamp(start_date)
                end_ts = pd.Timestamp(end_date)
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

            out_detail = work_dir / "detail.csv"
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            out_excel = work_dir / f"PaymeterReport_{timestamp}.xlsx"
            result = merge_and_analyze(
                str(filtered_eko), str(filtered_trans),
                str(district_info_path) if district_info_path else None,
                str(kcg_path) if kcg_path else None,
                str(out_detail), str(out_excel)
            )

            st.session_state.eko_paymeter_output = result["out_excel"]

            st.success("EKO vs Paymeter complete.")
            with open(out_excel, "rb") as f:
                st.download_button("Download EKO-Paymeter Excel", f, out_excel.name)

with tab2:
    st.header("Providus vs VPS Reconciliation")

    providus_file = st.file_uploader("PROVIDUS file", type=["csv", "xlsx", "xls"], key="providus")
    vps_file = st.file_uploader("VPS file", type=["csv", "xlsx", "xls"], key="vps")
    PRV_COL_DATE = st.text_input("PROVIDUS Date", value="Transaction Date", key="prv_date")
    PRV_COL_CREDIT = st.text_input("PROVIDUS Credit", value="Credit Amount", key="prv_credit")
    PRV_NARRATION_COL = st.text_input("PROVIDUS Narration", value="Transaction Details", key="prv_narr")
    PRV_COL_DEBIT = st.text_input("PROVIDUS Debit (drop)", value="Debit Amount", key="prv_debit")
    VPS_COL_DATE = st.text_input("VPS Date", value="created_at", key="vps_date")
    VPS_COL_SETTLED = st.text_input("VPS Settled", value="settled_amount_minor", key="vps_settled")
    VPS_COL_CHARGE = st.text_input("VPS Charge", value="charge_amount_minor", key="vps_charge")
    date_tolerance_days = st.slider("Date tolerance (± days)", 0, 7, 3)
    enable_amount_only_fallback = st.checkbox("Amount-only fallback", value=False)
    enable_ref_matching = st.checkbox("Reference token matching", value=True)
    run2 = st.button("Run Providus-VPS", key="run2")

    if run2:
        prv_df = read_file_any(providus_file)
        vps_df = read_file_any(vps_file)
        if prv_df is None or vps_df is None:
            st.error("Upload both files.")
        else:
            opts = {
                "PRV_COL_DATE": PRV_COL_DATE,
                "PRV_COL_CREDIT": PRV_COL_CREDIT,
                "PRV_NARRATION_COL": PRV_NARRATION_COL,
                "PRV_COL_DEBIT": PRV_COL_DEBIT,
                "VPS_COL_DATE": VPS_COL_DATE,
                "VPS_COL_SETTLED": VPS_COL_SETTLED,
                "VPS_COL_CHARGE": VPS_COL_CHARGE,
                "ref_matching": enable_ref_matching,
                "plus_minus_N_days": date_tolerance_days > 0,
                "amount_only_fallback": enable_amount_only_fallback
            }

            progress_text = st.empty()
            progress_bar = st.progress(0)
            def update_progress(cur, total):
                p = cur / total
                progress_text.text(f"Matching... {int(p*100)}% ({cur}/{total})")
                progress_bar.progress(p)

            out_prv, vps_unmatched, excel_buf, csv_bufs, stats, vps = run_vps_recon_enhanced(
                prv_df, vps_df, opts, date_tolerance_days, update_progress
            )

            progress_text.empty()
            progress_bar.empty()

            st.session_state.providus_vps_output = excel_buf.getvalue()

            st.success("Providus vs VPS complete.")
            st.download_button("Download Providus-VPS Excel", excel_buf, f"Recon_{datetime.now():%Y%m%d_%H%M%S}.xlsx")

with tab3:
    st.header("VPS vs Paymeter Reconciliation")

    use_previous = st.checkbox("Use output from Providus vs VPS tab (if run)", value=False)
    if use_previous and "providus_vps_output" in st.session_state:
        vps_file = io.BytesIO(st.session_state.providus_vps_output)
    else:
        vps_file = st.file_uploader("Upload VPS Excel (or Providus-VPS output)", type=["xlsx", "xls"], key="vps3")
    paymeter_file = st.file_uploader("Upload Paymeter file", type=["csv", "xlsx", "xls"], key="paymeter3")
    prefix_len = st.number_input("Chars from vps_settlement_ref", 1, 64, 15)
    rrn_column_input = st.text_input("Paymeter RRN col", "RRN")
    vps_ref_column_input = st.text_input("VPS ref col", "vps_settlement_ref")
    vps_virtual_acct_col_input = st.text_input("VPS virtual acct col", "vps_virtual_acct_no")
    paymeter_account_col_input = st.text_input("Paymeter account col", "Account Number")
    pm_cols_chosen = st.multiselect("Paymeter columns to include", options=["RRN", "Account Number", "Transaction Amount", "Transaction ID", "Customer Name", "Input Amount","Meter Number","Phone Number","Status","Reference","Created At"], default=["RRN", "Account Number", "Transaction Amount", "Transaction ID", "Customer Name"])
    include_json = st.checkbox("Include PM_all_matches_json (slower)", value=False)
    clean_debit_col = st.checkbox("Clear 'Debit Amount' in VPS", value=True)
    drop_empty_rows = st.checkbox("Drop empty VPS rows", value=True)
    run3 = st.button("Run VPS-Paymeter", key="run3")

    if run3:
        vps_df = read_vps_bytes(vps_file)
        paymeter_df = read_paymeter_bytes(paymeter_file)
        if vps_df is None or paymeter_df is None:
            st.error("Upload both files.")
        else:
            opts = {
                "prefix_len": prefix_len,
                "rrn_column_input": rrn_column_input,
                "vps_ref_column_input": vps_ref_column_input,
                "vps_virtual_acct_col_input": vps_virtual_acct_col_input,
                "paymeter_account_col_input": paymeter_account_col_input,
                "pm_cols_chosen": pm_cols_chosen,
                "include_json": include_json,
                "clean_debit_col": clean_debit_col,
                "drop_empty_rows": drop_empty_rows
            }

            vps_updated_df, matched_df, paymeter_remaining_df, not_consumed_df, stats = run_vps_paymeter_recon(vps_df, paymeter_df, opts)

            output = io.BytesIO()
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            out_filename = f"VPS_Paymeter_Reconciled_{ts}.xlsx"
            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                vps_updated_df.to_excel(writer, sheet_name="Cleaned_Providus_updated", index=False)
                matched_df.to_excel(writer, sheet_name="Matched_From_Paymeter", index=False)
                paymeter_remaining_df.to_excel(writer, sheet_name="Paymeter_Remaining", index=False)
                not_consumed_df.to_excel(writer, sheet_name="Not_Consumed", index=False)
            output.seek(0)

            st.success("VPS vs Paymeter complete.")
            st.download_button("Download VPS-Paymeter Excel", output, out_filename)

st.caption("Merged Recon App | Full Pipeline Chaining Added")
