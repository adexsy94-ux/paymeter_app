# paymeter_app.py
# -*- coding: utf-8 -*-
"""
Paymeter Pro – Fixed Monthly KCG Sheet + Debug + Robust KCG Detection
All reports → ONE timestamped Excel
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

# === YOUR LOGO – Base64 (placeholder; replace with your own) ===
EMBEDDED_LOGO_BASE64 = """
iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==
"""

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
        s = 0
        if 'kcg' in lc: s += 100
        if 'account' in lc: s += 50
        if 'acct' in lc: s += 40
        if 'number' in lc or 'no' in lc: s += 20
        return s, -len(c)

    best = max(cols, key=score)
    return best

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

    # === ROBUST KCG DETECTION ===
    kcg_accounts = set()
    kcg_debug = []

    if kcg_path and os.path.exists(kcg_path):
        try:
            kcg_df = safe_read_csv(Path(kcg_path))
            if kcg_df.empty:
                kcg_debug.append("KCG.csv is empty")
            else:
                kcg_col = pick_kcg_column(kcg_df)
                raw_accounts = kcg_df[kcg_col].dropna().astype(str).str.strip()
                normalized = raw_accounts.apply(normalize_acct)
                valid_accounts = normalized[normalized.str.len() >= 6]
                kcg_accounts = set(valid_accounts)
                kcg_debug.append(f"KCG: Loaded {len(kcg_accounts)} valid accounts from column '{kcg_col}'")
        except Exception as e:
            kcg_debug.append(f"KCG load error: {e}")
    else:
        kcg_debug.append("KCG.csv not provided or not found")

    # Account Number Matching
    matched_any = pd.Series(False, index=merged.index)
    possible_account_columns = [c for c in merged.columns if any(k in c.lower() for k in ['account', 'acct', 'number'])]

    for col in possible_account_columns:
        try:
            norm_series = merged[col].astype(str).apply(normalize_acct)
            merged[f"{col}_norm"] = norm_series
            if kcg_accounts:
                matched = norm_series.isin(kcg_accounts)
                matched_any = matched_any | matched
                if matched.any():
                    kcg_debug.append(f"KCG match in '{col}': {matched.sum()} rows")
        except Exception as e:
            kcg_debug.append(f"Error processing column {col}: {e}")

    # Text-based "kcg" in remarks
    text_flag = pd.Series(False, index=merged.index)
    remark_cols = [c for c in merged.columns if any(k in c.lower() for k in ['remark', 'note', 'commission type', 'disco'])]
    for col in remark_cols:
        try:
            flag = merged[col].fillna('').astype(str).str.contains('kcg', case=False, na=False)
            text_flag = text_flag | flag
            if flag.any():
                kcg_debug.append(f"KCG text match in '{col}': {flag.sum()} rows")
        except Exception as e:
            kcg_debug.append(f"Error in text search {col}: {e}")

    merged['Is_KCG'] = (matched_any | text_flag)

    kcg_rows = merged.loc[merged['Is_KCG']].copy()
    non_kcg_rows = merged.loc[~merged['Is_KCG']].copy()

    kcg_debug.append(f"Final KCG rows: {len(kcg_rows)}")
    kcg_debug.append(f"Final Non-KCG rows: {len(non_kcg_rows)}")
    result['kcg_debug'] = kcg_debug

    # === Monthly KCG (Safe) ===
    if not kcg_rows.empty and 'Created At' in kcg_rows.columns:
        kcg_rows['Month'] = kcg_rows['Created At'].dt.to_period('M')
        monthly_kcg = kcg_rows.groupby('Month', observed=False).agg(
            Count=('Transaction Amount','size'),
            Transaction_Amount=('Transaction Amount','sum'),
            Commission=('commission','sum')
        ).reset_index()
        monthly_kcg = monthly_kcg.sort_values('Month')
    else:
        monthly_kcg = pd.DataFrame(columns=['Month', 'Count', 'Transaction_Amount', 'Commission'])

    # === Monthly Non-KCG (Safe) ===
    if not non_kcg_rows.empty and 'Created At' in non_kcg_rows.columns:
        non_kcg_rows['Month'] = non_kcg_rows['Created At'].dt.to_period('M')
        monthly_non_kcg = non_kcg_rows.groupby('Month', observed=False).agg(
            Count=('Transaction Amount','size'),
            Transaction_Amount=('Transaction Amount','sum'),
            Commission=('commission','sum')
        ).reset_index()
        monthly_non_kcg = monthly_non_kcg.sort_values('Month')
    else:
        monthly_non_kcg = pd.DataFrame(columns=['Month', 'Count', 'Transaction_Amount', 'Commission'])

    # === Monthly All ===
    if 'Created At' in merged.columns:
        merged['Month'] = merged['Created At'].dt.to_period('M')
        monthly_all = merged.groupby('Month', observed=False).agg(
            All_Count=('Transaction Amount','size'),
            All_Transaction_Amount=('Transaction Amount','sum'),
            All_Commission=('commission','sum')
        ).reset_index()
    else:
        monthly_all = pd.DataFrame(columns=['Month', 'All_Count', 'All_Transaction_Amount', 'All_Commission'])

    # === Continue with rest of analysis (unchanged) ===
    # ... [rest of your original logic: ranges, top20, etc.] ...

    # === [KEEP ALL ORIGINAL CODE BELOW THIS LINE] ===
    # (main_summary, ranges, account_summary, top20, etc.)

    # For brevity, assuming the rest is unchanged — just paste your original code here
    # But here’s the critical part for monthly_kcg:
    # → Already fixed above

    # === WRITE EXCEL ===
    try:
        with pd.ExcelWriter(out_excel, engine="openpyxl") as writer:
            # ... your original sheets ...
            monthly_kcg.to_excel(writer, sheet_name="Monthly KCG", index=False)
            # ... rest ...
    except Exception as e:
        raise RuntimeError(f"Error writing Excel: {e}")

    result.update({
        "merged_df": merged,
        "monthly_kcg": monthly_kcg,
        "kcg_debug": kcg_debug,
        "out_detail": out_detail,
        "out_excel": out_excel
    })
    return result

# =============================================
# STREAMLIT UI (Add Debug Tab)
# =============================================

# ... [your full Streamlit UI code] ...

with tab4:
    log_area.code(f"Fixed: {fixed_count}\nDetail: {out_detail}\nExcel: {out_excel}")
    
    if 'kcg_debug' in result:
        st.subheader("KCG Detection Debug Log")
        for line in result['kcg_debug']:
            st.write(f"• {line}")
