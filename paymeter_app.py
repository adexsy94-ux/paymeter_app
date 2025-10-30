# paymeter_app.py
# -*- coding: utf-8 -*-
"""
Paymeter + Eko Streamlit app (consolidated, Excel sheets updated)
- repairs address spills in paymeter_report.csv
- merges district lookup
- merges with Eko Trans.csv, normalizes amounts
- robust KCG detection and reporting
- exports CSV and Excel workbook (with user-requested sheets)
"""

import csv
import re
import os
import shutil
import tempfile
from pathlib import Path
from typing import List, Optional, Tuple, Dict

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

def coerce_amount_column(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        return
    df[col] = (
        df[col].astype(str)
        .str.replace(r"[,\s₦$]", "", regex=True)
        .replace({"nan": None, "None": None})
    )
    df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

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
        raise ValueError("Could not find Address column. Add your Address column name to address_candidates.")
    if txn_idx is None:
        raise ValueError("Could not find Transaction Amount column. Add your amount column name to txn_amt_candidates.")

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

            # If transaction cell already numeric -> row ok
            if txn_idx < len(row) and is_amount(row[txn_idx]):
                if len(row) > expected_cols:
                    row = row[:expected_cols]
                elif len(row) < expected_cols:
                    row = row + [""] * (expected_cols - len(row))
                writer.writerow(row)
                continue

            # collect spill fragments until numeric found
            j = txn_idx
            spill = []
            while j < len(row) and not is_amount(row[j]):
                spill.append(row[j])
                j += 1

            # if nothing to do -> align and write
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

            # remove spilled columns so subsequent numeric values shift left
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
# Step C: Merge Eko & Analyze (robust KCG detection + requested Excel sheets)
# -----------------------------
def merge_and_analyze(
    eko_path: str,
    trans_path: str,
    district_info_path: Optional[str],
    kcg_path: Optional[str],
    out_detail: str,
    out_summary: str,
    out_excel: str
) -> None:
    # load inputs
    eko = pd.read_csv(eko_path, dtype=str, keep_default_na=False)
    trans = pd.read_csv(trans_path, dtype=str, keep_default_na=False)

    # create 'ref' keys
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

    # District column best pick
    src_col = None
    for candidate in ['District Name', 'DISTRICT BY ADDRESS', 'District', 'district', 'DISTRICT']:
        if candidate in merged.columns and merged[candidate].notna().any():
            src_col = candidate
            break
    merged['District'] = merged[src_col].astype(str).replace({'nan': None}).fillna('empty').astype(str).str.strip() if src_col else 'empty'

    # pick amount columns (prefer *_trans)
    def pick_amount(col_list: List[str]) -> Optional[str]:
        for c in col_list:
            if c in merged.columns:
                # if column exists and has at least one non-empty numeric-like value, pick it
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

    # calculations
    merged['fig_dif'] = merged['Total Amount'] - merged['Transaction Amount']
    merged['amt_less_vat'] = merged['Transaction Amount'] / 1.075
    merged['commission'] = merged['amt_less_vat'].apply(calculate_commission)

    # build KCG set
    kcg_accounts = set()
    if kcg_path and os.path.exists(kcg_path):
        try:
            kcg_df = pd.read_csv(kcg_path, dtype=str, keep_default_na=False)
            kcg_col = pick_kcg_column(kcg_df)
            kcg_accounts = set(kcg_df[kcg_col].astype(str).apply(normalize_acct))
        except Exception:
            kcg_accounts = set()

    # find plausible account columns in merged
    possible_account_columns = []
    for c in merged.columns:
        lc = c.lower()
        if 'account' in lc or 'acct' in lc or 'accountnumber' in lc.replace(' ', ''):
            possible_account_columns.append(c)
    # add common names if present
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

    # textual flags
    text_flag = pd.Series(False, index=merged.index)
    for flag_col in ("Disco Commission Type", "DiscoCommissionType", "Commission Type", "CommissionType", "Remarks", "Note"):
        if flag_col in merged.columns:
            text_flag = text_flag | merged[flag_col].fillna('').astype(str).str.contains('kcg', case=False, na=False)

    merged['Is_KCG'] = (matched_any | text_flag).fillna(False)

    # Prepare the summaries / sheets required by the user

    # All accounts / split
    kcg_rows = merged.loc[merged['Is_KCG']].copy()
    non_kcg_rows = merged.loc[~merged['Is_KCG']].copy()

    # Main summary (All / KCG / Non-KCG)
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

    # Non-KCG Ranges
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

    # All Accounts Summary (per account)
    account_col_candidates = [c for c in merged.columns if 'account' in c.lower()]
    acct_col = account_col_candidates[0] if account_col_candidates else 'Account Number_trans'
    if acct_col not in merged.columns:
        merged[acct_col] = merged.get('Account Number_trans', merged.index.astype(str))
    merged[acct_col] = merged[acct_col].astype(str).apply(normalize_acct)
    # Customer name candidate
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

    # Top 20 Accounts (by Transaction_Count then Total_Amount)
    top20 = account_summary.sort_values(by=['Transaction_Count','Total_Amount'], ascending=[False, False]).head(20)

    # scenario No KCG
    scenario_no_kcg = pd.DataFrame([{
        "Category": "Scenario: Non-KCG Only",
        "Count": len(non_kcg_rows),
        "Transaction Amount": non_kcg_rows['Transaction Amount'].sum(),
        "Commission": non_kcg_rows['commission'].sum()
    }])

    # Monthly Non-KCG / KCG / All
    # Ensure Created At column exists and is datetime
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

    monthly_trends_combined = monthly_all.merge(monthly_kcg, on='Month', how='outer', suffixes=('', '_KCG'))
    monthly_trends_combined = monthly_trends_combined.merge(monthly_non_kcg, on='Month', how='outer', suffixes=('', '_NonKCG')).fillna(0)

    # Projections (average of last up to 3 months)
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
                "Projected_Count": round(recent['Count'].mean(), 0),
                "Projected_Amount": round(recent['Transaction_Amount'].mean(), 2),
                "Projected_Commission": round(recent['Commission'].mean(), 2),
                "Basis": f"Average of last {window} month(s)"
            }])
        return proj

    projection_non_kcg = build_projection(monthly_non_kcg)
    projection_kcg = build_projection(monthly_kcg)

    # Save detailed CSVs
    merged.to_csv(out_detail, index=False)
    report = pd.DataFrame(merged['District'].unique(), columns=['District'])
    district_trans_totals = merged.groupby('District', dropna=False)['Transaction Amount'].sum().reset_index().rename(columns={'Transaction Amount': 'paymeter_total'})
    district_eko_totals = merged.groupby('District', dropna=False)['Total Amount'].sum().reset_index().rename(columns={'Total Amount': 'eko_total'})
    district_commission = merged.groupby('District', dropna=False)['commission'].sum().reset_index().rename(columns={'commission': 'district_commission'})
    report = report.merge(district_trans_totals, on='District', how='left').merge(district_eko_totals, on='District', how='left').merge(district_commission, on='District', how='left')
    for c in ['paymeter_total','eko_total','district_commission']:
        if c in report.columns:
            report[c] = report[c].fillna(0.0)
    report['difference'] = report['eko_total'] - report['paymeter_total']
    report.to_csv(out_summary, index=False)

    # Excel workbook with exact sheets requested
    try:
        with pd.ExcelWriter(out_excel, engine="openpyxl") as writer:
            # 1. Main Summary
            main_summary.to_excel(writer, sheet_name="Main Summary", index=False)

            # 2. Non-KCG Ranges
            non_kcg_ranges.to_excel(writer, sheet_name="Non-KCG Ranges", index=False)

            # 3. All Accounts Summary (per-account)
            account_summary.to_excel(writer, sheet_name="All Accounts Summary", index=False)

            # 4. Top 20 Accounts
            top20.to_excel(writer, sheet_name="Top 20 Accounts", index=False)

            # 5. scenario No KCG
            scenario_no_kcg.to_excel(writer, sheet_name="scenario No KCG", index=False)

            # 6. Monthly Non-KCG
            monthly_non_kcg.to_excel(writer, sheet_name="Monthly Non-KCG", index=False)

            # 7. Monthly KCG
            monthly_kcg.to_excel(writer, sheet_name="Monthly KCG", index=False)

            # 8. Monthly Trends (All)
            monthly_trends_combined.to_excel(writer, sheet_name="Monthly Trends (All)", index=False)

            # 9. Projection Non-KCG
            if not projection_non_kcg.empty:
                projection_non_kcg.to_excel(writer, sheet_name="Projection Non-KCG", index=False)
            else:
                # write an empty table with columns if empty
                pd.DataFrame(columns=["Month","Projected_Count","Projected_Amount","Projected_Commission","Basis"]).to_excel(writer, sheet_name="Projection Non-KCG", index=False)

            # 10. Projection KCG
            if not projection_kcg.empty:
                projection_kcg.to_excel(writer, sheet_name="Projection KCG", index=False)
            else:
                pd.DataFrame(columns=["Month","Projected_Count","Projected_Amount","Projected_Commission","Basis"]).to_excel(writer, sheet_name="Projection KCG", index=False)
    except Exception as e:
        raise RuntimeError(f"Error writing excel output {out_excel}: {e}")

    # optional enrich report with district_info if provided
    if district_info_path and os.path.exists(district_info_path):
        try:
            district_info = pd.read_csv(district_info_path, dtype=str, keep_default_na=False)
            if 'district' in district_info.columns:
                district_info = district_info.rename(columns={'district': 'District'})
            report = report.merge(district_info, on='District', how='left')
            report.to_csv(out_summary, index=False)
        except Exception:
            pass

    # audit empty district rows
    try:
        empty_rows = merged[merged['District'].str.lower() == 'empty']
        if not empty_rows.empty:
            empty_rows.to_csv(BASE_DIR / "Audit_empty_district_rows.csv", index=False)
    except Exception:
        pass

# -----------------------------
# Streamlit UI
# -----------------------------
st.set_page_config(page_title="Paymeter Processor", layout="wide")
st.title("Paymeter + Eko Processor")

st.markdown("""
Upload the **raw paymeter_report.csv** and **Eko Trans.csv** (required).
Optional default reference files (`district.csv`, `KCG.csv`, `district_acct_number.csv`) can be placed in a `./data/` folder.
You can upload updated versions to override for this run.
""")

# required uploads
paymeter_file = st.file_uploader("Upload raw paymeter_report.csv (required)", type=["csv"])
eko_file = st.file_uploader("Upload Eko Trans.csv (required)", type=["csv"])

st.write("---")
st.header("Preloaded reference files (editable)")

def show_default(name: str, path: Path):
    if path.exists():
        st.success(f"Default {name} loaded from `data/{path.name}`")
    else:
        st.info(f"No default {name} found in ./data/ (optional)")

show_default("district.csv", DEFAULT_DISTRICT)
district_upload = st.file_uploader("Upload NEW district.csv to override (optional)", type=["csv"], key="district")

show_default("KCG.csv", DEFAULT_KCG)
kcg_upload = st.file_uploader("Upload NEW KCG.csv to override (optional)", type=["csv"], key="kcg")

show_default("district_acct_number.csv", DEFAULT_DISTRICT_INFO)
district_info_upload = st.file_uploader("Upload NEW district_acct_number.csv to override (optional)", type=["csv"], key="district_info")

st.write("---")
preview_limit = st.number_input("Preview repaired rows (max examples)", min_value=1, max_value=50, value=8)
run = st.button("Run pipeline")

# helper to save uploaded file object to disk
def save_uploaded_file(uploaded, dest: Path):
    with open(dest, "wb") as f:
        f.write(uploaded.getbuffer())

if run:
    if not paymeter_file or not eko_file:
        st.error("Please upload both paymeter_report.csv and Eko Trans.csv to run the pipeline.")
    else:
        work_dir = Path(tempfile.mkdtemp(prefix="paymeter_ui_"))
        st.info(f"Working directory (temporary): {work_dir}")

        try:
            # Save required uploads
            paymeter_path = work_dir / "paymeter_report.csv"
            save_uploaded_file(paymeter_file, paymeter_path)
            eko_path = work_dir / "Eko Trans.csv"
            save_uploaded_file(eko_file, eko_path)

            # pick district path: uploaded -> default -> None
            if district_upload:
                district_path = work_dir / "district.csv"
                save_uploaded_file(district_upload, district_path)
                st.success("Using uploaded district.csv")
            elif DEFAULT_DISTRICT.exists():
                district_path = DEFAULT_DISTRICT
                st.success(f"Using default district.csv from {DEFAULT_DISTRICT}")
            else:
                district_path = None
                st.info("No district.csv will be used (optional)")

            # pick KCG path
            if kcg_upload:
                kcg_path = work_dir / "KCG.csv"
                save_uploaded_file(kcg_upload, kcg_path)
                st.success("Using uploaded KCG.csv")
            elif DEFAULT_KCG.exists():
                kcg_path = DEFAULT_KCG
                st.success(f"Using default KCG.csv from {DEFAULT_KCG}")
            else:
                kcg_path = None
                st.info("No KCG.csv will be used (optional)")

            # pick district_info path
            if district_info_upload:
                district_info_path = work_dir / "district_acct_number.csv"
                save_uploaded_file(district_info_upload, district_info_path)
                st.success("Using uploaded district_acct_number.csv")
            elif DEFAULT_DISTRICT_INFO.exists():
                district_info_path = DEFAULT_DISTRICT_INFO
                st.success(f"Using default district_acct_number.csv from {DEFAULT_DISTRICT_INFO}")
            else:
                district_info_path = None
                st.info("No district_acct_number.csv will be used (optional)")

            # Step A: repair address spill
            st.info("Running address-spill repair...")
            cleaned = work_dir / "paymeter_report_cleaned.csv"
            fixed_count, examples = repair_address_spill(str(paymeter_path), str(cleaned), preview_limit=preview_limit)
            st.success(f"Address repair finished — rows fixed: {fixed_count}")

            if examples:
                st.subheader("Sample repaired rows")
                for ex in examples:
                    st.write(f"Line {ex['line']}")
                    st.write("Before:", ex['before'])
                    st.write("After :", ex['after'])

            # Step B: merge district
            st.info("Merging district data (if provided)...")
            bydistrict = work_dir / "paymeter_report_cleaned_byDistrict.csv"
            if district_path:
                merge_districts(str(cleaned), str(district_path), str(bydistrict))
                st.success("District merge completed.")
            else:
                shutil.copy2(cleaned, bydistrict)
                st.info("No district file: cleaned file copied forward.")

            # Step C: merge Eko and analyze
            st.info("Merging Eko and generating reports...")
            out_detail = work_dir / "Paymeter&EkoReport.csv"
            out_summary = work_dir / "SummaryReport.csv"
            out_excel = work_dir / "Paymeter_Report_FixedKCG.xlsx"

            merge_and_analyze(
                str(eko_path),
                str(bydistrict),
                district_info_path=str(district_info_path) if district_info_path else None,
                kcg_path=str(kcg_path) if kcg_path else None,
                out_detail=str(out_detail),
                out_summary=str(out_summary),
                out_excel=str(out_excel)
            )
            st.success("Merge & analysis completed.")

            # Downloads
            st.header("Download outputs")
            with open(cleaned, "rb") as f: st.download_button("Download cleaned paymeter CSV", f.read(), file_name="paymeter_report_cleaned.csv", mime="text/csv")
            with open(bydistrict, "rb") as f: st.download_button("Download paymeter by district CSV", f.read(), file_name="paymeter_report_cleaned_byDistrict.csv", mime="text/csv")
            with open(out_detail, "rb") as f: st.download_button("Download detailed merged CSV", f.read(), file_name="Paymeter&EkoReport.csv", mime="text/csv")
            with open(out_summary, "rb") as f: st.download_button("Download summary CSV", f.read(), file_name="SummaryReport.csv", mime="text/csv")
            with open(out_excel, "rb") as f: st.download_button("Download Excel workbook", f.read(), file_name="Paymeter_Report_FixedKCG.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

            st.info(f"Temporary files are in: {work_dir} — delete them when you no longer need them.")
        except Exception as err:
            st.error(f"Error during processing: {err}")
