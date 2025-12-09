#!/usr/bin/env python3
"""
This script parses the various source documents provided for the weekly
video‑conference (ВКС) report and produces JSON files that can be
consumed by a static website.  It extracts summary text from the Word
document, aggregates plan and fact revenue from Excel spreadsheets,
parses dynamic information from the PDF presentation, and processes the
АО «Энергосервис‑Кубани» revenue report.  When run as part of a
GitHub Action, the script should be executed in the root of the
repository with the source documents residing in the `data/` folder.

The JSON files created by this script live in the `site/data/`
directory and are named according to their content.  Front‑end
JavaScript code can fetch these JSON files to render charts and
tables.
"""
import json
import os
import re
import sys
from datetime import datetime

import pandas as pd

# Directories
DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'site', 'data')

def ensure_dirs():
    """Ensure that the output directory exists."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def read_short_report(path: str) -> dict:
    """Extract paragraphs from the Word document using pandoc.

    If pandoc is not available or fails, the function returns an
    empty list.

    Args:
        path: Path to the .docx file.

    Returns:
        A dictionary with a 'paragraphs' key containing a list of
        paragraph strings.
    """
    try:
        import subprocess
        # Use pandoc to convert docx to plain text.  This avoids a
        # dependency on python‑docx, which might not be installed in
        # the GitHub runner environment.
        result = subprocess.run(
            ['pandoc', '-f', 'docx', '-t', 'plain', path],
            capture_output=True, text=True, check=True
        )
        text = result.stdout
    except Exception:
        text = ""
    paragraphs = [p.strip() for p in text.split("\n") if p.strip()]
    return {'paragraphs': paragraphs}

def parse_overall_plan_fact(path: str) -> dict:
    """Parse the overall plan/fact revenue Excel sheet.

    The sheet contains data for the entire company across several
    periods (year, half‑year, quarter and month).  This function
    extracts the row labelled "Всего АО \"Россети Кубань\"" and
    returns structured information for each period.

    Args:
        path: Path to the Excel file.

    Returns:
        A dictionary mapping period names to plan, fact, percent and
        (when available) cash receipts.
    """
    df = pd.read_excel(path, header=None)
    # Locate the row containing the totals for the entire company.
    mask = df[0].astype(str).str.contains('Всего АО', na=False)
    row = df[mask].iloc[0]
    # The header for the periods is in row 2 and row 3.
    periods_row = df.loc[2]
    # Build mapping from column ranges to period names.
    periods = []
    for idx in [1, 5, 9, 13]:
        period_name = periods_row[idx]
        # Some periods span two columns (e.g., 'четвертый', 'квартал'),
        # so join adjacent non‑NaN cells.
        extra = periods_row.get(idx + 1)
        if isinstance(extra, str) and not pd.isna(extra):
            period_name = f"{period_name} {extra}"
        periods.append(period_name.strip())

    data = {}
    for i, start in enumerate([1, 5, 9, 13]):
        period = periods[i] if i < len(periods) else f"period_{i}"
        plan = row[start]
        fact = row[start + 1]
        percent = row[start + 2]
        cash = row[start + 3] if start + 3 < len(row) else None
        try:
            plan = float(plan)
        except Exception:
            plan = None
        try:
            fact = float(fact)
        except Exception:
            fact = None
        try:
            percent = float(percent)
        except Exception:
            percent = None
        try:
            cash = float(cash) if cash not in (None, '', ' ', 'nan') else None
        except Exception:
            cash = None
        data[period] = {
            'plan': plan,
            'fact': fact,
            'percent_of_plan': percent,
            'cash_receipts': cash,
            'difference': (fact - plan) if (fact is not None and plan is not None) else None
        }
    return data

def parse_planned_revenue_by_branch(path: str) -> list:
    """Parse the planned revenue by branch spreadsheet.

    The file contains a header spanning several rows.  Starting from
    row 4 the data contains one row per branch.  This function
    constructs a list of dictionaries with numeric fields converted to
    floats where possible.

    Args:
        path: Path to the Excel file.

    Returns:
        A list of dictionaries keyed by column names.
    """
    df = pd.read_excel(path)
    df = df.iloc[4:].reset_index(drop=True)
    df.columns = [
        'filial',
        'business_plan',
        'fact',
        'planned_confirmed',
        'planned_DKP',
        'planned_unconfirmed',
        'expected_fact',
        'expected_pct'
    ]
    # Convert numeric columns
    numeric_cols = [c for c in df.columns if c != 'filial']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['fact_diff'] = df['fact'] - df['business_plan']
    df['fact_pct'] = (df['fact'] / df['business_plan'] * 100).round(2)
    return df.to_dict(orient='records')

def parse_vols_presentation(path: str) -> dict:
    """Extract key metrics from the VOLS (fiber optics) PDF presentation.

    The PDF contains textual summaries of plan/fact performance by
    quarter and dynamic counts of unauthorized attachments.  This
    parser relies on regular expressions to extract numeric data.

    Args:
        path: Path to the PDF file.

    Returns:
        A dictionary with two keys: 'plan_fact' and 'unauthorized'.
        'plan_fact' maps period names to plan, fact and percent.
        'unauthorized' contains per‑branch counts of previous,
        current and delta for unauthorized attachments.
    """
    # Use pdftotext to convert the PDF to plain text.  Poppler's
    # pdftotext utility is widely available and produces good results.
    import subprocess
    try:
        text = subprocess.run(
            ['pdftotext', path, '-'], capture_output=True, text=True, check=True
        ).stdout
    except Exception:
        text = ''
    # Extract plan/fact table
    plan_fact = {}
    # Pattern: label (Факт 1 кв. 2025 etc.), numbers: opor, percent_of_poles,
    # plan_revenue, fact_revenue, percent_revenue.
    pattern = re.compile(
        r"(Факт \d+ кв\. \d{4}|Факт п/г \d{4}|Прогноз \d+ кв\. \d{4}|План \d{4}|План \d{4})"  # period
        r"\s+([\d\s]+)"       # number of poles
        r"\s+([\d.,]+)%"      # percent of poles
        r"\s+([\d.,]+)"       # plan revenue (mln)
        r"\s+([\d.,]+)"       # fact revenue (mln)
        r"\s+([\d.,]+)%"      # percent of revenue
    )
    for match in pattern.finditer(text):
        period_label = match.group(1).strip()
        poles = match.group(2).replace(' ', '')
        poles = int(poles) if poles.isdigit() else None
        percent_poles = float(match.group(3).replace(',', '.'))
        plan_rev = float(match.group(4).replace(',', '.'))
        fact_rev = float(match.group(5).replace(',', '.'))
        percent_rev = float(match.group(6).replace(',', '.'))
        plan_fact[period_label] = {
            'poles': poles,
            'poles_percent': percent_poles,
            'plan_revenue': plan_rev,
            'fact_revenue': fact_rev,
            'revenue_percent': percent_rev,
            'difference': fact_rev - plan_rev
        }
    # Extract unauthorized dynamics table
    unauthorized = []
    # The section header
    unauth_match = re.search(r"Динамика выявления бездоговорного размещения [^\n]+\n(.+?)Итого", text, re.S)
    if unauth_match:
        lines = unauth_match.group(1).strip().splitlines()
        # Each entry consists of a branch name followed by prev, current, delta.
        for line in lines:
            parts = line.split()
            # branch names may contain spaces; digits start at the end.
            digits = [p for p in parts if re.match(r"[\d+\-]+", p)]
            if not digits:
                continue
            # Branch name is everything before the first digit
            idx = parts.index(digits[0])
            branch_name = ' '.join(parts[:idx])
            nums = parts[idx:]
            # Expect exactly 3 numbers: prev, current, delta (delta may start with + or -)
            if len(nums) >= 3:
                prev = int(nums[0].replace(' ', '').replace(' ', ''))
                curr = int(nums[1].replace(' ', '').replace(' ', ''))
                # Remove any plus sign for delta
                delta_str = nums[2].replace(' ', '').replace(' ', '')
                delta = int(delta_str.replace('+', ''))
                unauthorized.append({
                    'filial': branch_name,
                    'previous': prev,
                    'current': curr,
                    'delta': delta
                })
    return {
        'plan_fact': plan_fact,
        'unauthorized': unauthorized
    }

def parse_energy_service_report(path: str) -> dict:
    """Parse the revenue report for АО «Энергосервис‑Кубани».

    The report is laid out similarly to the overall plan/fact file but
    contains only two rows of data: total revenue and revenue from
    non‑tariff services.  Each row contains four periods (year, 2nd
    half, quarter and month) with plan, fact and percent values.

    Args:
        path: Path to the Excel file.

    Returns:
        A dictionary with two keys: 'total' and 'non_tariff', each of
        which contains a mapping from period names to plan, fact and
        percent.
    """
    df = pd.read_excel(path, sheet_name=0, header=None)
    periods = []
    # Row 2 holds period names; join adjacent cells to form full period names.
    period_row = df.loc[2]
    for idx in [1, 4, 7, 10]:
        name = str(period_row[idx])
        extra = period_row[idx + 1] if (idx + 1) in period_row else ''
        if isinstance(extra, str) and not pd.isna(extra):
            name = f"{name} {extra}"
        periods.append(name.strip())
    def extract_row(row_label: str) -> dict:
        row = df[df[0] == row_label].iloc[0]
        data = {}
        for i, start in enumerate([1, 4, 7, 10]):
            period = periods[i] if i < len(periods) else f"period_{i}"
            plan = row[start]
            fact = row[start + 1]
            pct = row[start + 2]
            try:
                plan = float(plan)
            except Exception:
                plan = None
            try:
                fact = float(fact)
            except Exception:
                fact = None
            try:
                pct = float(pct) if pct not in ('', None) else None
            except Exception:
                pct = None
            data[period] = {
                'plan': plan,
                'fact': fact,
                'percent_of_plan': pct,
                'difference': (fact - plan) if (fact is not None and plan is not None) else None
            }
        return data
    result = {
        'total': extract_row('Выручка в целом по Обществу'),
        'non_tariff': extract_row('Выручка от сторонних юридических лиц (нетарифные услуги)')
    }
    return result

def main():
    ensure_dirs()
    # Paths to input files relative to the data directory.
    # Use simplified Latin filenames to avoid issues with non-Latin characters, spaces and punctuation.
    report_path = os.path.join(DATA_DIR, 'report.docx')
    plan_fact_path = os.path.join(DATA_DIR, 'planfact.xlsx')
    planned_revenue_path = os.path.join(DATA_DIR, 'planrevenue.xlsx')
    vols_presentation_path = os.path.join(DATA_DIR, 'vols.pdf')
    energy_service_path = os.path.join(DATA_DIR, 'energy.xlsx')

    # Read documents and produce JSON.
    short_report = read_short_report(report_path)
    with open(os.path.join(OUTPUT_DIR, 'report_summary.json'), 'w', encoding='utf-8') as f:
        json.dump(short_report, f, ensure_ascii=False, indent=2)

    overall_plan_fact = parse_overall_plan_fact(plan_fact_path)
    with open(os.path.join(OUTPUT_DIR, 'overall_plan_fact.json'), 'w', encoding='utf-8') as f:
        json.dump(overall_plan_fact, f, ensure_ascii=False, indent=2)

    planned_by_branch = parse_planned_revenue_by_branch(planned_revenue_path)
    with open(os.path.join(OUTPUT_DIR, 'planned_by_branch.json'), 'w', encoding='utf-8') as f:
        json.dump(planned_by_branch, f, ensure_ascii=False, indent=2)

    vols_data = parse_vols_presentation(vols_presentation_path)
    with open(os.path.join(OUTPUT_DIR, 'vols_data.json'), 'w', encoding='utf-8') as f:
        json.dump(vols_data, f, ensure_ascii=False, indent=2)

    energy_service = parse_energy_service_report(energy_service_path)
    with open(os.path.join(OUTPUT_DIR, 'energy_service.json'), 'w', encoding='utf-8') as f:
        json.dump(energy_service, f, ensure_ascii=False, indent=2)

if __name__ == '__main__':
    main()