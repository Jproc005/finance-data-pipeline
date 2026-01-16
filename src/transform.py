from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Tuple

import pandas as pd


@dataclass
class TransformMetadata:
    rows_in: int
    rows_out_clean: int
    rows_out_issues: int
    duplicates_removed: int


# -------------------------
# Helpers
# -------------------------
def _clean_text(series: pd.Series) -> pd.Series:
    # Keep blanks as blanks; strip whitespace; collapse repeated spaces
    s = series.fillna("").astype(str)
    s = s.str.replace(r"\s+", " ", regex=True).str.strip()
    return s


def _parse_date(series: pd.Series) -> Tuple[pd.Series, pd.Series]:
    """
    Returns:
      - parsed ISO date strings YYYY-MM-DD (blank if invalid)
      - boolean mask for invalid (True if original had content but couldn't parse)
    """
    raw = _clean_text(series)
    # Treat empty as empty (not an error)
    raw_is_blank = raw.eq("")

    parsed = pd.to_datetime(raw, errors="coerce", infer_datetime_format=True)
    invalid = parsed.isna() & ~raw_is_blank

    iso = parsed.dt.strftime("%Y-%m-%d")
    iso = iso.fillna("")  # keep blanks
    return iso, invalid


_amount_cleanup_re = re.compile(r"[,\$]")

def _parse_amount(series: pd.Series) -> Tuple[pd.Series, pd.Series]:
    """
    Handles:
      $1,234.56 -> 1234.56
      (45.10)   -> -45.10
      -10       -> -10
    Returns:
      - numeric float series (NaN if invalid)
      - boolean invalid mask (True if original had content but couldn't parse)
    """
    raw = _clean_text(series)

    # Convert parentheses negatives: (123.45) -> -123.45
    raw = raw.str.replace(r"^\((.*)\)$", r"-\1", regex=True)

    # Remove $ and commas
    raw = raw.str.replace(_amount_cleanup_re, "", regex=True)

    raw_is_blank = raw.eq("")
    amt = pd.to_numeric(raw, errors="coerce")

    invalid = amt.isna() & ~raw_is_blank
    return amt, invalid


def _make_txn_key(date_iso: pd.Series, desc: pd.Series, amt: pd.Series) -> pd.Series:
    """
    Deterministic key for duplicate detection.
    Uses date + normalized description + rounded amount.
    """
    desc_norm = _clean_text(desc).str.lower()
    amt_norm = amt.round(2).astype("Float64").astype(str).fillna("")
    key = date_iso.astype(str) + "|" + desc_norm + "|" + amt_norm
    return key


# -------------------------
# Main transform
# -------------------------
def transform_transactions(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, TransformMetadata]:
    """
    Input contract:
      df must include columns: date, description, amount
      ingest step already standardized headers + added source_file

    Output:
      clean_df: valid rows (date + amount parseable)
      issues_df: rows with invalid date/amount (kept, not dropped silently)
      metadata: counts + duplicates removed
    """
    rows_in = len(df)

    out = df.copy()

    # Ensure required columns exist (defensive)
    for col in ["date", "amount"]:
        if col not in out.columns:
            raise ValueError(f"transform_transactions missing required column: {col}")

    # If description is missing, create one (non-technical friendly behavior)
    if "description" not in out.columns:
        out["description"] = ""

    # Clean/normalize text fields
    out["description"] = _clean_text(out["description"])
    if "category" in out.columns:
        out["category"] = _clean_text(out["category"])
    else:
        out["category"] = ""  # optional

    if "source" in out.columns:
        out["source"] = _clean_text(out["source"])
    else:
        out["source"] = ""

    # Parse date and amount
    out["date_iso"], bad_date = _parse_date(out["date"])
    out["amount_num"], bad_amount = _parse_amount(out["amount"])

    # Build issue flags
    out["issue_date_invalid"] = bad_date
    out["issue_amount_invalid"] = bad_amount
    out["has_issue"] = out["issue_date_invalid"] | out["issue_amount_invalid"]

    # Create deterministic key and remove duplicates (across clean rows only)
    out["txn_key"] = _make_txn_key(out["date_iso"], out["description"], out["amount_num"])

    # Split issues first (keep them as-is for audit)
    issues_df = out[out["has_issue"]].copy()

    # Clean rows are those without issues AND with non-empty date_iso
    clean_df = out[~out["has_issue"]].copy()

    # Remove duplicates among clean rows
    before = len(clean_df)
    clean_df = clean_df.drop_duplicates(subset=["txn_key"], keep="first")
    after = len(clean_df)
    duplicates_removed = before - after

    # Select + order columns for clean output
    # Keep original columns, plus standardized ones
    base_cols = []
    for c in ["date_iso", "description", "amount_num", "category", "source", "source_file", "txn_key"]:
        if c in clean_df.columns:
            base_cols.append(c)

    # Include any extra columns that existed in input (not losing info)
    extra_cols = [c for c in clean_df.columns if c not in base_cols and not c.startswith("issue_") and c not in ["has_issue"]]
    clean_df = clean_df[base_cols + extra_cols].copy()

    # For issues, keep helpful columns + flags
    issue_cols = []
    for c in ["date", "date_iso", "description", "amount", "amount_num", "category", "source", "source_file", "txn_key",
              "issue_date_invalid", "issue_amount_invalid"]:
        if c in issues_df.columns:
            issue_cols.append(c)
    extra_issue_cols = [c for c in issues_df.columns if c not in issue_cols and c not in ["has_issue"]]
    issues_df = issues_df[issue_cols + extra_issue_cols].copy()

    meta = TransformMetadata(
        rows_in=rows_in,
        rows_out_clean=len(clean_df),
        rows_out_issues=len(issues_df),
        duplicates_removed=duplicates_removed
    )

    return clean_df, issues_df, meta
