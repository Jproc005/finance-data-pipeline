from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd


CANONICAL_REQUIRED = {"date", "amount"}          # description becomes optional
CANONICAL_OPTIONAL = {"description", "category", "source"}


@dataclass
class IngestMetadata:
    files_read: int
    file_paths: List[str]
    rows_read: int
    columns_found: List[str]
    mapped_columns: Dict[str, str]  # canonical -> actual column used


class UserFacingError(Exception):
    """Raised for errors that should be shown to non-technical users."""
    pass


def _standardize_columns(cols: List[str]) -> List[str]:
    return [str(c).strip().lower().replace(" ", "_") for c in cols]


def _read_one_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(
        path,
        dtype=str,               # read everything as text first (dirty exports)
        encoding_errors="ignore",
        keep_default_na=False
    )


def _load_column_map(config_path: Path) -> Dict[str, List[str]]:
    if not config_path.exists():
        raise UserFacingError(
            f"Missing config file: {config_path}\n"
            f"Create it at config/column_map.json (see README/User Manual)."
        )
    try:
        mapping = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception as e:
        raise UserFacingError(
            f"Could not read column map JSON: {config_path}\n"
            f"Error: {e}\n"
            f"Fix the JSON formatting and try again."
        )

    # Standardize mapping entries
    standardized = {}
    for canonical, aliases in mapping.items():
        if not isinstance(aliases, list):
            continue
        standardized[canonical.strip().lower()] = [
            str(a).strip().lower().replace(" ", "_") for a in aliases
        ]
    return standardized


def _find_first_existing_column(df_cols: List[str], aliases: List[str]) -> str | None:
    cols_set = set(df_cols)
    for a in aliases:
        if a in cols_set:
            return a
    return None


def ingest_csv_folder(raw_dir: Path, config_path: Path) -> Tuple[pd.DataFrame, IngestMetadata]:
    """
    Reads all CSV files in raw_dir, applies column mapping, and returns combined DataFrame + metadata.

    Contract:
    - raw_dir exists and has at least 1 CSV
    - mapping file exists at config_path
    - after mapping/renaming, canonical columns must include: date, amount
    - description is optional (but recommended)
    """
    if not raw_dir.exists():
        raise UserFacingError(
            f"Input folder not found: {raw_dir}\n"
            f"Create it and place CSV files inside it."
        )

    csv_files = sorted(raw_dir.glob("*.csv"))
    if not csv_files:
        raise UserFacingError(
            f"No CSV files found in: {raw_dir}\n"
            f"Add one or more CSV files, then run again."
        )

    column_map = _load_column_map(config_path)

    dfs: List[pd.DataFrame] = []
    total_rows = 0
    all_cols = set()

    # Track which actual columns were mapped (canonical -> actual)
    mapped_columns: Dict[str, str] = {}

    for f in csv_files:
        df = _read_one_csv(f)
        df.columns = _standardize_columns(list(df.columns))
        all_cols.update(df.columns)

        # Map canonical columns to existing columns for this file
        rename_dict = {}

        for canonical in list(CANONICAL_REQUIRED | CANONICAL_OPTIONAL):
            aliases = column_map.get(canonical, [canonical])
            found = _find_first_existing_column(list(df.columns), aliases)
            if found:
                rename_dict[found] = canonical
                mapped_columns.setdefault(canonical, found)

        # Apply renaming
        df = df.rename(columns=rename_dict)

        # Add source file traceability
        df["source_file"] = f.name

        dfs.append(df)
        total_rows += len(df)

    combined = pd.concat(dfs, ignore_index=True)

    # Validate required canonical columns exist after mapping
    missing = CANONICAL_REQUIRED - set(combined.columns)
    if missing:
        found = sorted(set(combined.columns))
        raise UserFacingError(
            "Missing required canonical column(s) after mapping: " + ", ".join(sorted(missing)) + "\n\n"
            "This project requires at minimum:\n"
            "- date (a date column)\n"
            "- amount (a numeric revenue/expense column like revenue/net_revenue)\n\n"
            "Columns found in your file(s):\n- " + "\n- ".join(found) + "\n\n"
            "Fix by editing config/column_map.json to map your column names to 'date' and 'amount'."
        )

    meta = IngestMetadata(
        files_read=len(csv_files),
        file_paths=[str(p) for p in csv_files],
        rows_read=total_rows,
        columns_found=sorted(set(combined.columns)),
        mapped_columns=mapped_columns
    )

    return combined, meta
