from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import pandas as pd


@dataclass
class ReportMetadata:
    output_path: str
    sheets_written: int


def _safe_sheet_name(name: str) -> str:
    # Excel sheet name max is 31 chars
    name = name.strip()
    return name[:31]


def _write_df(writer: pd.ExcelWriter, df: pd.DataFrame, sheet_name: str) -> None:
    sheet = _safe_sheet_name(sheet_name)
    df.to_excel(writer, sheet_name=sheet, index=False)


def _autofit_and_format(writer: pd.ExcelWriter, sheet_name: str, df: pd.DataFrame) -> None:
    """
    Apply basic usability formatting:
    - Freeze top row
    - Auto filter
    - Set column widths (simple heuristic)
    """
    ws = writer.sheets[sheet_name]

    # Freeze header row
    ws.freeze_panes = "A2"

    # Auto filter
    if df.shape[1] > 0 and df.shape[0] > 0:
        ws.auto_filter.ref = ws.dimensions

    # Column widths (simple)
    for i, col in enumerate(df.columns, start=1):
        # width based on header + small sample of rows
        header_len = len(str(col))
        sample = df[col].astype(str).head(50).map(len)
        max_len = max([header_len] + sample.tolist()) if len(sample) else header_len
        width = min(max_len + 2, 45)  # cap to avoid huge columns
        ws.column_dimensions[ws.cell(row=1, column=i).column_letter].width = width


def generate_excel_report(
    output_path: Path,
    *,
    kpi_summary: pd.DataFrame,
    monthly_trends: pd.DataFrame,
    category_summary: pd.DataFrame,
    source_file_summary: pd.DataFrame,
    clean_df: pd.DataFrame,
    issues_df: pd.DataFrame,
) -> ReportMetadata:
    """
    Create a multi-sheet Excel report at output_path.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    sheets: Dict[str, pd.DataFrame] = {
        "Summary": kpi_summary,
        "Monthly_Trends": monthly_trends,
        "Category_Summary": category_summary,
        "Source_File_Summary": source_file_summary,
        "Clean_Data": clean_df,
        "Data_Issues": issues_df,
    }

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for name, df in sheets.items():
            # Ensure df is a DataFrame (defensive)
            if df is None:
                df = pd.DataFrame()

            sheet_name = _safe_sheet_name(name)
            _write_df(writer, df, sheet_name)
            _autofit_and_format(writer, sheet_name, df)

    return ReportMetadata(output_path=str(output_path), sheets_written=len(sheets))
