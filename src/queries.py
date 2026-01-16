from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

import pandas as pd


@dataclass
class QueryResults:
    kpi_summary: pd.DataFrame
    monthly_trends: pd.DataFrame
    category_summary: pd.DataFrame
    source_file_summary: pd.DataFrame


def _connect(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(str(db_path))


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    q = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
    return conn.execute(q, (table_name,)).fetchone() is not None


def _read_sql(conn: sqlite3.Connection, sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
    return pd.read_sql_query(sql, conn, params=params or ())


def get_query_results(db_path: Path, *, run_id: str | None = None) -> QueryResults:
    """
    Returns core KPI tables as DataFrames.
    If run_id is provided, queries are filtered to that pipeline run.
    """
    conn = _connect(db_path)
    try:
        if not _table_exists(conn, "transactions"):
            raise RuntimeError("SQLite table 'transactions' not found. Run the pipeline load step first.")

        # Filter clause for a specific run
        where = ""
        params: tuple = ()
        if run_id:
            where = "WHERE run_id = ?"
            params = (run_id,)

        # 1) KPI Summary: totals and row counts
        # Note: We treat amount_num positive/negative as is.
        # If dataset is revenue-only (all positive), expenses will be 0.
        kpi_sql = f"""
            SELECT
                COUNT(*) AS row_count,
                ROUND(SUM(CASE WHEN amount_num > 0 THEN amount_num ELSE 0 END), 2) AS total_income,
                ROUND(ABS(SUM(CASE WHEN amount_num < 0 THEN amount_num ELSE 0 END)), 2) AS total_expenses,
                ROUND(SUM(amount_num), 2) AS net_total
            FROM transactions
            {where}
        """

        # 2) Monthly Trends (YYYY-MM)
        monthly_sql = f"""
            SELECT
                SUBSTR(date_iso, 1, 7) AS year_month,
                ROUND(SUM(amount_num), 2) AS net_total,
                ROUND(SUM(CASE WHEN amount_num > 0 THEN amount_num ELSE 0 END), 2) AS income,
                ROUND(ABS(SUM(CASE WHEN amount_num < 0 THEN amount_num ELSE 0 END)), 2) AS expenses,
                COUNT(*) AS row_count
            FROM transactions
            {where}
            GROUP BY SUBSTR(date_iso, 1, 7)
            ORDER BY year_month
        """

        # 3) Category Summary
        # If category is blank, bucket it as "Uncategorized"
        category_sql = f"""
            SELECT
                CASE
                    WHEN category IS NULL OR TRIM(category) = '' THEN 'Uncategorized'
                    ELSE category
                END AS category,
                ROUND(SUM(amount_num), 2) AS net_total,
                COUNT(*) AS row_count
            FROM transactions
            {where}
            GROUP BY
                CASE
                    WHEN category IS NULL OR TRIM(category) = '' THEN 'Uncategorized'
                    ELSE category
                END
            ORDER BY ABS(net_total) DESC
        """

        # 4) Source file summary (useful for debugging / audit)
        source_file_sql = f"""
            SELECT
                source_file,
                ROUND(SUM(amount_num), 2) AS net_total,
                COUNT(*) AS row_count
            FROM transactions
            {where}
            GROUP BY source_file
            ORDER BY row_count DESC
        """

        kpi_summary = _read_sql(conn, kpi_sql, params)
        monthly_trends = _read_sql(conn, monthly_sql, params)
        category_summary = _read_sql(conn, category_sql, params)
        source_file_summary = _read_sql(conn, source_file_sql, params)

        return QueryResults(
            kpi_summary=kpi_summary,
            monthly_trends=monthly_trends,
            category_summary=category_summary,
            source_file_summary=source_file_summary,
        )

    finally:
        conn.close()
