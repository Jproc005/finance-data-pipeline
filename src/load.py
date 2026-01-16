from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd


@dataclass
class LoadMetadata:
    db_path: str
    run_id: str
    loaded_at_utc: str
    rows_loaded_clean: int
    rows_loaded_issues: int


def _utc_now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _make_run_id() -> str:
    # Stable, readable run id (UTC timestamp)
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def _ensure_run_log_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_id TEXT PRIMARY KEY,
            loaded_at_utc TEXT NOT NULL,
            rows_loaded_clean INTEGER NOT NULL,
            rows_loaded_issues INTEGER NOT NULL
        )
        """
    )
    conn.commit()


def _add_audit_columns(df: pd.DataFrame, run_id: str, loaded_at_utc: str) -> pd.DataFrame:
    out = df.copy()
    out["run_id"] = run_id
    out["loaded_at_utc"] = loaded_at_utc
    return out


def load_to_sqlite(
    clean_df: pd.DataFrame,
    issues_df: pd.DataFrame,
    db_path: Path,
    *,
    mode: str = "replace"
) -> Tuple[LoadMetadata, pd.DataFrame, pd.DataFrame]:
    """
    Loads clean and issues dataframes into SQLite.

    mode:
      - "replace" (default): replaces tables each run (simple + predictable)
      - "append": appends new rows (advanced; use later with dedupe keys)

    Returns:
      metadata, clean_df_with_audit, issues_df_with_audit
    """
    if mode not in {"replace", "append"}:
        raise ValueError("mode must be 'replace' or 'append'")

    run_id = _make_run_id()
    loaded_at_utc = _utc_now_iso()

    clean_out = _add_audit_columns(clean_df, run_id, loaded_at_utc)
    issues_out = _add_audit_columns(issues_df, run_id, loaded_at_utc)

    conn = _connect(db_path)
    try:
        _ensure_run_log_table(conn)

        # Write dataframes to SQLite
        # Use if_exists based on mode
        if_exists = "replace" if mode == "replace" else "append"

        clean_out.to_sql("transactions", conn, if_exists=if_exists, index=False)
        issues_out.to_sql("data_issues", conn, if_exists=if_exists, index=False)

        # Log the run
        conn.execute(
            """
            INSERT OR REPLACE INTO pipeline_runs (run_id, loaded_at_utc, rows_loaded_clean, rows_loaded_issues)
            VALUES (?, ?, ?, ?)
            """,
            (run_id, loaded_at_utc, int(len(clean_out)), int(len(issues_out)))
        )
        conn.commit()

        meta = LoadMetadata(
            db_path=str(db_path),
            run_id=run_id,
            loaded_at_utc=loaded_at_utc,
            rows_loaded_clean=int(len(clean_out)),
            rows_loaded_issues=int(len(issues_out)),
        )

        return meta, clean_out, issues_out

    finally:
        conn.close()
