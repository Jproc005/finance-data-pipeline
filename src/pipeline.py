from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.ingest import ingest_csv_folder, UserFacingError
from src.transform import transform_transactions
from src.load import load_to_sqlite
from src.queries import get_query_results
from src.report import generate_excel_report


@dataclass
class PipelinePaths:
    raw_dir: Path
    config_path: Path
    db_path: Path
    output_report_path: Path


@dataclass
class PipelineResult:
    success: bool
    message: str
    report_path: str | None = None
    run_id: str | None = None


def run_pipeline(paths: PipelinePaths) -> PipelineResult:
    try:
        print("=== Finance Data Pipeline ===")
        print(f"Input folder: {paths.raw_dir}")
        print(f"Config file:  {paths.config_path}")
        print(f"Database:     {paths.db_path}")
        print(f"Output:       {paths.output_report_path}")
        print("")

        # 1) Ingest
        print("1) Ingesting CSV files...")
        raw_df, ingest_meta = ingest_csv_folder(paths.raw_dir, paths.config_path)
        print(f"   Files read: {ingest_meta.files_read}")
        print(f"   Rows read:  {ingest_meta.rows_read}")
        if ingest_meta.mapped_columns:
            print("   Column mapping (canonical -> actual):")
            for k, v in ingest_meta.mapped_columns.items():
                print(f"     {k} -> {v}")
        print("")

        # 2) Transform
        print("2) Transforming / cleaning data...")
        clean_df, issues_df, tmeta = transform_transactions(raw_df)
        print(f"   Clean rows:   {tmeta.rows_out_clean}")
        print(f"   Issue rows:   {tmeta.rows_out_issues}")
        print(f"   Duplicates removed: {tmeta.duplicates_removed}")
        print("")

        # 3) Load
        print("3) Loading into SQLite database...")
        lmeta, clean_with_audit, issues_with_audit = load_to_sqlite(
            clean_df,
            issues_df,
            paths.db_path,
            mode="replace"
        )
        print(f"   Run ID: {lmeta.run_id}")
        print(f"   Loaded clean rows:  {lmeta.rows_loaded_clean}")
        print(f"   Loaded issue rows:  {lmeta.rows_loaded_issues}")
        print("")

        # 4) Query
        print("4) Generating KPI tables (SQL queries)...")
        results = get_query_results(paths.db_path, run_id=lmeta.run_id)
        print("   KPI tables ready.")
        print("")

        # 5) Report
        print("5) Writing Excel report...")
        rmeta = generate_excel_report(
            paths.output_report_path,
            kpi_summary=results.kpi_summary,
            monthly_trends=results.monthly_trends,
            category_summary=results.category_summary,
            source_file_summary=results.source_file_summary,
            clean_df=clean_with_audit,
            issues_df=issues_with_audit
        )
        print(f"   Report created: {rmeta.output_path}")
        print("")
        print("✅ Pipeline complete.")

        return PipelineResult(
            success=True,
            message="Pipeline completed successfully.",
            report_path=rmeta.output_path,
            run_id=lmeta.run_id
        )

    except UserFacingError as e:
        # Friendly errors for non-technical users
        msg = f"❌ Input Error:\n{e}"
        print(msg)
        return PipelineResult(success=False, message=str(e))

    except Exception as e:
        # Unexpected errors (still keep message readable)
        msg = f"❌ Unexpected Error: {e}"
        print(msg)
        return PipelineResult(success=False, message=msg)
