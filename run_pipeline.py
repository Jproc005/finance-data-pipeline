from pathlib import Path

from src.pipeline import run_pipeline, PipelinePaths


def main():
    paths = PipelinePaths(
        raw_dir=Path("data/raw"),
        config_path=Path("config/column_map.json"),
        db_path=Path("database/finance.db"),
        output_report_path=Path("data/output/finance_report.xlsx"),
    )

    result = run_pipeline(paths)

    # Exit codes matter for automation
    raise SystemExit(0 if result.success else 1)


if __name__ == "__main__":
    main()
