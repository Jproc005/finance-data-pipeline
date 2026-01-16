# Finance Data Automation Pipeline

A **one-click financial reporting pipeline** that converts raw CSV exports into a standardized, Excel-ready report for **non-technical business users**.

This project demonstrates:
- Python automation
- Data cleaning and validation
- SQLite storage
- SQL-based KPI generation
- Excel reporting
- User-friendly execution (no coding required)

---

## What Problem This Solves

Finance and operations teams often receive CSV exports from multiple systems with:
- Inconsistent column names
- Messy formatting
- Missing or invalid values
- No standard reporting structure

This pipeline lets a user:
1. Drop CSV files into a folder
2. Double-click one file
3. Receive a clean, standardized Excel report

No Python knowledge required.

---

## How Non-Technical Users Run It

1. Place CSV files into:

data/raw/

2. Double-click:

run_pipeline.bat

3. Open the generated Excel file:

data/output/finance_report.xlsx


---

## Excel Report Output

The generated Excel workbook contains:

| Sheet Name | Description |
|-----------|------------|
| Summary | KPI totals (income, expenses, net) |
| Monthly_Trends | Monthly rollups |
| Category_Summary | Totals by category |
| Source_File_Summary | Totals by source file |
| Clean_Data | Fully cleaned dataset |
| Data_Issues | Rows with validation problems |

---

## Flexible Column Mapping

This pipeline supports inconsistent CSV headers using a mapping file:



config/column_map.json


Example:

```json
{
  "date": ["date", "transaction_date", "posted_date"],
  "amount": ["amount", "net_revenue", "revenue", "value"],
  "description": ["description", "memo", "channel", "dataset"]
}


Project Structure
finance-data-pipeline/
│
├─ config/
│   └─ column_map.json
├─ data/
│   ├─ raw/            # user drops CSVs here
│   ├─ processed/
│   └─ output/         # Excel report appears here
├─ database/
│   └─ finance.db
├─ src/
│   ├─ ingest.py
│   ├─ transform.py
│   ├─ load.py
│   ├─ queries.py
│   ├─ report.py
│   └─ pipeline.py
├─ run_pipeline.py
├─ run_pipeline.bat
├─ requirements.txt
└─ README.md

Developer Setup

Install dependencies:

pip install -r requirements.txt


Run pipeline:

python run_pipeline.py

Tech Stack

Python

Pandas

SQLite

SQL

openpyxl