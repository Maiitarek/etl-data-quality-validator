# ETL Data Quality Validator

[![CI](https://github.com/Maiitarek/etl-data-quality-validator/actions/workflows/ci.yml/badge.svg)](https://github.com/Maiitarek/etl-data-quality-validator/actions)
[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat&logo=python&logoColor=white)](https://python.org)
[![pytest](https://img.shields.io/badge/pytest-8.0+-0A9EDC?style=flat&logo=pytest&logoColor=white)](https://pytest.org)
[![SQLite](https://img.shields.io/badge/SQLite-003B57?style=flat&logo=sqlite&logoColor=white)](https://sqlite.org)

A production-style Python framework that simulates a real ETL pipeline (CSV → transform → SQLite), then runs 15 automated data quality checks — null rates, row counts, referential integrity, transformation accuracy, value ranges, and allowed values.

Mirrors real-world ETL QA work done with GCP BigQuery, Oracle, and Teradata.

---

## Quick start

```bash
git clone https://github.com/Maiitarek/etl-data-quality-validator.git
cd etl-data-quality-validator
pip install -r requirements.txt
python main.py
```

---

## What it does

```
CSV files (raw data)
      |
   Extract    — reads customers.csv + orders.csv
      |
  Transform   — normalises fields, derives columns, calculates totals
      |
    Load      — writes to SQLite database
      |
  Validate    — runs 15 data quality checks
      |
   Report     — prints results + saves JSON report
```

---

## Data quality checks

| Check | What it validates |
|---|---|
| Row count | Table has minimum expected rows |
| Null rate | Column nulls below threshold |
| Uniqueness | No duplicate values |
| Referential integrity | All foreign keys exist in parent table |
| Value range | Numeric values within min/max bounds |
| Transformation accuracy | Computed column matches formula |
| Allowed values | Values belong to permitted set |

---

## Project structure

```
etl-data-quality-validator/
├── src/
│   ├── pipeline/etl.py           # Extract, Transform, Load
│   ├── validators/checks.py      # 7 check types
│   ├── validators/suite.py       # 15-check suite runner
│   └── reporters/report_builder  # Console + JSON reports
├── tests/
│   ├── test_pipeline.py          # 14 ETL unit tests
│   └── test_validators.py        # 14 validator unit tests
├── data/raw/
│   ├── customers.csv             # Sample data (with seeded quality issues)
│   └── orders.csv
└── main.py
```

---

## Usage

```bash
python main.py                     # full pipeline + checks
python main.py --skip-pipeline     # validate existing DB
python main.py --no-report         # skip JSON report
pytest tests/ -v                   # unit tests only
```

---

## The seeded quality issues

The raw data intentionally contains real data problems to demonstrate detection:

- `orders.csv` row 1011: `customer_id=99` — no matching customer (orphan FK)
- `orders.csv` row 1013: `quantity=-1` — invalid negative quantity
- `orders.csv` row 1014: `quantity=0` — invalid zero quantity

The validator detects all three and reports them as FAIL with exact row counts.

---

## Real-world equivalents

| This project | Production |
|---|---|
| SQLite | BigQuery / Oracle / Teradata / PostgreSQL |
| CSV files | S3 / GCS / HDFS / Hive tables |
| pandas transforms | Spark / dbt / Dataflow |

---

## Author

**Mai Ibrahim** — Senior SDET / QA Engineer  
[LinkedIn](https://www.linkedin.com/in/mai-tarek/) · [GitHub](https://github.com/Maiitarek)
