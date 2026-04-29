#!/usr/bin/env python3
import argparse
import sys
import logging
from src.pipeline.etl import run_pipeline
from src.validators.suite import run_all_checks
from src.reporters.report_builder import print_results, save_json_report

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-pipeline", action="store_true")
    parser.add_argument("--db-path", default="data/etl_output.db")
    parser.add_argument("--no-report", action="store_true")
    return parser.parse_args()

def main():
    args = parse_args()
    if not args.skip_pipeline:
        print("\nRunning ETL pipeline...")
        conn = run_pipeline(db_path=args.db_path)
        print("ETL complete.")
    else:
        import sqlite3
        conn = sqlite3.connect(args.db_path)
    print("\nRunning data quality checks...")
    results = run_all_checks(conn)
    conn.close()
    print_results(results)
    if not args.no_report:
        save_json_report(results)
    failed = [r for r in results if r.status == "FAIL"]
    sys.exit(1 if failed else 0)

if __name__ == "__main__":
    main()
