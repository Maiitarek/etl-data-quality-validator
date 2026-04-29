import json
from datetime import datetime
from pathlib import Path

REPORTS_DIR = "reports"

def print_results(results):
    passed = [r for r in results if r.status == "PASS"]
    failed = [r for r in results if r.status == "FAIL"]
    print("\n" + "=" * 70)
    print("  ETL DATA QUALITY VALIDATOR — RESULTS")
    print("=" * 70)
    for r in results:
        icon = "✓" if r.status == "PASS" else "✗"
        print(f"  {icon} [{r.status}]  {r.table:<12} {r.check_name:<28} {r.details}")
    print("=" * 70)
    print(f"  Total: {len(results)}  |  Passed: {len(passed)}  |  Failed: {len(failed)}  |  Pass rate: {len(passed)/len(results)*100:.0f}%")
    print("=" * 70)
    if failed:
        print("\n  FAILED CHECKS:")
        for r in failed:
            print(f"  ✗ [{r.table}] {r.check_name} — {r.details}")

def save_json_report(results, output_dir=REPORTS_DIR):
    Path(output_dir).mkdir(exist_ok=True)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    path = Path(output_dir) / f"dq_report_{ts}.json"
    report = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "summary": {
            "total": len(results),
            "passed": sum(1 for r in results if r.status == "PASS"),
            "failed": sum(1 for r in results if r.status == "FAIL"),
        },
        "checks": [{"check_name": r.check_name, "table": r.table, "status": r.status,
                    "details": r.details, "rows_affected": r.rows_affected} for r in results],
    }
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"\n  Report saved: {path}")
    return str(path)
