import sqlite3
from dataclasses import dataclass
from typing import Optional


@dataclass
class CheckResult:
    check_name: str
    table: str
    status: str
    details: str
    expected: Optional[str] = None
    actual: Optional[str] = None
    rows_affected: int = 0


def check_row_count(conn, table, min_rows):
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    passed = count >= min_rows
    return CheckResult("row_count", table, "PASS" if passed else "FAIL",
        f"Expected >= {min_rows} rows, found {count}", f">= {min_rows}", str(count), count)


def check_null_rate(conn, table, column, max_null_rate=0.1):
    total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    nulls = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} IS NULL OR {column} = ''").fetchone()[0]
    rate = nulls / total if total > 0 else 0
    passed = rate <= max_null_rate
    return CheckResult("null_rate", table, "PASS" if passed else "FAIL",
        f"Column '{column}': {nulls}/{total} nulls ({rate:.1%}), threshold {max_null_rate:.1%}",
        f"<= {max_null_rate:.1%}", f"{rate:.1%}", nulls)


def check_unique(conn, table, column):
    total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    distinct = conn.execute(f"SELECT COUNT(DISTINCT {column}) FROM {table}").fetchone()[0]
    duplicates = total - distinct
    return CheckResult("uniqueness", table, "PASS" if duplicates == 0 else "FAIL",
        f"Column '{column}': {duplicates} duplicate value(s)", "0 duplicates", f"{duplicates} duplicates", duplicates)


def check_referential_integrity(conn, child_table, child_col, parent_table, parent_col):
    orphans = conn.execute(f"""
        SELECT COUNT(*) FROM {child_table} c
        WHERE c.{child_col} NOT IN (SELECT p.{parent_col} FROM {parent_table} p)
    """).fetchone()[0]
    return CheckResult("referential_integrity", child_table, "PASS" if orphans == 0 else "FAIL",
        f"{orphans} row(s) in {child_table}.{child_col} with no match in {parent_table}.{parent_col}",
        "0 orphan rows", f"{orphans} orphan rows", orphans)


def check_value_range(conn, table, column, min_val=None, max_val=None):
    conditions = []
    if min_val is not None: conditions.append(f"{column} < {min_val}")
    if max_val is not None: conditions.append(f"{column} > {max_val}")
    if not conditions:
        return CheckResult("value_range", table, "PASS", "No range constraints")
    out_of_range = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {' OR '.join(conditions)}").fetchone()[0]
    constraint = f"[{min_val}, {max_val}]"
    return CheckResult("value_range", table, "PASS" if out_of_range == 0 else "FAIL",
        f"Column '{column}': {out_of_range} value(s) outside {constraint}",
        f"all in {constraint}", f"{out_of_range} out of range", out_of_range)


def check_transformation_accuracy(conn, table, computed_col, factor_a, factor_b, tolerance=0.01):
    mismatches = conn.execute(f"""
        SELECT COUNT(*) FROM {table}
        WHERE ABS({computed_col} - ({factor_a} * {factor_b})) > {tolerance}
    """).fetchone()[0]
    return CheckResult("transformation_accuracy", table, "PASS" if mismatches == 0 else "FAIL",
        f"{mismatches} row(s) where {computed_col} != {factor_a}*{factor_b} (tol={tolerance})",
        "0 mismatches", f"{mismatches} mismatches", mismatches)


def check_allowed_values(conn, table, column, allowed):
    placeholders = ", ".join(f"'{v}'" for v in allowed)
    invalid = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} NOT IN ({placeholders})").fetchone()[0]
    return CheckResult("allowed_values", table, "PASS" if invalid == 0 else "FAIL",
        f"Column '{column}': {invalid} value(s) not in {allowed}",
        f"all in {allowed}", f"{invalid} invalid", invalid)
