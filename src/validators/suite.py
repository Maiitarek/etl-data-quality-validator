import sqlite3
from src.validators.checks import (
    check_row_count, check_null_rate, check_unique,
    check_referential_integrity, check_value_range,
    check_transformation_accuracy, check_allowed_values,
)

def run_all_checks(conn: sqlite3.Connection) -> list:
    results = []
    results.append(check_row_count(conn, "customers", min_rows=10))
    results.append(check_null_rate(conn, "customers", "name", max_null_rate=0.0))
    results.append(check_null_rate(conn, "customers", "email", max_null_rate=0.3))
    results.append(check_unique(conn, "customers", "customer_id"))
    results.append(check_unique(conn, "customers", "email"))
    results.append(check_value_range(conn, "customers", "age", min_val=0, max_val=120))
    results.append(check_allowed_values(conn, "customers", "customer_segment",
        ["STANDARD", "PREMIUM", "ENTERPRISE", "UNKNOWN"]))
    results.append(check_row_count(conn, "orders", min_rows=10))
    results.append(check_null_rate(conn, "orders", "order_id", max_null_rate=0.0))
    results.append(check_unique(conn, "orders", "order_id"))
    results.append(check_value_range(conn, "orders", "quantity", min_val=1))
    results.append(check_value_range(conn, "orders", "unit_price", min_val=0.01))
    results.append(check_transformation_accuracy(conn, "orders", "total_amount", "quantity", "unit_price"))
    results.append(check_allowed_values(conn, "orders", "status",
        ["COMPLETED", "PENDING", "REFUNDED", "CANCELLED"]))
    results.append(check_referential_integrity(conn, "orders", "customer_id", "customers", "customer_id"))
    return results
