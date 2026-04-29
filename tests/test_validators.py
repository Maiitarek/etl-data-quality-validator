import pytest
import sqlite3
from src.validators.checks import (
    check_row_count, check_null_rate, check_unique,
    check_referential_integrity, check_value_range,
    check_transformation_accuracy, check_allowed_values,
)

@pytest.fixture
def db():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE customers (customer_id INTEGER, name TEXT, email TEXT, country TEXT, age INTEGER, customer_segment TEXT)")
    conn.executemany("INSERT INTO customers VALUES (?,?,?,?,?,?)", [
        (1, "Alice", "alice@test.com", "USA", 28, "PREMIUM"),
        (2, "Bob",   None,            "UK",  34, "STANDARD"),
        (3, "Carol", "carol@test.com","USA", 25, "ENTERPRISE"),
        (4, "Dave",  "dave@test.com", "AUS", 45, "STANDARD"),
        (5, "Eve",   "eve@test.com",  "USA", 31, "PREMIUM"),
    ])
    conn.execute("CREATE TABLE orders (order_id INTEGER, customer_id INTEGER, product TEXT, quantity INTEGER, unit_price REAL, total_amount REAL, status TEXT)")
    conn.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?,?)", [
        (1001, 1,  "Laptop",   1, 999.99, 999.99, "COMPLETED"),
        (1002, 2,  "Mouse",    2, 29.99,  59.98,  "COMPLETED"),
        (1003, 3,  "Keyboard", 1, 79.99,  79.99,  "REFUNDED"),
        (1004, 99, "Monitor",  1, 349.99, 349.99, "COMPLETED"),  # orphan
        (1005, 5,  "Webcam",   0, 89.99,  0.0,    "PENDING"),    # invalid qty
    ])
    conn.commit()
    yield conn
    conn.close()


class TestRowCount:
    def test_passes_above_min(self, db): assert check_row_count(db, "customers", 3).status == "PASS"
    def test_fails_below_min(self, db): assert check_row_count(db, "customers", 100).status == "FAIL"
    def test_correct_count(self, db): assert check_row_count(db, "customers", 1).actual == "5"

class TestNullRate:
    def test_passes_below_threshold(self, db): assert check_null_rate(db, "customers", "name", 0.0).status == "PASS"
    def test_fails_above_threshold(self, db): assert check_null_rate(db, "customers", "email", 0.0).status == "FAIL"
    def test_detects_one_null(self, db): assert check_null_rate(db, "customers", "email", 0.5).rows_affected == 1

class TestUnique:
    def test_passes_unique(self, db): assert check_unique(db, "customers", "customer_id").status == "PASS"
    def test_fails_duplicate(self, db): assert check_unique(db, "customers", "country").status == "FAIL"

class TestReferentialIntegrity:
    def test_fails_orphan(self, db):
        result = check_referential_integrity(db, "orders", "customer_id", "customers", "customer_id")
        assert result.status == "FAIL" and result.rows_affected == 1
    def test_passes_valid(self, db):
        assert check_referential_integrity(db, "customers", "customer_id", "customers", "customer_id").status == "PASS"

class TestValueRange:
    def test_passes_valid_ages(self, db): assert check_value_range(db, "customers", "age", 0, 120).status == "PASS"
    def test_fails_invalid_qty(self, db):
        result = check_value_range(db, "orders", "quantity", min_val=1)
        assert result.status == "FAIL" and result.rows_affected == 1

class TestTransformationAccuracy:
    def test_passes_correct_calc(self, db): assert check_transformation_accuracy(db, "orders", "total_amount", "quantity", "unit_price").status == "PASS"
    def test_check_name(self, db): assert check_transformation_accuracy(db, "orders", "total_amount", "quantity", "unit_price").check_name == "transformation_accuracy"

class TestAllowedValues:
    def test_passes_valid(self, db): assert check_allowed_values(db, "customers", "customer_segment", ["STANDARD","PREMIUM","ENTERPRISE"]).status == "PASS"
    def test_fails_invalid(self, db): assert check_allowed_values(db, "orders", "status", ["COMPLETED"]).status == "FAIL"
