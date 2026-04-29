import pytest
import sqlite3
import pandas as pd
from src.pipeline.etl import extract_customers, extract_orders, transform_customers, transform_orders

@pytest.fixture
def raw_customers():
    return pd.DataFrame({
        "customer_id": [1, 2, 3], "name": ["Alice", "Bob", "Carol"],
        "email": ["alice@test.com", None, "carol@test.com"],
        "country": ["usa", "uk", "canada"], "age": [28, 34, 25],
        "signup_date": ["2023-01-15", "2023-02-20", "2023-03-10"],
        "plan": ["premium", "basic", "enterprise"],
    })

@pytest.fixture
def raw_orders():
    return pd.DataFrame({
        "order_id": [1001, 1002], "customer_id": [1, 2],
        "product": ["Laptop", "Mouse"], "quantity": [1, 2],
        "unit_price": [999.99, 29.99], "order_date": ["2023-02-01", "2023-02-15"],
        "status": ["completed", "refunded"],
    })

@pytest.fixture
def in_memory_db(raw_customers, raw_orders):
    conn = sqlite3.connect(":memory:")
    transform_customers(raw_customers).to_sql("customers", conn, if_exists="replace", index=False)
    transform_orders(raw_orders).to_sql("orders", conn, if_exists="replace", index=False)
    conn.commit()
    yield conn
    conn.close()


class TestExtract:
    def test_extract_customers_returns_dataframe(self):
        assert isinstance(extract_customers("data/raw/customers.csv"), pd.DataFrame)

    def test_extract_customers_has_expected_columns(self):
        df = extract_customers("data/raw/customers.csv")
        for col in ["customer_id", "name", "email", "country", "age", "plan"]:
            assert col in df.columns

    def test_extract_customers_not_empty(self):
        assert len(extract_customers("data/raw/customers.csv")) > 0

    def test_extract_orders_returns_dataframe(self):
        assert isinstance(extract_orders("data/raw/orders.csv"), pd.DataFrame)

    def test_extract_orders_has_expected_columns(self):
        df = extract_orders("data/raw/orders.csv")
        for col in ["order_id", "customer_id", "product", "quantity", "unit_price"]:
            assert col in df.columns

    def test_extract_raises_on_missing_file(self):
        with pytest.raises(FileNotFoundError):
            extract_customers("data/raw/nonexistent.csv")


class TestTransformCustomers:
    def test_country_uppercased(self, raw_customers):
        result = transform_customers(raw_customers)
        assert all(result["country"] == result["country"].str.upper())

    def test_signup_date_is_datetime(self, raw_customers):
        result = transform_customers(raw_customers)
        assert pd.api.types.is_datetime64_any_dtype(result["signup_date"])

    def test_customer_segment_derived(self, raw_customers):
        result = transform_customers(raw_customers)
        assert "customer_segment" in result.columns
        assert set(result["customer_segment"].values) == {"PREMIUM", "STANDARD", "ENTERPRISE"}

    def test_loaded_at_added(self, raw_customers):
        assert "loaded_at" in transform_customers(raw_customers).columns

    def test_original_not_mutated(self, raw_customers):
        original = raw_customers["country"].copy()
        transform_customers(raw_customers)
        pd.testing.assert_series_equal(raw_customers["country"], original)


class TestTransformOrders:
    def test_order_date_is_datetime(self, raw_orders):
        assert pd.api.types.is_datetime64_any_dtype(transform_orders(raw_orders)["order_date"])

    def test_total_amount_calculated(self, raw_orders):
        result = transform_orders(raw_orders)
        assert result.iloc[0]["total_amount"] == pytest.approx(999.99)
        assert result.iloc[1]["total_amount"] == pytest.approx(59.98)

    def test_status_uppercased(self, raw_orders):
        result = transform_orders(raw_orders)
        assert all(result["status"] == result["status"].str.upper())

    def test_loaded_at_added(self, raw_orders):
        assert "loaded_at" in transform_orders(raw_orders).columns


class TestLoad:
    def test_customers_table_exists(self, in_memory_db):
        tables = [t[0] for t in in_memory_db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        assert "customers" in tables

    def test_orders_table_exists(self, in_memory_db):
        tables = [t[0] for t in in_memory_db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        assert "orders" in tables

    def test_customers_row_count(self, in_memory_db):
        assert in_memory_db.execute("SELECT COUNT(*) FROM customers").fetchone()[0] == 3

    def test_orders_row_count(self, in_memory_db):
        assert in_memory_db.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == 2
