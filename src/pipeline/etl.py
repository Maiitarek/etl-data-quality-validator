import pandas as pd
import sqlite3
import logging
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)

def extract_customers(path: str = "data/raw/customers.csv") -> pd.DataFrame:
    df = pd.read_csv(path)
    logger.info(f"Extracted {len(df)} customer rows")
    return df

def extract_orders(path: str = "data/raw/orders.csv") -> pd.DataFrame:
    df = pd.read_csv(path)
    logger.info(f"Extracted {len(df)} order rows")
    return df

def transform_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["country"] = df["country"].str.upper().str.strip()
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")
    df["customer_segment"] = df["plan"].map({
        "basic": "STANDARD", "premium": "PREMIUM", "enterprise": "ENTERPRISE"
    }).fillna("UNKNOWN")
    df["loaded_at"] = datetime.utcnow().isoformat()
    return df

def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["total_amount"] = df["quantity"] * df["unit_price"]
    df["status"] = df["status"].str.upper().str.strip()
    df["loaded_at"] = datetime.utcnow().isoformat()
    return df

def load_to_db(customers: pd.DataFrame, orders: pd.DataFrame, db_path: str = "data/etl_output.db") -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    customers.to_sql("customers", conn, if_exists="replace", index=False)
    orders.to_sql("orders", conn, if_exists="replace", index=False)
    conn.commit()
    return conn

def run_pipeline(customers_path="data/raw/customers.csv", orders_path="data/raw/orders.csv", db_path="data/etl_output.db") -> sqlite3.Connection:
    return load_to_db(transform_customers(extract_customers(customers_path)), transform_orders(extract_orders(orders_path)), db_path)
