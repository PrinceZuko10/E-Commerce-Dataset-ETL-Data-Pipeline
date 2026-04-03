import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from sqlalchemy import create_engine
from config.config import *

# connect staging DB
staging_engine = create_engine(
    f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/olist_staging"
)

# connect target DB
target_engine = create_engine(
    f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/olist_target"
)

print("Reading staging payments table...")

stg_df = pd.read_sql(
    "SELECT * FROM stg_order_payments",
    staging_engine
)

print("Cleaning payments data...")

# remove duplicates
stg_df = stg_df.drop_duplicates(
    subset=["order_id", "payment_sequential"]
)

# ensure numeric format
stg_df["payment_value"] = pd.to_numeric(
    stg_df["payment_value"],
    errors="coerce"
)

print("Checking existing records in target table...")

existing = pd.read_sql(
    "SELECT order_id, payment_sequential FROM payments_target",
    target_engine
)

if existing.empty:

    print("First-time load detected")

    final_df = stg_df

else:

    print("Incremental load detected")

    final_df = stg_df.merge(
        existing,
        on=["order_id", "payment_sequential"],
        how="left",
        indicator=True
    )

    final_df = final_df[final_df["_merge"] == "left_only"]

    final_df = final_df.drop(columns=["_merge"])

print("Loading data into payments_target...")

final_df.to_sql(
    "payments_target",
    con=target_engine,
    if_exists="append",
    index=False
)

print("payments_target loaded successfully")