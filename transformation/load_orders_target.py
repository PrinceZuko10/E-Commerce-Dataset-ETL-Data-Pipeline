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

print("Reading staging orders table...")

stg_df = pd.read_sql("SELECT * FROM stg_orders", staging_engine)

print("Cleaning orders data...")

# remove duplicates
stg_df = stg_df.drop_duplicates(subset=["order_id"])

# convert timestamps safely
date_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for col in date_columns:
    stg_df[col] = pd.to_datetime(stg_df[col], errors="coerce")

# standardize order status text
stg_df["order_status"] = stg_df["order_status"].str.lower().str.strip()

print("Reading existing target data...")

try:
    tgt_df = pd.read_sql(
        "SELECT order_id, record_hash FROM orders_target",
        target_engine
    )
except:
    tgt_df = pd.DataFrame(columns=["order_id", "record_hash"])

print("Applying CDC logic...")

merged_df = stg_df.merge(
    tgt_df,
    on="order_id",
    how="left",
    suffixes=("", "_target")
)

new_records = merged_df[
    merged_df["record_hash_target"].isna()
]

updated_records = merged_df[
    (merged_df["record_hash_target"].notna()) &
    (merged_df["record_hash"] != merged_df["record_hash_target"])
]

final_df = pd.concat([new_records, updated_records])

final_df = final_df.drop(
    columns=["record_hash_target"],
    errors="ignore"
)

print("Loading incremental records into orders_target...")

final_df.to_sql(
    "orders_target",
    con=target_engine,
    if_exists="append",
    index=False
)

print("orders_target CDC load completed successfully")