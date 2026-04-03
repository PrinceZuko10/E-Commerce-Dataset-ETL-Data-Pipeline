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

print("Reading staging order_items table...")

stg_df = pd.read_sql("SELECT * FROM stg_order_items", staging_engine)

print("Cleaning order_items data...")

# remove duplicates
stg_df = stg_df.drop_duplicates(
    subset=["order_id", "order_item_id"]
)

# convert shipping date
stg_df["shipping_limit_date"] = pd.to_datetime(
    stg_df["shipping_limit_date"],
    errors="coerce"
)

# ensure numeric values valid
stg_df["price"] = pd.to_numeric(
    stg_df["price"],
    errors="coerce"
)

stg_df["freight_value"] = pd.to_numeric(
    stg_df["freight_value"],
    errors="coerce"
)

print("Reading existing target data...")

try:
    tgt_df = pd.read_sql(
        "SELECT order_id, order_item_id, record_hash FROM order_items_target",
        target_engine
    )
except:
    tgt_df = pd.DataFrame(
        columns=["order_id", "order_item_id", "record_hash"]
    )

print("Applying CDC logic...")

merged_df = stg_df.merge(
    tgt_df,
    on=["order_id", "order_item_id"],
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

print("Loading incremental records into order_items_target...")

final_df.to_sql(
    "order_items_target",
    con=target_engine,
    if_exists="append",
    index=False
)

print("order_items_target CDC load completed successfully")