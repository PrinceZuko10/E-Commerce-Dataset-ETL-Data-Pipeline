import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
from sqlalchemy import create_engine, text
from config.config import *

# connect staging DB
staging_engine = create_engine(
    f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/olist_staging"
)

# connect target DB
target_engine = create_engine(
    f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/olist_target"
)

print("Reading staging customers table...")

stg_df = pd.read_sql("SELECT * FROM stg_customers", staging_engine)

print("Cleaning data...")

# remove duplicates
stg_df = stg_df.drop_duplicates(subset=["customer_id"])

# clean city names
stg_df["customer_city"] = stg_df["customer_city"].str.lower().str.strip()

# clean state names
stg_df["customer_state"] = stg_df["customer_state"].str.upper().str.strip()

print("Reading existing target data...")

try:
    tgt_df = pd.read_sql("SELECT customer_id, record_hash FROM customers_target", target_engine)
except:
    tgt_df = pd.DataFrame(columns=["customer_id", "record_hash"])

print("Applying CDC logic...")

merged_df = stg_df.merge(
    tgt_df,
    on="customer_id",
    how="left",
    suffixes=("", "_target")
)

new_records = merged_df[merged_df["record_hash_target"].isna()]

updated_records = merged_df[
    (merged_df["record_hash_target"].notna()) &
    (merged_df["record_hash"] != merged_df["record_hash_target"])
]

final_df = pd.concat([new_records, updated_records])

final_df = final_df.drop(columns=["record_hash_target"], errors="ignore")

print("Loading incremental records into target table...")

final_df.to_sql(
    "customers_target",
    con=target_engine,
    if_exists="append",
    index=False
)

print("CDC load completed successfully")