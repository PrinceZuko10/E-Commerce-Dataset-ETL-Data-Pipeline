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

print("Reading staging sellers table...")

stg_df = pd.read_sql(
    "SELECT * FROM stg_sellers",
    staging_engine
)

print("Cleaning sellers data...")

# remove duplicates
stg_df = stg_df.drop_duplicates(subset=["seller_id"])

# normalize city/state formatting
stg_df["seller_city"] = stg_df["seller_city"].str.lower().str.strip()
stg_df["seller_state"] = stg_df["seller_state"].str.upper().str.strip()

print("Checking existing records in target table...")

existing = pd.read_sql(
    "SELECT seller_id FROM sellers_target",
    target_engine
)

if existing.empty:

    print("First-time load detected")

    final_df = stg_df

else:

    print("Incremental load detected")

    final_df = stg_df.merge(
        existing,
        on="seller_id",
        how="left",
        indicator=True
    )

    final_df = final_df[
        final_df["_merge"] == "left_only"
    ]

    final_df = final_df.drop(columns=["_merge"])

print("Loading data into sellers_target...")

final_df.to_sql(
    "sellers_target",
    con=target_engine,
    if_exists="append",
    index=False
)

print("sellers_target loaded successfully")