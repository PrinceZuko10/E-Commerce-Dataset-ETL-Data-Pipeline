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

print("Reading staging reviews table...")

stg_df = pd.read_sql(
    "SELECT * FROM stg_order_reviews",
    staging_engine
)

print("Cleaning reviews data...")

# remove duplicates
stg_df = stg_df.drop_duplicates(subset=["review_id"])

# convert datetime columns
date_columns = [
    "review_creation_date",
    "review_answer_timestamp"
]

for col in date_columns:
    stg_df[col] = pd.to_datetime(
        stg_df[col],
        errors="coerce"
    )

print("Checking existing records in target table...")

existing = pd.read_sql(
    "SELECT review_id FROM reviews_target",
    target_engine
)

if existing.empty:

    print("First-time load detected")

    final_df = stg_df

else:

    print("Incremental load detected")

    final_df = stg_df.merge(
        existing,
        on="review_id",
        how="left",
        indicator=True
    )

    final_df = final_df[
        final_df["_merge"] == "left_only"
    ]

    final_df = final_df.drop(columns=["_merge"])

print("Loading data into reviews_target...")

final_df.to_sql(
    "reviews_target",
    con=target_engine,
    if_exists="append",
    index=False
)

print("reviews_target loaded successfully")