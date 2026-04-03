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

print("Reading staging tables...")

products_df = pd.read_sql(
    "SELECT * FROM stg_products",
    staging_engine
)

translation_df = pd.read_sql(
    "SELECT product_category_name, product_category_name_english FROM stg_category_translation",
    staging_engine
)

print("Cleaning products data...")

# remove duplicates
products_df = products_df.drop_duplicates(subset=["product_id"])

# join translation table
products_df = products_df.merge(
    translation_df,
    on="product_category_name",
    how="left"
)

print("Checking if target table already contains data...")

existing = pd.read_sql(
    "SELECT product_id FROM products_target",
    target_engine
)

if len(existing) == 0:

    print("First-time load detected")

    final_df = products_df.copy()

else:

    print("Incremental load detected")

    final_df = products_df[
        ~products_df["product_id"].isin(existing["product_id"])
    ]

print("Loading into products_target...")

final_df.to_sql(
    "products_target",
    con=target_engine,
    if_exists="append",
    index=False
)

print("products_target loaded successfully")