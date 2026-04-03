import sys
import os

# allow script to find config folder
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pandas as pd
import boto3
import hashlib
from sqlalchemy import create_engine
from datetime import datetime
from config.config import *

# connect to S3
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

# connect to MySQL
engine = create_engine(
    f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}"
)

# folder inside S3 bucket
S3_FOLDER = "Use Case E-Commerce Data"

# mapping S3 files → MySQL staging tables
file_table_map = {
    "olist_customers_dataset.csv": "stg_customers",
    "olist_geolocation_dataset.csv": "stg_geolocation",
    "olist_orders_dataset.csv": "stg_orders",
    "olist_order_items_dataset.csv": "stg_order_items",
    "olist_order_payments_dataset.csv": "stg_order_payments",
    "olist_order_reviews_dataset.csv": "stg_order_reviews",
    "olist_products_dataset.csv": "stg_products",
    "olist_sellers_dataset.csv": "stg_sellers",
    "product_category_name_translation.csv": "stg_category_translation"
}


def generate_hash(row):
    return hashlib.md5(str(row.values).encode()).hexdigest()


def load_file(file_name, table_name):

    print(f"Loading {file_name} into {table_name}")

    try:

        file_path = f"{S3_FOLDER}/{file_name}"

        obj = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=file_path
        )

        # read CSV in chunks (important for large geolocation file)
        df_iter = pd.read_csv(obj["Body"], chunksize=10000)

        for df in df_iter:

            df["source_file"] = file_name
            df["load_timestamp"] = datetime.now()

            df["record_hash"] = df.apply(generate_hash, axis=1)

            df.to_sql(
                table_name,
                con=engine,
                if_exists="append",
                index=False
            )

        print(f"{table_name} loaded successfully")

    except Exception as e:

        print(f"Error loading {file_name}: {e}")


def main():

    for file_name, table_name in file_table_map.items():

        load_file(file_name, table_name)


if __name__ == "__main__":
    main()