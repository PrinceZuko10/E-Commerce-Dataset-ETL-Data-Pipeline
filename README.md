# 🛒 Olist E-Commerce Data Engineering Pipeline

## 📌 Project Overview

This project implements an **End-to-End Data Engineering Pipeline** using the **Brazilian Olist E-Commerce Dataset**.

The pipeline demonstrates:

✅ Data ingestion  
✅ Data cleaning & transformation  
✅ Change Data Capture (CDC) logic  
✅ Warehouse modeling  
✅ Analytics view creation  
✅ Tableau dashboard visualization  

🎯 **Goal:** Simulate a real-world analytics workflow from raw data ingestion to business intelligence reporting.

---

## 🏗️ Architecture Pipeline

### 🔄 Data Flow Architecture

```
Kaggle Dataset
    ↓
AWS S3 (Raw Data Layer)
    ↓
Python Ingestion Scripts
    ↓
MySQL Staging Tables
    ↓
Python Transformation + CDC Processing
    ↓
MySQL Target Warehouse Tables
    ↓
SQL Analytics Views
    ↓
Tableau Dashboards
```

📊 Architecture diagram available inside:

```
architecture/pipeline_architecture.png
```

---

## 📂 Dataset Source

Dataset used:

📦 **Brazilian Olist E-Commerce Dataset (Kaggle)**

Files included:

- olist_customers_dataset.csv
- olist_orders_dataset.csv
- olist_order_items_dataset.csv
- olist_order_payments_dataset.csv
- olist_order_reviews_dataset.csv
- olist_products_dataset.csv
- olist_sellers_dataset.csv
- olist_geolocation_dataset.csv
- product_category_name_translation.csv

Raw datasets stored inside:

```
data/raw/
```

and uploaded to **AWS S3** as part of pipeline raw layer implementation.

---

## 🧱 Pipeline Layers

### ☁️ 1. Raw Layer (AWS S3)

Stores original CSV datasets without modification.

Acts as the **source storage layer** for ingestion scripts.

---

### 🗄️ 2. Staging Layer (MySQL)

Data loaded from **AWS S3 → MySQL staging tables** using Python ingestion scripts.

Example staging tables:

- stg_customers
- stg_orders
- stg_order_items
- stg_products
- stg_reviews
- stg_payments
- stg_sellers

---

### 🧹 3. Transformation Layer (Python + CDC Logic)

Data cleaning and transformation performed using Python scripts.

Operations performed:

✔ Duplicate removal  
✔ Datatype correction  
✔ Null handling  
✔ Category translation mapping  
✔ Normalization of text columns  
✔ Incremental loading using record hashing (**CDC logic**)

Outputs stored in **analytics-ready warehouse tables**

---

### 🏢 4. Target Warehouse Layer (MySQL)

Final cleaned warehouse tables:

- customers_target
- orders_target
- order_items_target
- products_target
- payments_target
- reviews_target
- sellers_target

These act as the **semantic analytics source layer**

---

### 📊 5. Analytics Layer (SQL Views)

Analytics-ready SQL views created:

- vw_sales_analytics
- vw_revenue_summary
- vw_category_performance
- vw_state_performance

Used for:

📈 dashboard joins  
📉 aggregations  
📊 KPI computation  

---

## 📊 Dashboards Created (Tableau)

Two interactive dashboards developed using Tableau.

---

### 📍 1. Sales & Geography Dashboard

Includes:

📈 Total Revenue KPI  
📦 Total Orders KPI  
🚚 Total Freight KPI  
💳 Payment Value KPI  
📅 Monthly Revenue Trend  
📉 Monthly Freight Trend  
🗺️ Sales Distribution by State  
🏙️ Top Cities by Revenue  

Screenshots inside:

```
dashboards/
```

---

### ⭐ 2. Product Category & Customer Experience Dashboard

Includes:

⭐ Average Customer Rating KPI  
📦 Total Categories KPI  
🏙️ Total Cities KPI  
💰 Average Order Value KPI  
📊 Top Categories by Revenue  
📉 Category-wise Rating Analysis  
📊 Review Score Distribution  
🙂 Customer Satisfaction Share  

Screenshots inside:

```
dashboards/
```

---

## 📌 Key Business Insights

Insights derived from dashboards:

📍 São Paulo contributes the highest share of revenue  
⭐ Customer satisfaction average rating remains above **4.0**  
💄 Health & Beauty category generates strong revenue contribution  
📊 Majority of customer ratings fall between **4–5 stars**

---

## 🛠️ Technologies Used

| Layer | Tools |
|------|------|
| Storage | AWS S3 |
| Processing | Python (Pandas, Boto3, SQLAlchemy) |
| Database | MySQL |
| Analytics | SQL Views |
| Visualization | Tableau |

---

## 📁 Project Structure

```
olist-data-engineering-pipeline/

config/
config_template.py

data/
raw/

ingestion/
load_s3_to_mysql.py

transformation/
load_*_target.py

sql_views/
vw_sales_analytics.sql
vw_revenue_summary.sql
vw_category_performance.sql
vw_state_performance.sql

dashboards/
dashboard_sales_geo.png
dashboard_category_experience.png

architecture/
pipeline_architecture.png

README.md
requirements.txt
.gitignore
```

---

## 🚀 How to Run This Pipeline

### Step 1️⃣

Upload raw dataset to AWS S3 bucket

### Step 2️⃣

Configure credentials inside:

```
config/config.py
```

### Step 3️⃣

Run ingestion script:

```
python ingestion/load_s3_to_mysql.py
```

### Step 4️⃣

Run transformation scripts:

```
python transformation/load_customers_target.py
python transformation/load_orders_target.py
python transformation/load_products_target.py
python transformation/load_reviews_target.py
python transformation/load_payments_target.py
python transformation/load_sellers_target.py
```

### Step 5️⃣

Execute SQL view files inside MySQL:

```
sql_views/
```

### Step 6️⃣

Connect Tableau to MySQL target database

Build dashboards using analytics views

---

## 🔮 Future Improvements

Possible enhancements:

📌 Add Apache Airflow DAG orchestration  
📌 Automate incremental loading scheduling  
📌 Add logging framework for monitoring pipeline runs  
📌 Deploy warehouse layer on cloud database services  
📌 Extend analytics views for advanced KPIs

---

## 👨‍💻 Author

**Somit Prakash**

End-to-End Data Engineering Pipeline Project
