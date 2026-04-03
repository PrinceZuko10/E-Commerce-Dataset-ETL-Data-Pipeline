USE olist_target;

CREATE VIEW vw_revenue_summary AS
SELECT
DATE(order_purchase_timestamp) AS order_date,
SUM(price) AS total_sales,
SUM(freight_value) AS total_freight,
SUM(payment_value) AS total_payment_value,
COUNT(DISTINCT order_id) AS total_orders
FROM vw_sales_analytics
GROUP BY DATE(order_purchase_timestamp);