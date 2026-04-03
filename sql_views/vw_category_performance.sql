USE olist_target;

CREATE VIEW vw_category_performance AS
SELECT
product_category_name_english,
SUM(price) AS category_sales,
COUNT(DISTINCT order_id) AS total_orders,
AVG(review_score) AS avg_rating
FROM vw_sales_analytics
GROUP BY product_category_name_english;