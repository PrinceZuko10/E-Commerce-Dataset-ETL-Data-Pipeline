USE olist_target;

CREATE VIEW vw_state_performance AS
SELECT
customer_state,
SUM(price) AS total_sales,
COUNT(DISTINCT order_id) AS total_orders
FROM vw_sales_analytics
WHERE customer_state IS NOT NULL
GROUP BY customer_state;