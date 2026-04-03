USE olist_target;

CREATE VIEW vw_sales_analytics AS
SELECT
o.order_id,
o.customer_id,
c.customer_city,
c.customer_state,
oi.product_id,

COALESCE(p.product_category_name_english, 'Unknown Category')
AS product_category_name_english,

oi.price,
oi.freight_value,

pay.payment_value,

o.order_purchase_timestamp,
o.order_delivered_customer_date,

r.review_score

FROM orders_target o

LEFT JOIN customers_target c
ON o.customer_id = c.customer_id

LEFT JOIN order_items_target oi
ON o.order_id = oi.order_id

LEFT JOIN products_target p
ON oi.product_id = p.product_id

LEFT JOIN (
SELECT
order_id,
SUM(payment_value) AS payment_value
FROM payments_target
GROUP BY order_id
) pay
ON o.order_id = pay.order_id

LEFT JOIN (
SELECT
order_id,
AVG(review_score) AS review_score
FROM reviews_target
GROUP BY order_id
) r
ON o.order_id = r.order_id;