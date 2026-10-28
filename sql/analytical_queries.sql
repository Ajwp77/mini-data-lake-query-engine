/*These are the analytical queries that i performed on the transformed data
  Advanced analytical queries or custom queries can also be performed.*/

SELECT
    customer_state,
    SUM(revenue) AS total_revenue
FROM orders
GROUP BY customer_state
ORDER BY total_revenue DESC;

SELECT
    order_year,
    order_month,
    SUM(revenue) AS total_revenue
FROM orders
GROUP BY order_year, order_month
ORDER BY order_year, order_month;

SELECT
    product_category_name,
    SUM(revenue) AS total_revenue
FROM orders
WHERE product_category_name IS NOT NULL
GROUP BY product_category_name
ORDER BY total_revenue DESC
LIMIT 10;

SELECT
    AVG(order_revenue) AS average_order_value
FROM (
    SELECT
        order_id,
        SUM(revenue) AS order_revenue
    FROM orders
    GROUP BY order_id
);

SELECT
    SUM(revenue) AS total_revenue
FROM orders
WHERE order_year = 2018
  AND order_month = 1;
  
SELECT
    SUM(revenue) AS total_revenue
FROM orders;

/*revenue by state*/
SELECT
    customer_state,
    SUM(revenue) AS total_revenue
FROM orders
GROUP BY customer_state
ORDER BY total_revenue DESC;

/*monthly revenue*/
SELECT
    order_year,
    order_month,
    SUM(revenue) AS total_revenue
FROM orders
GROUP BY order_year, order_month
ORDER BY order_year, order_month;

/*Top product categories*/
SELECT
    product_category_name,
    SUM(revenue) AS total_revenue
FROM orders
WHERE product_category_name IS NOT NULL
GROUP BY product_category_name
ORDER BY total_revenue DESC
LIMIT 10;

/*Order statistics*/
SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    SUM(revenue) AS total_revenue,
    AVG(revenue) AS average_item_revenue
FROM orders;
