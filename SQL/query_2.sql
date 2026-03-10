SELECT 
	dim_product.product_sub_category,
    COUNT(DISTINCT fact_orders.order_id) AS Number_of_orders,
    SUM(fact_orders.sales) AS Total_sales,
    SUM(fact_orders.profit) AS Total_profit
FROM fact_orders
JOIN dim_product ON dim_product.product_key = fact_orders.product_id
GROUP BY dim_product.product_sub_category
ORDER BY Number_of_orders DESC
LIMIT 3;