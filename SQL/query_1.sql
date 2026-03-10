SELECT 
	c.customer_name,
    SUM(f.sales) AS Total_Sales,
    SUM(f.profit) AS Total_Profit,
    COUNT(DISTINCT f.order_id) AS Number_Of_Orders
FROM fact_orders f
JOIN dim_customers c ON c.customer_key = f.customer_id
JOIN dim_geography g ON g.geography_id = f.geography_id
WHERE g.region = 'East'
GROUP BY c.customer_name
ORDER BY Total_Sales DESC
LIMIT 3; 