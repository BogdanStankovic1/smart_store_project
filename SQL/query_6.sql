SELECT 
    p.product_sub_category,
    COUNT(f.is_returned) AS Number_of_returns
FROM fact_orders f
JOIN dim_product p ON p.product_key = f.product_id
WHERE f.is_returned = 1
GROUP BY p.product_sub_category
ORDER BY Number_of_returns DESC
LIMIT 1;