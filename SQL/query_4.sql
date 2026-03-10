USE smart_store;

SELECT 
    p.product_sub_category,
    SUM(f.sales)                                    AS Total_Sales,
    RANK() OVER (ORDER BY SUM(f.sales) DESC)        AS Sales_Rank
FROM fact_orders f
JOIN dim_product p ON p.product_key = f.product_id
GROUP BY p.product_sub_category
ORDER BY Sales_Rank
LIMIT 5;