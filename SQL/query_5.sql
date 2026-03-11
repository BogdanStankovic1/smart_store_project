SELECT 
    m.manager_name,
    SUM(f.profit) AS Total_Profit,
    RANK() OVER (ORDER BY SUM(f.profit) DESC)  AS Profit_Rank
FROM fact_orders f
JOIN dim_manager m ON m.manager_id = f.manager_id
GROUP BY m.manager_name
ORDER BY Profit_Rank;