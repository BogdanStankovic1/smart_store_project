SELECT 
    cal.month_name,
    AVG(p.product_base_margin) AS Avg_Product_Base_Margin,
    AVG(f.profit)              AS Avg_Profit,
    AVG(f.sales)               AS Avg_Sales
FROM fact_orders f
JOIN dim_product  p   ON p.product_key = f.product_id
JOIN dim_calendar cal ON cal.date_id   = f.order_date_id
WHERE f.quantity > 10
AND f.sales > (SELECT AVG(sales) FROM fact_orders)
GROUP BY cal.month_name, cal.month_number
ORDER BY cal.month_number;