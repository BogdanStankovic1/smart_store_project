CREATE OR REPLACE VIEW view_3 AS 
SELECT 
    `Customer ID`,
    `Customer Name`,
    ROUND(SUM(Profit), 2)   AS Total_Profit,
    ROUND(SUM(Sales), 2)    AS Total_Sales,
    ROUND(SUM(Discount), 2) AS Total_Discount
FROM connect_all_regions
GROUP BY `Customer ID`, `Customer Name`
ORDER BY Total_Sales DESC;
    
    
SELECT * FROM view_3;
    
		