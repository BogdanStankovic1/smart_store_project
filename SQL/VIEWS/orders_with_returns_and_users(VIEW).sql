CREATE VIEW orders_with_returns_and_users AS
SELECT 
    o.*,
    r.Status,
    u.Manager
FROM connect_all_regions o
LEFT JOIN staging_returns r ON r.`Order ID` = o.`Order ID`
JOIN staging_users u ON u.Region = o.Region;

SELECT * FROM orders_with_returns_and_users;