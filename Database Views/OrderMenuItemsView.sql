create VIEW OrderMenuItemsView AS
SELECT 
    om.*,
    CASE
        WHEN om.size = 'Large' THEN m.PriceLarge
        WHEN om.size = 'Medium' THEN m.PriceMedium
        WHEN om.size = 'Small' THEN m.PriceSmall
    END AS Price,
    om.quantity * 
    CASE
        WHEN om.size = 'large' THEN m.PriceLarge
        WHEN om.size = 'medium' THEN m.PriceMedium
        WHEN om.size = 'small' THEN m.PriceSmall
    END AS TotalPrice
FROM 
    OrderMenuItem AS om
JOIN
    MenuItems AS m
ON
    om.MenuItemId = m.MenuItemId

select * FROM OrderMenuItemsView