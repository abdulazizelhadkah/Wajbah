CREATE VIEW ItemrateView AS
SELECT 
 i.CustomerId,
 i.MenuItemId,
 m.ChefId,
 i.Rating
FROM ItemRateRecords i join Menuitems m on i.MenuItemId = m.MenuItemId

SELECT * from ItemrateView