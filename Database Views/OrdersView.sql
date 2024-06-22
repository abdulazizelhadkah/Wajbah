SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER VIEW [dbo].[OrdersView] AS
SELECT 
o.CustomerId,
o.CashDelivered,
o.CompanyId ,
o.Copoun,
o.CreatedOn,
o.DeliveryFees as order_deliveryFees,
o.DeliveryTime,
o.EstimatedTime,
o.OrderId,
o.Status,
o.SubTotal,
o.TotalPrice,
DATEDIFF(year, birthdate, GETDATE()) - 
CASE 
WHEN GETDATE() < DATEADD(year, DATEDIFF(year, birthdate, GETDATE()), birthdate) THEN 1 
ELSE 0 
END AS age,
c.BirthDate,
c.Favourites,
concat(c.FirstName,' ',c.LastName) AS Name,
c.Role,
c.State,
c.UsedCoupones,
c.Wallet  as Customers_Wallet,
co.CompanyName,
co.Area,
co.DeliveryFees as Companies_deliveryFees,
co.Wallet as Companies_Wallet,
m.chefid
FROM Orders o
FULL JOIN Customers c on o.CustomerId = c.CustomerId
LEFT join Companies co on o.CompanyId = co.CompanyId
LEFT JOIN OrderMenuItem om  on o.OrderId = om.OrderId 
Left Join MenuItems m on om.MenuItemId = m.MenuItemId

GO
