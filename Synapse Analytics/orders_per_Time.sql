CREATE PROCEDURE [orders_per_Time]
    @ChefId NVARCHAR(20)
AS
BEGIN

    SET NOCOUNT ON;

    SELECT 
       FORMAT(CreatedOn, 'HH:mm:ss') AS time,
		COUNT(OrderId) as number_of_orders_requested
    FROM 
        Order_table
    WHERE
        (@ChefId IS NULL OR ChefId = @ChefId) 
        AND MONTH([CreatedOn]) = MONTH(GETDATE()) 
        AND YEAR([CreatedOn]) = YEAR(GETDATE())
    GROUP by 
        FORMAT(CreatedOn, 'HH:mm:ss');
END; 
GO

-- time = time, orders_per_Time= integer
EXEC orders_per_Time @ChefId = NULL
