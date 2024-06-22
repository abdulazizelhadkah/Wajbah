CREATE PROCEDURE [money_collected_per_Time]
    @ChefId NVARCHAR(20)
AS
BEGIN

    SET NOCOUNT ON;

    SELECT 
        FORMAT(CreatedOn, 'HH:mm:ss') AS Time,
	    SUM(TotalPrice) as Money_collected
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
EXEC money_collected_per_Time @ChefId = NULL
