CREATE PROCEDURE [money_collected_per_date]
    @ChefId NVARCHAR(20)
AS
BEGIN

    SET NOCOUNT ON;

    SELECT 
        Cast(CreatedOn As DATE) As Date ,
	    SUM(TotalPrice) as Money_collected
    FROM 
        Order_table
    WHERE
        (@ChefId IS NULL OR ChefId = @ChefId) 
        AND MONTH([CreatedOn]) = MONTH(GETDATE()) 
        AND YEAR([CreatedOn]) = YEAR(GETDATE())
    GROUP by 
        Cast(CreatedOn As DATE);
END; 
GO

-- time = time, orders_per_Time= integer
EXEC money_collected_per_date @ChefId = NULL
