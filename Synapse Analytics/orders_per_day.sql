CREATE PROCEDURE [orders_per_day]
    @ChefId NVARCHAR(20)
AS
BEGIN

    SET NOCOUNT ON;

    SELECT 
        CAST(CreatedOn AS DATE) AS date,
		COUNT(OrderId) as number_of_orders_requested
    FROM 
        Order_table
    WHERE
        (@ChefId IS NULL OR ChefId = @ChefId) 
        AND MONTH([CreatedOn]) = MONTH(GETDATE()) 
        AND YEAR([CreatedOn]) = YEAR(GETDATE())
    GROUP by 
        CAST(CreatedOn AS DATE);
END; 
GO

-- date = date, number of order requested = integer
EXEC orders_per_day @ChefId = NULL
