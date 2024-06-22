CREATE PROCEDURE [Total_revenue]
    @ChefId NVARCHAR(20)
AS
BEGIN

    SET NOCOUNT ON;

    SELECT 
		SUM(TotalPrice) as Total_revenue
    FROM 
        Order_table
    WHERE
        (@ChefId IS NULL OR ChefId = @ChefId)
         AND MONTH([CreatedOn]) = MONTH(GETDATE()) 
         AND YEAR([CreatedOn]) = YEAR(GETDATE());;
END; 
GO

-- TotalPrice column
EXEC Total_revenue @ChefId = NULL