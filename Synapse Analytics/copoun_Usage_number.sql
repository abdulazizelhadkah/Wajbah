CREATE PROCEDURE [copoun_Usage_number]
    @ChefId NVARCHAR(20)
AS
BEGIN

    SET NOCOUNT ON;

    SELECT 
        Copoun,
		COUNT(Copoun) as Usage_number,
        sum(TotalPrice) as Money_collected_Per_Order
    FROM 
        Order_table
    WHERE
        (@ChefId IS NULL OR ChefId = @ChefId) 
        AND MONTH([CreatedOn]) = MONTH(GETDATE()) 
        AND YEAR([CreatedOn]) = YEAR(GETDATE())
    GROUP by 
        Copoun;
END; 
GO

-- coupoun = nvarchar, Usage_number= integer,Money_collected_Per_Order = double
EXEC copoun_Usage_number @ChefId = NULL