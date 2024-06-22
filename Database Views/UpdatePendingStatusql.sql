SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[UpdatePendingStatus]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @currentTime DATETIME;
    SET @currentTime = GETDATE();

    UPDATE orders
    SET status = 'cancelled'
    WHERE status = 'pending'
    AND (DATEDIFF(MINUTE, createdon, @currentTime) > 5 
         OR (YEAR(createdon) <> YEAR(@currentTime) OR MONTH(createdon) <> MONTH(@currentTime)));
END
GO

EXEC updatependingstatus
select * from orders