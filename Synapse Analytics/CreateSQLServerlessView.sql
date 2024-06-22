USE wajbahDW 
GO

CREATE or ALTER proc CreateSQLServerlessView @viewName NVARCHAR(100)
AS
BEGIN
DECLARE @statement VARCHAR(MAX)
SET @statement = N'Create or Alter view ' + @viewname + ' AS 
SELECT * 
FROM 
    OPENROWSET(
        BULK ''https://wathba.dfs.core.windows.net/wajbah/Silver/dbo/'+@viewname+'/'',
        FORMAT = ''PARQUET''
    ) AS [result]
    '

EXEC (@statement)
END
GO