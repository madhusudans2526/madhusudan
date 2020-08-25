IF  OBJECT_ID(N'tempdb..#diffcols', N'U') IS NOT NULL
    DROP TABLE #diffcols;


SELECT 
    src.CustId,
    CONCAT(
    IIF(EXISTS(SELECT src.CustId       EXCEPT SELECT tgt.CustId      ), TRIM(', CustId      '), ''),
    IIF(EXISTS(SELECT src.CustName     EXCEPT SELECT tgt.CustName    ), TRIM(', CustName    '), ''),
    IIF(EXISTS(SELECT src.CustAddress  EXCEPT SELECT tgt.CustAddress ), TRIM(', CustAddress '), ''),
    IIF(EXISTS(SELECT src.CustPhone    EXCEPT SELECT tgt.CustPhone   ), TRIM(', CustPhone   '), '')) + ', '
AS cols
INTO #diffcols
FROM Original src
JOIN Revised tgt
  ON src.CustId = tgt.CustId
WHERE EXISTS (SELECT src.* EXCEPT SELECT tgt.*)
;
     
--SELECT COUNT(*) AS [Difference Row Count] from #diffcols;
 
;
WITH src AS (
    SELECT 

    SUM(IIF(d.cols LIKE '%, CustId, %'       , 1, 0)) AS CustId,
    SUM(IIF(d.cols LIKE '%, CustName, %'     , 1, 0)) AS CustName,
    SUM(IIF(d.cols LIKE '%, CustAddress, %'  , 1, 0)) AS CustAddress,
    SUM(IIF(d.cols LIKE '%, CustPhone, %'    , 1, 0)) AS CustPhone
    FROM #diffcols d
)
SELECT ca.col AS ColumnName, ca.diff AS [Difference Count]
FROM src
CROSS APPLY ( VALUES
    
    ('CustId      ',CustId      ),
    ('CustName    ',CustName    ),
    ('CustAddress ',CustAddress ),
    ('CustPhone   ',CustPhone   )
) ca(col, diff)
WHERE diff > 0
ORDER BY diff desc
;
SELECT *
FROM Original o
JOIN Revised r   ON o.CustId = r.CustId
JOIN #diffcols d ON o.CustId = d.CustId
     
