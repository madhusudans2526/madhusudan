createorreplaceTempView(cloudvsonprem)
------------------------------------------------
val test = spark.sql("""SELECT
    src.CustId,
    CONCAT(
    IIF(EXISTS(SELECT src.CustId       EXCEPT SELECT tgt.CustId      ), TRIM(', CustId      '), ''),
    IIF(EXISTS(SELECT src.CustName     EXCEPT SELECT tgt.CustName    ), TRIM(', CustName    '), ''),
    IIF(EXISTS(SELECT src.CustAddress  EXCEPT SELECT tgt.CustAddress ), TRIM(', CustAddress '), ''),
    IIF(EXISTS(SELECT src.CustPhone    EXCEPT SELECT tgt.CustPhone   ), TRIM(', CustPhone   '), '')) + ', '
AS cols
--INTO cloudvsonprem
FROM Original src
JOIN Revised tgt
  ON src.CustId = tgt.CustId
WHERE EXISTS (SELECT src.* EXCEPT SELECT tgt.*)
""")

------------------------
test.createorreplace TempView(cloudvsonprem)
-----------------------
 -- above querry from 5 to 18 will go to one table(tablename)
 

 
;
WITH src AS (
    SELECT

    SUM(IIF(d.cols LIKE '%, CustId, %'       , 1, 0)) AS CustId,
    SUM(IIF(d.cols LIKE '%, CustName, %'     , 1, 0)) AS CustName,
    SUM(IIF(d.cols LIKE '%, CustAddress, %'  , 1, 0)) AS CustAddress,
    SUM(IIF(d.cols LIKE '%, CustPhone, %'    , 1, 0)) AS CustPhone
    FROM cloudvsonprem d
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
-------------------------------------------------

SELECT *
FROM Original o
JOIN Revised r   ON o.CustId = r.CustId
JOIN cloudvsonprem d ON o.CustId = d.CustId
