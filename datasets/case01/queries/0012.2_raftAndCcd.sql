-- Test ORDER BY ... LIMIT ... against 
-- non-chunked data

SELECT sce.filterId, sce.filterName
FROM   {DBTAG}.Science_Ccd_Exposure AS sce
WHERE  (sce.visit = 887404831)
   AND (sce.raftName = '3,3')
   AND (sce.ccdName LIKE '%')
ORDER BY filterId LIMIT 5
