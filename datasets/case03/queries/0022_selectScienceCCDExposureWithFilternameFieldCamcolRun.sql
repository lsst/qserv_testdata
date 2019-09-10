-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

-- pragma sortresult
SELECT sce.filterName, sce.field, sce.camcol, sce.run
FROM   {DBTAG}.Science_Ccd_Exposure AS sce 
WHERE  sce.filterName like '%'
   AND sce.field = 535
   AND sce.camcol like '%'
   AND sce.run = 94;
