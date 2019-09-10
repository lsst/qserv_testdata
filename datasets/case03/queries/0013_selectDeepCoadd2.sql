-- https://dev.lsstcorp.org/trac/wiki/dbPipeQAQueries

-- pragma sortresult
SELECT sce.filterName, sce.tract, sce.patch
FROM   {DBTAG}.DeepCoadd AS sce
WHERE  sce.filterName = 'r'
   AND sce.tract = 0
   AND sce.patch = '159,3';

