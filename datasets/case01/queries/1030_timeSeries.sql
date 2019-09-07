-- Select time series data for all objects 
-- in a given area of the sky, 
-- in a given photometric band 
-- Similar query: http://dev.lsstcorp.org/trac/wiki/dbQuery007

-- See ticket #2052: https://dev.lsstcorp.org/trac/ticket/2052

-- pragma noheader
SELECT objectId, taiMidPoint, scisql_fluxToAbMag(psfFlux)
FROM   {DBTAG}.Source
JOIN   {DBTAG}.Object USING(objectId)
JOIN   {DBTAG}.Filter USING(filterId)
 WHERE ra_PS BETWEEN 355 AND 360 -- noQserv
   and decl_PS BETWEEN 0 AND 20  -- noQserv
-- withQserv WHERE qserv_areaspec_box(355, 0, 360, 20)
   AND filterName = 'g'
ORDER BY objectId, taiMidPoint ASC
