
-- this query exercises secondary index in an interesting way:
-- because Qserv needs to figure out what the director table
-- is and use it when setting up its secondary index.
-- This query returns 0 rows

SELECT sourceId, objectId
FROM {DBTAG}.Source
WHERE objectId IN (1234)
ORDER BY sourceId;
