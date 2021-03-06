-- Tests that the having clause is handled properly
-- pragma sortresult

SELECT objectId,
       MAX(raFlux) - MIN(raFlux) AS flx
FROM Source
GROUP BY objectId
HAVING MAX(raFlux)-MIN(raFlux) > 5;
