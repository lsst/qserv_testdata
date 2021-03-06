-- Tests that the having clause is handled properly
-- pragma sortresult

SELECT objectId,
       MAX(raFlux) - MIN(raFlux) as `flx`
FROM Source
GROUP BY objectId
HAVING `flx` > 5
LIMIT 5;
