-- Test to verify that a query returns correct results with a secondary index less than comparison. 
-- pragma sortresult

SELECT objectId
FROM Object
WHERE objectId < 400000000000000; 
