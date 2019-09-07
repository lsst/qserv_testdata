-- test for the & operator

-- pragma sortresult

SELECT objectId from {DBTAG}.Object where objectId & 1 = 1;

