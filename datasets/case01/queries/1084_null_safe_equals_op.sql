-- find columns whose value is NULL using the null safe
-- equals operator, `<=>`


-- pragma sortresult

select objectId from {DBTAG}.Object where raRange <=> NULL;

