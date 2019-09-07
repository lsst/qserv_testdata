-- test for the >> operator

-- pragma sortresult

select * from {DBTAG}.Filter where filterId >> 1 = 1;
