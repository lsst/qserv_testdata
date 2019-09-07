-- test for the ^ operator

-- pragma sortresult

select * from {DBTAG}.Filter where filterId ^ 3 != 0;