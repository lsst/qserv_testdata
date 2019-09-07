-- test for the MOD operator

-- pragma sortresult

select objectId, ra_PS, decl_PS from {DBTAG}.Object where ra_PS MOD 3 > 1.5

