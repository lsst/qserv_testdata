-- test for the XOR operator

-- pragma sortresult

select ra_PS, objectId from Object where ra_PS > 1 XOR ra_PS < 2;

