-- Basic test to return [VAR]BINARY data.

-- pragma sortresult
SELECT scienceCcdExposureId, hex(poly) as hexPoly 
FROM {DBTAG}.Science_Ccd_Exposure;



