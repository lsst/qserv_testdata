-- Test that result size can exceed xrootd maxRSZ (ie 2MB) build-in constant

SELECT scienceCcdExposureId
FROM Science_Ccd_Exposure_Metadata
LIMIT 3500
