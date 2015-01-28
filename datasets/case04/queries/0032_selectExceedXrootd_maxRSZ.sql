-- Test that result size can exceed xrootd maxRSZ (ie 2MB) build-in constant
-- binary field 'poly' removed until Qserv support binary field

SELECT scienceCcdExposureId, run, camcol, filterId, field, filterName, ra, decl,
    htmId20, equinox, raDeSys, ctype1, ctype2, crpix1, crpix2, crval1, crval2,
    cd1_1, cd1_2, cd2_1, cd2_2, corner1Ra, corner1Decl, corner2Ra, corner2Decl,
    corner3Ra, corner3Decl, corner4Ra, corner4Decl, taiMjd, obsStart, expMidpt,
    expTime, nCombine, binX, binY, fluxMag0, fluxMag0Sigma, fwhm, path, airmass
FROM Science_Ccd_Exposure
LIMIT 4000
