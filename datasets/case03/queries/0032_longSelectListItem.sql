-- Used to reproduce DM-20693

-- pragma sortresult
-- withQserv  SELECT qservTest_case03_qserv.Science_Ccd_Exposure_To_Htm10_coadd_r.scienceCcdExposureId FROM qservTest_case03_qserv.Science_Ccd_Exposure_To_Htm10_coadd_r;
SELECT qservTest_case03_mysql.Science_Ccd_Exposure_To_Htm10_coadd_r.scienceCcdExposureId FROM qservTest_case03_mysql.Science_Ccd_Exposure_To_Htm10_coadd_r; -- noQserv
