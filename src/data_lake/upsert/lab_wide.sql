SELECT rec.REC_ID,
       rec.PATIENT_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       -- hbsag
       MAX(IF(lab.LAB_TEST = 1, DATE(lab.LAB_DATE), NULL))                                             AS LAB_HBSAG_DATE,
       MAX(CASE
               WHEN lab.LAB_TEST = 1 AND lab.LAB_RESULT = 1 THEN '1_Reactive'
               WHEN lab.LAB_TEST = 1 AND lab.LAB_RESULT = 2 THEN '2_Non-reactive'
               ELSE NULL
           END)                                                                                        AS LAB_HBSAG_RESULT,

       -- creatinine
       MAX(IF(lab.LAB_TEST = 2, DATE(lab.LAB_DATE), NULL))                                             AS LAB_CREA_DATE,
       MAX(IF(lab.LAB_TEST = 2, lab.LAB_RESULT, NULL))                                                 AS LAB_CREA_RESULT,
       MAX(IF(lab.LAB_TEST = 2, lab.LAB_RESULT_OTHER, NULL))                                           AS LAB_CREA_CLEARANCE,

       -- syphilis
       MAX(IF(lab.LAB_TEST = 3, DATE(lab.LAB_DATE), NULL))                                             AS LAB_SYPH_DATE,
       MAX(CASE
               WHEN lab.LAB_TEST = 3 AND lab.LAB_RESULT = 1 THEN '1_Reactive'
               WHEN lab.LAB_TEST = 3 AND lab.LAB_RESULT = 2 THEN '2_Non-reactive'
               ELSE NULL
           END)                                                                                        AS LAB_SYPH_RESULT,
       MAX(IF(lab.LAB_TEST = 3, lab.LAB_RESULT_OTHER, NULL))                                           AS LAB_SYPH_TITER,

       -- viral load
       MAX(IF(lab.LAB_TEST = 4, DATE(lab.LAB_DATE), NULL))                                             AS LAB_VIRAL_DATE,
       MAX(IF(lab.LAB_TEST = 4, lab.LAB_RESULT, NULL))                                                 AS LAB_VIRAL_RESULT,

       -- cd4
       MAX(IF(lab.LAB_TEST = 5, DATE(lab.LAB_DATE), NULL))                                             AS LAB_CD4_DATE,
       MAX(IF(lab.LAB_TEST = 5, lab.LAB_RESULT, NULL))                                                 AS LAB_CD4_RESULT,

       -- xray
       MAX(IF(lab.LAB_TEST = 6, DATE(lab.LAB_DATE), NULL))                                             AS LAB_XRAY_DATE,
       MAX(IF(lab.LAB_TEST = 6, lab.LAB_RESULT, NULL))                                                 AS LAB_XRAY_RESULT,

       -- xpert
       MAX(IF(lab.LAB_TEST = 7, DATE(lab.LAB_DATE), NULL))                                             AS LAB_XPERT_DATE,
       MAX(IF(lab.LAB_TEST = 7, lab.LAB_RESULT, NULL))                                                 AS LAB_XPERT_RESULT,

       -- dssm
       MAX(IF(lab.LAB_TEST = 8, DATE(lab.LAB_DATE), NULL))                                             AS LAB_DSSM_DATE,
       MAX(IF(lab.LAB_TEST = 8, lab.LAB_RESULT, NULL))                                                 AS LAB_DSSM_RESULT,

       -- hivdr
       MAX(IF(lab.LAB_TEST = 9, DATE(lab.LAB_DATE), NULL))                                             AS LAB_HIVDR_DATE,
       MAX(IF(lab.LAB_TEST = 9, lab.LAB_RESULT, NULL))                                                 AS LAB_HIVDR_RESULT,

       -- hemoglobin
       MAX(IF(lab.LAB_TEST = 10, DATE(lab.LAB_DATE), NULL))                                            AS LAB_HEMOG_DATE,
       MAX(IF(lab.LAB_TEST = 10, lab.LAB_RESULT, NULL))                                                AS LAB_HEMOG_RESULT
FROM ohasis_interim.px_record AS rec
         JOIN ohasis_interim.px_labs AS lab ON rec.REC_ID = lab.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
