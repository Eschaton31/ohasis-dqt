SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,
       conf.FACI_ID                                                                                    AS CONFIRM_FACI,
       conf.SUB_FACI_ID                                                                                AS CONFIRM_SUB_FACI,
       CASE conf.CONFIRM_TYPE
           WHEN 1 THEN '1_Central NRL'
           WHEN 2 THEN '2_CrCL'
           ELSE conf.CONFIRM_TYPE END                                                                  AS CONFIRM_TYPE,
       conf.CONFIRM_CODE,
       CASE conf.CLIENT_TYPE
           WHEN '1' THEN '1_Inpatient'
           WHEN '2' THEN '2_Walk-in / Outpatient'
           WHEN '3' THEN '3_Mobile HTS Client'
           WHEN '5' THEN '5_Satellite Client'
           WHEN '4' THEN '4_Referral'
           WHEN '6' THEN '6_Transient'
           ELSE conf.CLIENT_TYPE END                                                                   AS SPECIMEN_REFER_TYPE,
       conf.SOURCE                                                                                     AS SPECIMEN_SOURCE,
       conf.SUB_SOURCE                                                                                 AS SPECIMEN_SUB_SOURCE,
       CASE
           WHEN conf.FINAL_RESULT LIKE '%Positive%' THEN '1_Positive'
           WHEN conf.FINAL_RESULT LIKE '%Negative%' THEN '2_Negative'
           WHEN conf.FINAL_RESULT LIKE '%Inconclusive%' THEN '3_Indeterminate'
           WHEN conf.FINAL_RESULT LIKE '%Indeterminate%' THEN '3_Indeterminate'
           WHEN conf.FINAL_RESULT LIKE '%PENDING%' THEN '4_Pending'
           WHEN conf.FINAL_RESULT LIKE '%Duplicate%' THEN '5_Duplicate'
           ELSE conf.FINAL_RESULT END                                                                  AS CONFIRM_RESULT,
       conf.SIGNATORY_1,
       conf.SIGNATORY_2,
       conf.SIGNATORY_3,
       conf.DATE_RELEASE,
       conf.DATE_CONFIRM,
       conf.IDNUM,
       CASE rtri.RT_AGREED
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           ELSE RT_AGREED END                                                                          AS RT_AGREED,
       MAX(IF(test_hiv.TEST_TYPE = 60, test.DATE_PERFORM, NULL))                                       AS RT_DATE,
       MAX(CASE
               WHEN test_hiv.TEST_TYPE = 60 AND test_hiv.KIT_NAME = '1014' THEN 'Asante HIV-1 Rapid Recency Assay'
               WHEN test_hiv.TEST_TYPE = 60 AND test_hiv.KIT_NAME IS NOT NULL THEN test_hiv.KIT_NAME
           END)                                                                                        AS RT_KIT,
       CASE
           WHEN rtri.RT_RESULT LIKE 'Recent%' THEN '1_Recent'
           WHEN rtri.RT_RESULT LIKE 'Long%' THEN '2_Long-term'
           WHEN rtri.RT_RESULT LIKE 'Inconclusive%' THEN '3_Inconclusive'
           WHEN rtri.RT_RESULT LIKE 'Invalid%' THEN '0_Invalid'
           END                                                                                         AS RT_RESULT,
       CASE rtri.VL_REQUESTED
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           ELSE VL_REQUESTED END                                                                       AS RT_VL_REQUESTED,
       CASE rtri.VL_DONE
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           ELSE VL_DONE END                                                                            AS VL_DONE,
       labs.LAB_DATE                                                                                   AS RT_VL_DATE,
       labs.LAB_RESULT                                                                                 AS RT_VL_RESULT,
       CASE
           WHEN rtri.RT_RESULT = 'Recent Infection' AND labs.LAB_RESULT >= 1000 THEN '1_Recent'
           WHEN rtri.RT_RESULT = 'Recent Infection' AND labs.LAB_RESULT < 1000 THEN '2_Long-term'
           WHEN rtri.RT_RESULT = 'Recent Infection' AND labs.LAB_RESULT IS NULL THEN '4_Pending'
           ELSE NULL END                                                                               AS RITA_RESULT,
       MAX(IF(test.TEST_TYPE = 10, test.DATE_PERFORM, NULL))                                           AS T0_DATE,
       MAX(CASE
               WHEN test.TEST_TYPE = 10 AND test.RESULT = 1 THEN '1_Reactive'
               WHEN test.TEST_TYPE = 10 AND test.RESULT = 2 THEN '2_Non-reactive'
               ELSE NULL END)                                                                          AS T0_RESULT,
       MAX(IF(test_hiv.TEST_TYPE = 31, test.DATE_PERFORM, NULL))                                       AS T1_DATE,
       MAX(IF(test_hiv.TEST_TYPE = 31, test_hiv.KIT_NAME, NULL))                                       AS T1_KIT,
       MAX(CASE
               WHEN test_hiv.TEST_TYPE = 31 AND test_hiv.FINAL_RESULT LIKE '1%' THEN '1_Positive / Reactive'
               WHEN test_hiv.TEST_TYPE = 31 AND test_hiv.FINAL_RESULT LIKE '2%' THEN '2_Negative / Non-reactive'
               WHEN test_hiv.TEST_TYPE = 31 AND test_hiv.FINAL_RESULT LIKE '3%' THEN '3_Indeterminate / Inconclusive'
               WHEN test_hiv.TEST_TYPE = 31 AND test_hiv.FINAL_RESULT LIKE '0%' THEN '0_Invalid'
               ELSE NULL END)                                                                          AS T1_RESULT,
       MAX(IF(test_hiv.TEST_TYPE = 32, test.DATE_PERFORM, NULL))                                       AS T2_DATE,
       MAX(IF(test_hiv.TEST_TYPE = 32, test_hiv.KIT_NAME, NULL))                                       AS T2_KIT,
       MAX(CASE
               WHEN test_hiv.TEST_TYPE = 32 AND test_hiv.FINAL_RESULT LIKE '1%' THEN '1_Positive / Reactive'
               WHEN test_hiv.TEST_TYPE = 32 AND test_hiv.FINAL_RESULT LIKE '2%' THEN '2_Negative / Non-reactive'
               WHEN test_hiv.TEST_TYPE = 32 AND test_hiv.FINAL_RESULT LIKE '3%' THEN '3_Indeterminate / Inconclusive'
               WHEN test_hiv.TEST_TYPE = 32 AND test_hiv.FINAL_RESULT LIKE '0%' THEN '0_Invalid'
               ELSE NULL END)                                                                          AS T2_RESULT,
       MAX(IF(test_hiv.TEST_TYPE = 33, test.DATE_PERFORM, NULL))                                       AS T3_DATE,
       MAX(IF(test_hiv.TEST_TYPE = 33, test_hiv.KIT_NAME, NULL))                                       AS T3_KIT,
       MAX(CASE
               WHEN test_hiv.TEST_TYPE = 33 AND test_hiv.FINAL_RESULT LIKE '1%' THEN '1_Positive / Reactive'
               WHEN test_hiv.TEST_TYPE = 33 AND test_hiv.FINAL_RESULT LIKE '2%' THEN '2_Negative / Non-reactive'
               WHEN test_hiv.TEST_TYPE = 33 AND test_hiv.FINAL_RESULT LIKE '3%' THEN '3_Indeterminate / Inconclusive'
               WHEN test_hiv.TEST_TYPE = 33 AND test_hiv.FINAL_RESULT LIKE '0%' THEN '0_Invalid'
               ELSE NULL END)                                                                          AS T3_RESULT
FROM ohasis_interim.px_record AS rec
         LEFT JOIN ohasis_interim.px_test AS test ON rec.REC_ID = test.REC_ID
         LEFT JOIN ohasis_interim.px_confirm AS conf ON rec.REC_ID = conf.REC_ID
         LEFT JOIN ohasis_interim.px_rtri AS rtri ON rec.REC_ID = rtri.REC_ID
         LEFT JOIN ohasis_interim.px_labs AS labs ON rec.REC_ID = labs.REC_ID AND labs.LAB_TEST = 4
         LEFT JOIN ohasis_interim.px_test_hiv AS test_hiv
                   ON test.REC_ID = test_hiv.REC_ID AND test.TEST_TYPE = test_hiv.TEST_TYPE AND
                      test.TEST_NUM = test_hiv.TEST_NUM
WHERE (test.RESULT <> 0 OR conf.FINAL_RESULT IS NOT NULL)
  AND ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID