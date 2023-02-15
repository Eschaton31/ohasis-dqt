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
       MAX(IF(test.TEST_TYPE = 10, test.DATE_PERFORM, NULL))                                           AS T0_DATE,
       MAX(CASE
               WHEN test.TEST_TYPE = 10 AND test.RESULT = 1 THEN '1_Reactive'
               WHEN test.TEST_TYPE = 10 AND test.RESULT = 2 THEN '2_Non-reactive'
               ELSE NULL END)                                                                          AS T0_RESULT,
       MAX(IF(test_hiv.TEST_TYPE = 31, test.DATE_PERFORM, NULL))                                       AS T1_DATE,
       MAX(IF(test_hiv.TEST_TYPE = 31, test_hiv.KIT_NAME, NULL))                                       AS T1_KIT,
       MAX(CASE
               WHEN test_hiv.TEST_TYPE = 31 AND test_hiv.RESULT LIKE '1%' THEN '1_Positive / Reactive'
               WHEN test_hiv.TEST_TYPE = 31 AND test_hiv.RESULT LIKE '2%' THEN '2_Negative / Non-reactive'
               WHEN test_hiv.TEST_TYPE = 31 AND test_hiv.RESULT LIKE '3%' THEN '3_Indeterminate / Inconclusive'
               WHEN test_hiv.TEST_TYPE = 31 AND test_hiv.RESULT LIKE '0%' THEN '0_Invalid'
               ELSE NULL END)                                                                          AS T1_RESULT,
       MAX(IF(test_hiv.TEST_TYPE = 32, test.DATE_PERFORM, NULL))                                       AS T2_DATE,
       MAX(IF(test_hiv.TEST_TYPE = 32, test_hiv.KIT_NAME, NULL))                                       AS T2_KIT,
       MAX(CASE
               WHEN test_hiv.TEST_TYPE = 32 AND test_hiv.RESULT LIKE '1%' THEN '1_Positive / Reactive'
               WHEN test_hiv.TEST_TYPE = 32 AND test_hiv.RESULT LIKE '2%' THEN '2_Negative / Non-reactive'
               WHEN test_hiv.TEST_TYPE = 32 AND test_hiv.RESULT LIKE '3%' THEN '3_Indeterminate / Inconclusive'
               WHEN test_hiv.TEST_TYPE = 32 AND test_hiv.RESULT LIKE '0%' THEN '0_Invalid'
               ELSE NULL END)                                                                          AS T2_RESULT,
       MAX(IF(test_hiv.TEST_TYPE = 33, test.DATE_PERFORM, NULL))                                       AS T3_DATE,
       MAX(IF(test_hiv.TEST_TYPE = 33, test_hiv.KIT_NAME, NULL))                                       AS T3_KIT,
       MAX(CASE
               WHEN test_hiv.TEST_TYPE = 33 AND test_hiv.RESULT LIKE '1%' THEN '1_Positive / Reactive'
               WHEN test_hiv.TEST_TYPE = 33 AND test_hiv.RESULT LIKE '2%' THEN '2_Negative / Non-reactive'
               WHEN test_hiv.TEST_TYPE = 33 AND test_hiv.RESULT LIKE '3%' THEN '3_Indeterminate / Inconclusive'
               WHEN test_hiv.TEST_TYPE = 33 AND test_hiv.RESULT LIKE '0%' THEN '0_Invalid'
               ELSE NULL END)                                                                          AS T3_RESULT
FROM px_record AS rec
         LEFT JOIN px_test AS test ON rec.REC_ID = test.REC_ID
         LEFT JOIN px_confirm AS conf ON rec.REC_ID = conf.REC_ID
         LEFT JOIN px_test_hiv AS test_hiv ON test.REC_ID = test_hiv.REC_ID AND test.TEST_TYPE = test_hiv.TEST_TYPE AND
                                              test.TEST_NUM = test_hiv.TEST_NUM
WHERE (test.RESULT <> 0 OR conf.FINAL_RESULT IS NOT NULL)
  AND ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID