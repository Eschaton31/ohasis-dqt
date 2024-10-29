SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       MAX(IF(sti.STI = '104000' AND sti.TEST_TYPE = 41, test.DATE_PERFORM, NULL)) AS SYPH_RPR_DATE,
       MAX(CASE
               WHEN sti.STI = '104000' AND sti.TEST_TYPE = 41 AND sti.FINAL_RESULT = '10' THEN '1_Reactive'
               WHEN sti.STI = '104000' AND sti.TEST_TYPE = 41 AND sti.FINAL_RESULT = '20' THEN '2_Non-reactive'
               ELSE NULL END)                                                      AS SYPH_RPR_RESULT,
       MAX(IF(sti.STI = '104000' AND sti.TEST_TYPE = 41, sti.RESULT_OTHER, NULL))  AS SYPH_RPR_QUANTI,

       MAX(IF(sti.STI = '104000' AND sti.TEST_TYPE = 42, test.DATE_PERFORM, NULL)) AS SYPH_TPPATPHA_DATE,
       MAX(CASE
               WHEN sti.STI = '104000' AND sti.TEST_TYPE = 42 AND sti.FINAL_RESULT = '10' THEN '1_Reactive'
               WHEN sti.STI = '104000' AND sti.TEST_TYPE = 42 AND sti.FINAL_RESULT = '20' THEN '2_Non-reactive'
               ELSE NULL END)                                                      AS SYPH_TPPATPHA_RESULT,
       MAX(IF(sti.STI = '104000' AND sti.TEST_TYPE = 42, sti.RESULT_OTHER, NULL))  AS SYPH_TPPATPHA_QUANTI,

       MAX(IF(sti.STI = '104000' AND sti.TEST_TYPE = 8888, sti.TEST_NAME, NULL))   AS SYPH_OTHER_NAME,
       MAX(IF(sti.STI = '104000' AND sti.TEST_TYPE = 8888, test.DATE_PERFORM,
              NULL))                                                               AS SYPH_OTHER_DATE,
       MAX(CASE
               WHEN sti.STI = '104000' AND sti.TEST_TYPE = 8888 AND sti.FINAL_RESULT = '10' THEN '1_Reactive'
               WHEN sti.STI = '104000' AND sti.TEST_TYPE = 8888 AND sti.FINAL_RESULT = '20' THEN '2_Non-reactive'
               ELSE NULL END)                                                      AS SYPH_OTHER_RESULT,
       MAX(IF(sti.STI = '104000' AND sti.TEST_TYPE = 8888, sti.RESULT_OTHER,
              NULL))                                                               AS SYPH_OTHER_QUANTI,

       MAX(IF(sti.STI = '105000' AND sti.TEST_TYPE = 51 AND sti.TEST_NUM = 1, test.DATE_PERFORM,
              NULL))                                                               AS GONO_GRAM_STAIN_DATE,
       MAX(CASE
               WHEN sti.STI = '105000' AND sti.TEST_TYPE = 51 AND sti.TEST_NUM = 1 AND sti.FINAL_RESULT = '0' THEN '0'
               WHEN sti.STI = '105000' AND sti.TEST_TYPE = 51 AND sti.TEST_NUM = 1 AND sti.FINAL_RESULT = '1' THEN '1+'
               WHEN sti.STI = '105000' AND sti.TEST_TYPE = 51 AND sti.TEST_NUM = 1 AND sti.FINAL_RESULT = '2' THEN '2+'
               WHEN sti.STI = '105000' AND sti.TEST_TYPE = 51 AND sti.TEST_NUM = 1 AND sti.FINAL_RESULT = '3' THEN '3+'
               WHEN sti.STI = '105000' AND sti.TEST_TYPE = 51 AND sti.TEST_NUM = 1 AND sti.FINAL_RESULT = '4' THEN '>3'
               ELSE NULL END)                                                      AS GONO_GRAM_PUS,
       MAX(CASE
               WHEN sti.STI = '105000' AND sti.TEST_TYPE = 51 AND sti.TEST_NUM = 2 AND sti.FINAL_RESULT = '0'
                   THEN '0_Absence'
               WHEN sti.STI = '105000' AND sti.TEST_TYPE = 51 AND sti.TEST_NUM = 2 AND sti.FINAL_RESULT = '1'
                   THEN '1_Presence'
               ELSE NULL END)                                                      AS GONO_GRAM_NEG_INTRA,
       MAX(CASE
               WHEN sti.STI = '105000' AND sti.TEST_TYPE = 51 AND sti.TEST_NUM = 3 AND sti.FINAL_RESULT = '0'
                   THEN '0_Absence'
               WHEN sti.STI = '105000' AND sti.TEST_TYPE = 51 AND sti.TEST_NUM = 3 AND sti.FINAL_RESULT = '1'
                   THEN '1_Presence'
               ELSE NULL END)                                                      AS GONO_GRAM_NEG_EXTRA,

       MAX(IF(sti.STI = '107000' AND sti.TEST_TYPE = 101, test.DATE_PERFORM,
              NULL))                                                               AS BACVAG_GRAM_STAIN_DATE,
       MAX(CASE
               WHEN sti.STI = '107000' AND sti.TEST_TYPE = 101 AND sti.FINAL_RESULT = '10' THEN '1_Positive'
               WHEN sti.STI = '107000' AND sti.TEST_TYPE = 101 AND sti.FINAL_RESULT = '20' THEN '2_Negative'
               ELSE NULL END)                                                      AS BACVAG_GRAM_STAIN_RESULT,
       MAX(IF(sti.STI = '107000' AND sti.TEST_TYPE = 101, sti.RESULT_OTHER, NULL)) AS BACVAG_GRAM_STAIN_QUANTI,

       MAX(IF(sti.STI = '107000' AND sti.TEST_TYPE = 8888, sti.TEST_NAME, NULL))   AS BACVAG_OTHER_NAME,
       MAX(IF(sti.STI = '107000' AND sti.TEST_TYPE = 8888, test.DATE_PERFORM,
              NULL))                                                               AS BACVAG_OTHER_DATE,
       MAX(CASE
               WHEN sti.STI = '107000' AND sti.TEST_TYPE = 8888 AND sti.FINAL_RESULT = '10' THEN '1_Positive'
               WHEN sti.STI = '107000' AND sti.TEST_TYPE = 8888 AND sti.FINAL_RESULT = '20' THEN '2_Negative'
               ELSE NULL END)                                                      AS BACVAG_OTHER_RESULT,
       MAX(IF(sti.STI = '107000' AND sti.TEST_TYPE = 8888, sti.RESULT_OTHER,
              NULL))                                                               AS BACVAG_OTHER_QUANTI,

       MAX(IF(sti.STI = '118000' AND sti.TEST_TYPE = 181, test.DATE_PERFORM,
              NULL))                                                               AS TRICHO_WET_MOUNT_DATE,
       MAX(CASE
               WHEN sti.STI = '118000' AND sti.TEST_TYPE = 181 AND sti.FINAL_RESULT = '10' THEN '1_Positive'
               WHEN sti.STI = '118000' AND sti.TEST_TYPE = 181 AND sti.FINAL_RESULT = '20' THEN '2_Negative'
               ELSE NULL END)                                                      AS TRICHO_WET_MOUNT_RESULT,
       MAX(IF(sti.STI = '118000' AND sti.TEST_TYPE = 181, sti.RESULT_OTHER, NULL)) AS TRICHO_WET_MOUNT_QUANTI,

       MAX(IF(sti.STI = '118000' AND sti.TEST_TYPE = 8888, sti.TEST_NAME, NULL))   AS TRICHO_OTHER_NAME,
       MAX(IF(sti.STI = '118000' AND sti.TEST_TYPE = 8888, test.DATE_PERFORM,
              NULL))                                                               AS TRICHO_OTHER_DATE,
       MAX(CASE
               WHEN sti.STI = '118000' AND sti.TEST_TYPE = 8888 AND sti.FINAL_RESULT = '10' THEN '1_Positive'
               WHEN sti.STI = '118000' AND sti.TEST_TYPE = 8888 AND sti.FINAL_RESULT = '20' THEN '2_Negative'
               ELSE NULL END)                                                      AS TRICHO_OTHER_RESULT,
       MAX(IF(sti.STI = '118000' AND sti.TEST_TYPE = 8888, sti.RESULT_OTHER,
              NULL))                                                               AS TRICHO_OTHER_QUANTI,

       MAX(IF(sti.STI = '101000' AND sti.TEST_TYPE = 10, test.DATE_PERFORM,
              NULL))                                                               AS HIV_TEST_DATE,
       MAX(CASE
               WHEN sti.STI = '101000' AND sti.TEST_TYPE = 10 AND sti.FINAL_RESULT = '10' THEN '1_Reactive'
               WHEN sti.STI = '101000' AND sti.TEST_TYPE = 10 AND sti.FINAL_RESULT = '20' THEN '2_Non-reactive'
               ELSE NULL END)                                                      AS HIV_TEST_RESULT,
       MAX(IF(sti.STI = '101000' AND sti.TEST_TYPE = 10, sti.RESULT_OTHER, NULL)) AS HIV_TEST_QUANTI,

       MAX(IF(sti.STI = '102000' AND sti.TEST_TYPE = 60, test.DATE_PERFORM,
              NULL))                                                               AS HEPB_TEST_DATE,
       MAX(CASE
               WHEN sti.STI = '102000' AND sti.TEST_TYPE = 60 AND sti.FINAL_RESULT = '10' THEN '1_Reactive'
               WHEN sti.STI = '102000' AND sti.TEST_TYPE = 60 AND sti.FINAL_RESULT = '20' THEN '2_Non-reactive'
               ELSE NULL END)                                                      AS HEPB_TEST_RESULT,
       MAX(IF(sti.STI = '102000' AND sti.TEST_TYPE = 60, sti.RESULT_OTHER, NULL)) AS HEPB_TEST_QUANTI,

       MAX(IF(sti.STI = '103000' AND sti.TEST_TYPE = 70, test.DATE_PERFORM,
              NULL))                                                               AS HEPC_TEST_DATE,
       MAX(CASE
               WHEN sti.STI = '103000' AND sti.TEST_TYPE = 70 AND sti.FINAL_RESULT = '10' THEN '1_Reactive'
               WHEN sti.STI = '103000' AND sti.TEST_TYPE = 70 AND sti.FINAL_RESULT = '20' THEN '2_Non-reactive'
               ELSE NULL END)                                                      AS HEPC_TEST_RESULT,
       MAX(IF(sti.STI = '103000' AND sti.TEST_TYPE = 70, sti.RESULT_OTHER, NULL)) AS HEPC_TEST_QUANTI
FROM ohasis_interim.px_record AS rec
    JOIN ohasis_interim.px_test AS test
ON rec.REC_ID = test.REC_ID
    JOIN ohasis_interim.px_test_sti AS sti
    ON rec.REC_ID = sti.REC_ID AND test.TEST_TYPE = sti.TEST_TYPE AND test.TEST_NUM = sti.TEST_NUM
WHERE ((rec.CREATED_AT BETWEEN ?
  AND ?)
   OR
    (rec.UPDATED_AT BETWEEN ?
  AND ?)
   OR
    (rec.DELETED_AT BETWEEN ?
  AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
