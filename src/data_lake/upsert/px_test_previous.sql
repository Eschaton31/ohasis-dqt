SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,
       CASE test_previous.PREV_TESTED
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '2_No'
           ELSE NULL
           END                                                                                         AS PREV_TESTED,
       IF(test_previous.PREV_TEST_DATE = '0000-00-00', NULL, PREV_TEST_DATE)                           AS PREV_TEST_DATE,
       UPPER(test_previous.PREV_TEST_FACI)                                                             AS PREV_TEST_FACI,
       CASE test_previous.PREV_TEST_RESULT
           WHEN 1 THEN '1_Positive'
           WHEN 2 THEN '2_Negative'
           WHEN 3 THEN '3_Indeterminate'
           WHEN 4 THEN '4_Was not able to get result'
           ELSE NULL END                                                                               AS PREV_TEST_RESULT
FROM ohasis_interim.px_record AS rec
         JOIN ohasis_interim.px_prev_test AS test_previous on rec.REC_ID = test_previous.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?));
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
