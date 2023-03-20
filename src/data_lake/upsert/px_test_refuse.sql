SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       MAX(IF(test_refuse.REASON = 2002, IS_REASON, NULL))                                             AS TEST_REFUSE_FEAR_MSM,
       MAX(IF(test_refuse.REASON = 3002, IS_REASON, NULL))                                             AS TEST_REFUSE_NO_TIME,
       MAX(IF(test_refuse.REASON = 2003, IS_REASON, NULL))                                             AS TEST_REFUSE_FEAR_RESULT,
       MAX(IF(test_refuse.REASON = 2004, IS_REASON, NULL))                                             AS TEST_REFUSE_FEAR_DISCLOSE,
       MAX(IF(test_refuse.REASON = 3003, IS_REASON, NULL))                                             AS TEST_REFUSE_NO_CURE,
       MAX(IF(test_refuse.REASON = 8888, IS_REASON, NULL))                                             AS TEST_REFUSE_OTHER,
       MAX(IF(test_refuse.REASON = 8888, REASON_OTHER, NULL))                                          AS TEST_REFUSE_OTHER_TEXT
FROM ohasis_interim.px_record AS rec
         JOIN (SELECT REC_ID,
                      REASON,
                      CASE IS_REASON
                          WHEN 1 THEN '1_Yes'
                          WHEN 0 THEN '0_No'
                          ELSE NULL END AS IS_REASON,
                      REASON_OTHER
               FROM ohasis_interim.px_test_refuse) AS test_refuse ON rec.REC_ID = test_refuse.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID