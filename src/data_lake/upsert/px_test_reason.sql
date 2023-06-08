SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       MAX(IF(test_reason.REASON = 1, IS_REASON, NULL))                                                AS TEST_REASON_HIV_EXPOSE,
       MAX(IF(test_reason.REASON = 2, IS_REASON, NULL))                                                AS TEST_REASON_PHYSICIAN,
       MAX(IF(test_reason.REASON = 3, IS_REASON, NULL))                                                AS TEST_REASON_EMPLOY_OFW,
       MAX(IF(test_reason.REASON = 4, IS_REASON, NULL))                                                AS TEST_REASON_EMPLOY_LOCAL,
       MAX(IF(test_reason.REASON = 5, IS_REASON, NULL))                                                AS TEST_REASON_INSURANCE,
       MAX(IF(test_reason.REASON = 6, IS_REASON, NULL))                                                AS TEST_REASON_NO_REASON,
       MAX(IF(test_reason.REASON = 7, IS_REASON, NULL))                                                AS TEST_REASON_RETEST,
       MAX(IF(test_reason.REASON = 8, IS_REASON, NULL))                                                AS TEST_REASON_PEER_ED,
       MAX(IF(test_reason.REASON = 9, IS_REASON, NULL))                                                AS TEST_REASON_TEXT_EMAIL,
       MAX(IF(test_reason.REASON = 8888, REASON_OTHER, NULL))                                          AS TEST_REASON_OTHER_TEXT
FROM ohasis_interim.px_record AS rec
         JOIN (SELECT REC_ID,
                      REASON,
                      CASE IS_REASON
                          WHEN 1 THEN '1_Yes'
                          WHEN 0 THEN '0_No'
                          ELSE NULL END AS IS_REASON,
                      REASON_OTHER
               FROM ohasis_interim.px_test_reason) AS test_reason ON rec.REC_ID = test_reason.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
