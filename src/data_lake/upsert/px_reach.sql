SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       MAX(IF(reach.REACH = 1, IS_REACH, NULL))                                                        AS REACH_CLINICAL,
       MAX(IF(reach.REACH = 2, IS_REACH, NULL))                                                        AS REACH_ONLINE,
       MAX(IF(reach.REACH = 3, IS_REACH, NULL))                                                        AS REACH_INDEX_TESTING,
       MAX(IF(reach.REACH = 4, IS_REACH, NULL))                                                        AS REACH_SSNT,
       MAX(IF(reach.REACH = 5, IS_REACH, NULL))                                                        AS REACH_VENUE
FROM ohasis_interim.px_record AS rec
         JOIN (SELECT REC_ID,
                      REACH,
                      CASE IS_REACH
                          WHEN 1 THEN '1_Yes'
                          WHEN 0 THEN '0_No'
                          ELSE NULL END AS IS_REACH,
                      REACH_OTHER
               FROM ohasis_interim.px_reach) AS reach ON rec.REC_ID = reach.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID