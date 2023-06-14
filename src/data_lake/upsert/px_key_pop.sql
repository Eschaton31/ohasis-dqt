SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       MAX(IF(kp.KP = 1, IS_KP, NULL))                                                                 AS KP_PDL,
       MAX(IF(kp.KP = 2, IS_KP, NULL))                                                                 AS KP_TG,
       MAX(IF(kp.KP = 3, IS_KP, NULL))                                                                 AS KP_PWID,
       MAX(IF(kp.KP = 5, IS_KP, NULL))                                                                 AS KP_MSM,
       MAX(IF(kp.KP = 6, IS_KP, NULL))                                                                 AS KP_SW,
       MAX(IF(kp.KP = 7, IS_KP, NULL))                                                                 AS KP_OFW,
       MAX(IF(kp.KP = 8, IS_KP, NULL))                                                                 AS KP_PARTNER,
       MAX(IF(kp.KP = 8888, KP_OTHER, NULL))                                                           AS KP_OTHER
FROM ohasis_interim.px_record AS rec
         JOIN (SELECT REC_ID,
                      KP,
                      CASE IS_KP
                          WHEN 1 THEN '1_Yes'
                          WHEN 0 THEN '0_No'
                          ELSE NULL END AS IS_KP,
                      KP_OTHER
               FROM ohasis_interim.px_key_pop) AS kp ON rec.REC_ID = kp.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
