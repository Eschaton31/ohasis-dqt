SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       MAX(IF(oi.OI = '101000', IS_OI, NULL))                                                          AS OI_HIV,
       MAX(IF(oi.OI = '102000', IS_OI, NULL))                                                          AS OI_HEPB,
       MAX(IF(oi.OI = '103000', IS_OI, NULL))                                                          AS OI_HEPC,
       MAX(IF(oi.OI = '104000', IS_OI, NULL))                                                          AS OI_SYPH,
       MAX(IF(oi.OI = '111000', IS_OI, NULL))                                                          AS OI_PCP,
       MAX(IF(oi.OI = '112000', IS_OI, NULL))                                                          AS OI_CMV,
       MAX(IF(oi.OI = '113000', IS_OI, NULL))                                                          AS OI_OROCAND,
       MAX(IF(oi.OI = '117000', IS_OI, NULL))                                                          AS OI_HERPES,
       MAX(IF(oi.OI = '202000', IS_OI, NULL))                                                          AS OI_TB,
       MAX(IF(oi.OI = '115000', IS_OI, NULL))                                                          AS OI_MENINGITIS,
       MAX(IF(oi.OI = '116000', IS_OI, NULL))                                                          AS OI_TOXOPLASMOSIS,
       MAX(IF(oi.OI = '201000', IS_OI, NULL))                                                          AS OI_COVID19,
       MAX(IF(oi.OI = '8888', IS_OI, NULL))                                                            AS OI_OTHER,
       MAX(IF(oi.OI = '8888', OI_OTHER, NULL))                                                         AS OI_OTHER_TEXT
FROM ohasis_interim.px_record AS rec
         JOIN (SELECT REC_ID,
                      OI,
                      CASE IS_OI
                          WHEN 1 THEN '1_Yes'
                          WHEN 0 THEN '0_No'
                          ELSE NULL END AS IS_OI,
                      OI_OTHER
               FROM ohasis_interim.px_oi) AS oi ON rec.REC_ID = oi.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
