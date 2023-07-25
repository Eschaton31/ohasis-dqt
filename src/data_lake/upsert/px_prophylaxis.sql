SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       MAX(IF(proph.PROPHYLAXIS = 1, IS_PROPH, NULL))                                                  AS PROPH_BCG_DONE,
       MAX(IF(proph.PROPHYLAXIS = 1, PROPH_DATE, NULL))                                                AS PROPH_BCG_DATE,
       MAX(IF(proph.PROPHYLAXIS = 2, IS_PROPH, NULL))                                                  AS PROPH_COTRI_DONE,
       MAX(IF(proph.PROPHYLAXIS = 2, PROPH_DATE, NULL))                                                AS PROPH_COTRI_DATE,
       MAX(IF(proph.PROPHYLAXIS = 3, IS_PROPH, NULL))                                                  AS PROPH_AZITHRO_DONE,
       MAX(IF(proph.PROPHYLAXIS = 3, PROPH_DATE, NULL))                                                AS PROPH_AZITHRO_DATE,
       MAX(IF(proph.PROPHYLAXIS = 4, IS_PROPH, NULL))                                                  AS PROPH_FLUCANO_DONE,
       MAX(IF(proph.PROPHYLAXIS = 4, PROPH_DATE, NULL))                                                AS PROPH_FLUCANO_DATE
FROM ohasis_interim.px_record AS rec
         JOIN (SELECT REC_ID,
                      PROPHYLAXIS,
                      CASE IS_PROPH
                          WHEN 1 THEN '1_Yes'
                          WHEN 0 THEN '0_No'
                          ELSE NULL END AS IS_PROPH,
                      PROPH_DATE
               FROM ohasis_interim.px_prophylaxis) AS proph ON rec.REC_ID = proph.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
