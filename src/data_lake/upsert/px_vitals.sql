SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       MAX(IF(vitals.VITAL_SIGN = 2, VITAL_RESULT, NULL))                                               AS WEIGHT,
       MAX(IF(vitals.VITAL_SIGN = 3, VITAL_RESULT, NULL))                                               AS BODY_TEMP
FROM ohasis_interim.px_record AS rec
         JOIN (SELECT REC_ID,
                      VITAL_SIGN,
                      VITAL_RESULT
               FROM ohasis_interim.px_vitals) AS vitals ON rec.REC_ID = vitals.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
