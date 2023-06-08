SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,
       ofw.OFW_YR_RET,
       CASE ofw.OFW_STATION
           WHEN 1 THEN '1_On a ship'
           WHEN 2 THEN '2_Land'
           ELSE ofw.OFW_STATION
           END                                                                                         AS OFW_STATION,
       ofw.OFW_COUNTRY
FROM ohasis_interim.px_record AS rec
         JOIN ohasis_interim.px_ofw AS ofw on rec.REC_ID = ofw.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?));
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
