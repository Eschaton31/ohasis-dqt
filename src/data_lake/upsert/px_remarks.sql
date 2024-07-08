SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       MAX(IF(remarks.REMARK_TYPE = 11, REMARKS, NULL))                                                AS PE_DIAGNOSIS,
       MAX(IF(remarks.REMARK_TYPE = 12, REMARKS, NULL))                                                AS PE_REMARKS
FROM ohasis_interim.px_record AS rec
         JOIN ohasis_interim.px_remarks AS remarks ON rec.REC_ID = remarks.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
  AND remarks.REMARK_TYPE IN (11, 12)
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
