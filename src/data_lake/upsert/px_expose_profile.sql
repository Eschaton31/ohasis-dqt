SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,
       expose.AGE_FIRST_SEX,
       expose.NUM_F_PARTNER,
       expose.YR_LAST_F,
       expose.NUM_M_PARTNER,
       expose.YR_LAST_M,
       expose.AGE_FIRST_INJECT,
       CASE expose.WEEK_AVG_SEX
           WHEN '1' THEN '1_<= 1 sex acts a week'
           WHEN '2' THEN '2_>= 2 sex acts a week'
           ELSE expose.WEEK_AVG_SEX END                                                                AS WEEK_AVG_SEX
FROM px_record AS rec
         JOIN px_expose_profile AS expose on rec.REC_ID = expose.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?));
-- ID_COLS: REC_ID