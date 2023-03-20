SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,
       UPPER(work.WORK_TEXT)                                                                           AS WORK,
       CASE work.IS_EMPLOYED
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           ELSE work.IS_EMPLOYED END                                                                   AS IS_EMPLOYED,
       CASE work.IS_STUDENT
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           ELSE work.IS_STUDENT END                                                                    AS IS_STUDENT,
       CASE work.IS_OFW
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           ELSE work.IS_OFW END                                                                        AS IS_OFW
FROM ohasis_interim.px_record AS rec
         JOIN ohasis_interim.px_occupation AS work on rec.REC_ID = work.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?));
-- ID_COLS: REC_ID