SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,
       CASE staging.CLINICAL_PIC
           WHEN 1 THEN '1_Asymptomatic'
           WHEN 2 THEN '2_Symptomatic'
           END                                                                                         AS CLINICAL_PIC,
       CASE staging.WHO_CLASS
           WHEN 1 THEN '1_I'
           WHEN 2 THEN '2_II'
           WHEN 3 THEN '3_III'
           WHEN 4 THEN '4_IV'
           END                                                                                         AS WHO_CLASS,
       UPPER(staging.SYMPTOMS)                                                                         AS SYMPTOMS
FROM ohasis_interim.px_record AS rec
         JOIN ohasis_interim.px_staging AS staging on rec.REC_ID = staging.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?));
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
