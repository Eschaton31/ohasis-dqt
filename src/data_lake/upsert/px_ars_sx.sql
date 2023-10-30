SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       MAX(IF(ars.ARS_SYMPTOM = 1, IS_SYMPTOM, NULL))                                                  AS ARS_SX_FEVER,
       MAX(IF(ars.ARS_SYMPTOM = 2, IS_SYMPTOM, NULL))                                                  AS ARS_SX_SORE_THROAT,
       MAX(IF(ars.ARS_SYMPTOM = 3, IS_SYMPTOM, NULL))                                                  AS ARS_SX_DIARRHEA,
       MAX(IF(ars.ARS_SYMPTOM = 4, IS_SYMPTOM, NULL))                                                  AS ARS_SX_SWOLLEN_LYMPH,
       MAX(IF(ars.ARS_SYMPTOM = 5, IS_SYMPTOM, NULL))                                                  AS ARS_SX_SWOLLEN_TONSILS,
       MAX(IF(ars.ARS_SYMPTOM = 6, IS_SYMPTOM, NULL))                                                  AS ARS_SX_RASH,
       MAX(IF(ars.ARS_SYMPTOM = 7, IS_SYMPTOM, NULL))                                                  AS ARS_SX_MUSCLE_PAINS,
       MAX(IF(ars.ARS_SYMPTOM = 9999, IS_SYMPTOM, NULL))                                               AS ARS_SX_NONE,
       MAX(IF(ars.ARS_SYMPTOM = 8888, IS_SYMPTOM, NULL))                                               AS ARS_SX_OTHER,
       MAX(IF(ars.ARS_SYMPTOM = 8888, SYMPTOM_OTHER, NULL))                                            AS ARS_SX_OTHER_TEXT
FROM ohasis_interim.px_record AS rec
         JOIN (SELECT REC_ID,
                      ARS_SYMPTOM,
                      CASE IS_SYMPTOM
                          WHEN 1 THEN '1_Yes'
                          WHEN 0 THEN '0_No'
                          ELSE NULL END AS IS_SYMPTOM,
                      SYMPTOM_OTHER
               FROM ohasis_interim.px_ars_sx) AS ars ON rec.REC_ID = ars.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
