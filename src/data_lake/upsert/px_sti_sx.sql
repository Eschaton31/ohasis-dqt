SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       MAX(IF(sti.STI_SYMPTOM = 1, IS_SYMPTOM, NULL))                                                  AS STI_SX_DISCHARGE_VAGINAL,
       MAX(IF(sti.STI_SYMPTOM = 2, IS_SYMPTOM, NULL))                                                  AS STI_SX_DISCHARGE_ANAL,
       MAX(IF(sti.STI_SYMPTOM = 3, IS_SYMPTOM, NULL))                                                  AS STI_SX_DISCHARGE_URETHRAL,
       MAX(IF(sti.STI_SYMPTOM = 4, IS_SYMPTOM, NULL))                                                  AS STI_SX_SWOLLEN_SCROTUM,
       MAX(IF(sti.STI_SYMPTOM = 5, IS_SYMPTOM, NULL))                                                  AS STI_SX_PAIN_URINE,
       MAX(IF(sti.STI_SYMPTOM = 6, IS_SYMPTOM, NULL))                                                  AS STI_SX_ULCER_GENITAL,
       MAX(IF(sti.STI_SYMPTOM = 7, IS_SYMPTOM, NULL))                                                  AS STI_SX_ULCER_ORAL,
       MAX(IF(sti.STI_SYMPTOM = 8, IS_SYMPTOM, NULL))                                                  AS STI_SX_WARTS_GENITAL,
       MAX(IF(sti.STI_SYMPTOM = 9, IS_SYMPTOM, NULL))                                                  AS STI_SX_PAIN_ABDOMEN,
       MAX(IF(sti.STI_SYMPTOM = 9999, IS_SYMPTOM, NULL))                                               AS STI_SX_NONE,
       MAX(IF(sti.STI_SYMPTOM = 8888, IS_SYMPTOM, NULL))                                               AS STI_SX_OTHER,
       MAX(IF(sti.STI_SYMPTOM = 8888, SYMPTOM_OTHER, NULL))                                            AS STI_SX_OTHER_TEXT
FROM ohasis_interim.px_record AS rec
         JOIN (SELECT REC_ID,
                      STI_SYMPTOM,
                      CASE IS_SYMPTOM
                          WHEN 1 THEN '1_Yes'
                          WHEN 0 THEN '0_No'
                          ELSE NULL END AS IS_SYMPTOM,
                      SYMPTOM_OTHER
               FROM ohasis_interim.px_sti_sx) AS sti ON rec.REC_ID = sti.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
