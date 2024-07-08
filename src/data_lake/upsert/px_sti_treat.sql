SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       MAX(IF(treat.DISEASE = '104000', treat.TREAT_TYPE, NULL))                                       AS SYPH_TREATMENT,
       MAX(IF(treat.DISEASE = '105000', treat.TREAT_TYPE, NULL))                                       AS GONO_TREATMENT,
       MAX(IF(treat.DISEASE = '107000', treat.TREAT_TYPE, NULL))                                       AS BACVAG_TREATMENT,
       MAX(IF(treat.DISEASE = '118000', treat.TREAT_TYPE, NULL))                                       AS TRICHO_TREATMENT
FROM ohasis_interim.px_record AS rec
         JOIN (SELECT REC_ID,
                      DISEASE,
                      CASE TREAT_TYPE
                          WHEN 2 THEN '2_Prescribed Treatment'
                          WHEN 1 THEN '1_Given Treatment'
                          WHEN 0 THEN '0_No Treatment'
                          ELSE NULL END AS TREAT_TYPE,
                      TREAT_OUTCOME,
                      TREAT_OTHER
               FROM ohasis_interim.px_treat) AS treat ON rec.REC_ID = treat.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
