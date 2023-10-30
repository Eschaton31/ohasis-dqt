SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       prep_check.PRE_INIT_HIV_NR,
       prep_check.PRE_INIT_WEIGHT,
       prep_check.PRE_INIT_NO_ARS,
       prep_check.PRE_INIT_CREA_CLEAR,
       prep_check.PRE_INIT_NO_ARV_ALLERGY,
       prep_main.PREP_HIV_DATE,
       CASE
           WHEN prep_main.PREP_VISIT = 1 & prep_main.PREP_ACCEPTED = 1 THEN '1_Accepted PrEP'
           WHEN prep_main.PREP_VISIT = 1 & prep_main.PREP_ACCEPTED = 0 THEN '0_Refused PrEP'
           ELSE NULL
           END                                                                                         AS PREP_STATUS,
       CASE prep_main.PREP_VISIT
           WHEN 1 THEN '1_Screening'
           WHEN 2 THEN '2_Follow-up'
           ELSE NULL END                                                                               AS PREP_VISIT,
       CASE prep_main.ELIGIBLE_BEHAVIOR
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           ELSE NULL END                                                                               AS ELIGIBLE_BEHAVIOR,
       CASE prep_main.ELIGIBLE_PREP
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           ELSE NULL END                                                                               AS ELIGIBLE_PREP,
       CASE prep_main.PREP_REQUESTED
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           ELSE NULL END                                                                               AS PREP_REQUESTED,
       CASE prep_main.PREP_PLAN
           WHEN 1 THEN '1_Clinic-supported'
           WHEN 2 THEN '2_Client-supported'
           WHEN 3 THEN '3_Cost-shared'
           ELSE NULL END                                                                               AS PREP_PLAN,
       CASE prep_main.PREP_TYPE
           WHEN 1 THEN '1_Daily'
           WHEN 2 THEN '2_Event-driven'
           ELSE NULL END                                                                               AS PREP_TYPE,
       CASE prep_main.FIRST_TIME
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           ELSE NULL END                                                                               AS FIRST_TIME,
       CASE
           WHEN prep_main.PREP_VISIT = 2 & prep_main.PREP_ACCEPTED = 1 THEN '1_Continue PrEP'
           WHEN prep_main.PREP_VISIT = 2 & prep_main.PREP_ACCEPTED = 0 THEN '0_Discontinue PrEP'
           ELSE NULL
           END                                                                                         AS PREP_CONTINUED,
       CASE prep_status.PREP_TYPE_LAST_VISIT
           WHEN 1 THEN '1_Daily'
           WHEN 2 THEN '2_Event-driven'
           ELSE NULL END                                                                               AS PREP_TYPE_LAST_VISIT,
       CASE prep_status.PREP_SHIFT
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           ELSE NULL END                                                                               AS PREP_SHIFT,
       CASE prep_status.PREP_MISSED
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           ELSE NULL END                                                                               AS PREP_MISSED,
       CASE prep_status.PREP_SIDE_EFFECTS
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           ELSE NULL END                                                                               AS PREP_SIDE_EFFECTS,
       prep_status.SIDE_EFFECTS                                                                        AS PREP_SIDE_EFFECTS_SPECIFY,
       CASE
           WHEN prep_finance.CAPACITY = 300000 AND CAPACITY_TYPE = 1 THEN '< PHP 30k'
           WHEN prep_finance.CAPACITY = 300000 AND CAPACITY_TYPE = 2 THEN '>= PHP 30k'
           ELSE NULL END                                                                               AS FINANCE_CAPACITY
FROM ohasis_interim.px_record AS rec
         JOIN ohasis_interim.px_prep AS prep_main ON rec.REC_ID = prep_main.REC_ID
         LEFT JOIN ohasis_interim.px_prep_status AS prep_status ON rec.REC_ID = prep_status.REC_ID
         LEFT JOIN ohasis_interim.px_prep_finance AS prep_finance ON rec.REC_ID = prep_finance.REC_ID
         LEFT JOIN (SELECT rec.REC_ID,
                           rec.CREATED_AT,
                           rec.UPDATED_AT,
                           rec.DELETED_AT,
                           GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0),
                                    COALESCE(rec.CREATED_AT, 0))                 AS SNAPSHOT,

                           MAX(IF(prep_check.REQUIREMENT = 1, IS_CHECKED, NULL)) AS PRE_INIT_HIV_NR,
                           MAX(IF(prep_check.REQUIREMENT = 2, IS_CHECKED, NULL)) AS PRE_INIT_WEIGHT,
                           MAX(IF(prep_check.REQUIREMENT = 3, IS_CHECKED, NULL)) AS PRE_INIT_NO_ARS,
                           MAX(IF(prep_check.REQUIREMENT = 4, IS_CHECKED, NULL)) AS PRE_INIT_CREA_CLEAR,
                           MAX(IF(prep_check.REQUIREMENT = 5, IS_CHECKED, NULL)) AS PRE_INIT_NO_ARV_ALLERGY
                    FROM ohasis_interim.px_record AS rec
                             JOIN (SELECT REC_ID,
                                          REQUIREMENT,
                                          CASE IS_CHECKED
                                              WHEN 1 THEN '1_Yes'
                                              WHEN 0 THEN '0_No'
                                              ELSE NULL END AS IS_CHECKED
                                   FROM ohasis_interim.px_prep_checklist) AS prep_check
                                  ON rec.REC_ID = prep_check.REC_ID
                    GROUP BY rec.REC_ID) AS prep_check ON rec.REC_ID = prep_check.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
