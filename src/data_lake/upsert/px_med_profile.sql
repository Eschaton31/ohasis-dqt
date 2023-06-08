SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       MAX(IF(med_profile.PROFILE = 1, IS_PROFILE, NULL))                                              AS MED_TB_PX,
       MAX(IF(med_profile.PROFILE = 2, IS_PROFILE, NULL))                                              AS MED_IS_PREGNANT,
       MAX(IF(med_profile.PROFILE = 3, IS_PROFILE, NULL))                                              AS MED_HEP_B,
       MAX(IF(med_profile.PROFILE = 4, IS_PROFILE, NULL))                                              AS MED_HEP_C,
       MAX(IF(med_profile.PROFILE = 5, IS_PROFILE, NULL))                                              AS MED_CBS_REACTIVE,
       MAX(IF(med_profile.PROFILE = 6, IS_PROFILE, NULL))                                              AS MED_PREP_PX,
       MAX(IF(med_profile.PROFILE = 7, IS_PROFILE, NULL))                                              AS MED_PEP_PX,
       MAX(IF(med_profile.PROFILE = 8, IS_PROFILE, NULL))                                              AS MED_STI
FROM ohasis_interim.px_record AS rec
         JOIN (SELECT REC_ID,
                      PROFILE,
                      CASE IS_PROFILE
                          WHEN 1 THEN '1_Yes'
                          WHEN 0 THEN '0_No'
                          ELSE NULL END AS IS_PROFILE
               FROM ohasis_interim.px_med_profile) AS med_profile ON rec.REC_ID = med_profile.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
