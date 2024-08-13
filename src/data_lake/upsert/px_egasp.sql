SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

       NULLIF(egasp.DATE_COLLECT, '0000-00-00 00:00:00')               AS DATE_COLLECT,
       IF(egasp.SPECIMEN_TYPE REGEXP '[[:<:]]1[[:>:]]', '1_Yes', NULL) AS SPECIMEN_MALE_UREHTRA,
       IF(egasp.SPECIMEN_TYPE REGEXP '[[:<:]]2[[:>:]]', '1_Yes', NULL) AS SPECIMEN_FEMALE_CERVICAL,
       IF(egasp.SPECIMEN_TYPE REGEXP '[[:<:]]3[[:>:]]', '1_Yes', NULL) AS SPECIMEN_PHARYNX,
       IF(egasp.SPECIMEN_TYPE REGEXP '[[:<:]]4[[:>:]]', '1_Yes', NULL) AS SPECIMEN_RECTUM,
       egasp.SPECIMEN_TYPE_OTHER,
       CASE egasp.IS_SYNDROMIC
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           ELSE NULL END                                               AS IS_SYNDROMIC,
       egasp.SYNDROMIC_ASSESSMENT
FROM ohasis_interim.px_record AS rec
         JOIN ohasis_interim.px_egasp AS egasp on rec.REC_ID = egasp.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?));
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
