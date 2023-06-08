SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,
       CASE linkage.REFER_ART
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           END                                                                                         AS REFER_ART,
       CASE linkage.REFER_CONFIRM
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           END                                                                                         AS REFER_CONFIRM,
       CASE linkage.REFER_RETEST
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           END                                                                                         AS REFER_RETEST,
       linkage.RETEST_MOS,
       linkage.RETEST_WKS,
       linkage.RETEST_DATE
FROM ohasis_interim.px_record AS rec
         JOIN ohasis_interim.px_linkage AS linkage on rec.REC_ID = linkage.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?));
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
