SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,
       CASE cfbs.SCREEN_AGREED
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           END                                                                                         AS SCREEN_AGREED,
       CASE cfbs.SCREEN_REFER
           WHEN 1 THEN '1_Client was accompanied to receiving facility'
           WHEN 0 THEN '0_Client opted to return some other time'
           END                                                                                         AS SCREEN_REFER,
       cfbs.PARTNER_FACI                                                                               AS PARTNER_REFERRAL_FACI
FROM ohasis_interim.px_record AS rec
         JOIN ohasis_interim.px_cfbs AS cfbs on rec.REC_ID = cfbs.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?));
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
