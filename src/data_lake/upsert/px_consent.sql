SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,
       IF(consent.SIGNATURE IS NOT NULL, '1_Yes', NULL)                                                AS SIGNATURE_NAME,
       IF(consent.ESIG IS NOT NULL, '1_Yes', NULL)                                                     AS SIGNATURE_ESIG,
       IF(consent.VERBAL_CONSENT = 1, '1_Yes', NULL)                                                   AS VERBAL_CONSENT
FROM ohasis_interim.px_record AS rec
         JOIN ohasis_interim.px_consent AS consent ON rec.REC_ID = consent.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?));
-- ID_COLS: REC_ID