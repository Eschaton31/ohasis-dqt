SELECT DISTINCT disc.REC_ID,
                rec.PATIENT_ID,
                rec.CREATED_AT,
                rec.UPDATED_AT,
                rec.DELETED_AT,
                GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0),
                         COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,

                disc.FACI_ID,
                disc.SUB_FACI_ID,
                disc.MEDICINE,
                DATE(disc.DISC_DATE)                  AS DISC_DATE,
                CASE disc.DISC_REASON
                    WHEN 1 THEN '1_Treatment Failure'
                    WHEN 2 THEN '2_Clinical Progression'
                    WHEN 3 THEN '3_Patient Decision/Request'
                    WHEN 4 THEN '4_Compliance Difficulties'
                    WHEN 5 THEN '5_Drug Interaction'
                    WHEN 6 THEN '6_Adverse Event'
                    WHEN 8 THEN '8_Death'
                    WHEN 8888 THEN '8888_Other'
                    END                               AS DISC_REASON,
                disc.DISC_REASON_OTHER
FROM ohasis_interim.px_record AS rec
         JOIN ohasis_interim.px_medicine_disc AS disc ON rec.REC_ID = disc.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?));
-- ID_COLS: REC_ID;
-- DELETED: DELETED_AT IS NOT NULL;
