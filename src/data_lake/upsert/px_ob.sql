SELECT rec.REC_ID,
       rec.CREATED_AT,
       rec.UPDATED_AT,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,
       CASE ob.IS_PREGNANT
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           ELSE IS_PREGNANT END                                                                        AS IS_PREGNANT,
       ob.LMP,
       ob.PREG_MOS,
       ob.PREG_WKS,
       ob.EDD,
       ob.DELIVER_DATE,
       ob.DELIVER_FACI,
       CASE ob.DELIVER_FACI_TYPE
           WHEN '8888' THEN '8888_Others'
           WHEN '9999' THEN '9999_No plans yet'
           WHEN '20' THEN '20_Home'
           WHEN '11' THEN '11_Hospital'
           WHEN '12' THEN '12_Lying-in clinic'
           ELSE DELIVER_FACI_TYPE END                                                                  AS DELIVER_FACI_TYPE,
       CASE ob.FEED_TYPE
           WHEN '1' THEN '1_Breasfeeding'
           WHEN '2' THEN '2_Formula feeding'
           WHEN '3' THEN '3_Mixed feeding'
           ELSE NULL END                                                                               AS FEED_TYPE,
       ob.EDD,
       ob.ANC_FACI,
       ob.OB_SCORE
FROM ohasis_interim.px_record AS rec
         JOIN ohasis_interim.px_ob AS ob on rec.REC_ID = ob.REC_ID
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?));
-- ID_COLS: REC_ID