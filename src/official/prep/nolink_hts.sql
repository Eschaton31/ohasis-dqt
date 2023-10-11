SELECT COALESCE(id_reg.CENTRAL_ID, hts.PATIENT_ID) AS CENTRAL_ID,
       RECORD_DATE                                 AS HTS_DATE,
       REC_ID                                      AS HTS_REC,
       FORM_VERSION AS HTS_FORM
FROM (SELECT REC_ID,
             PATIENT_ID,
             RECORD_DATE,
             T0_RESULT,
             T1_RESULT,
             T2_RESULT,
             T3_RESULT,
             CONFIRM_RESULT,
             FORM_VERSION
      FROM ohasis_warehouse.form_hts
      UNION ALL
      SELECT REC_ID,
             PATIENT_ID,
             RECORD_DATE,
             T0_RESULT,
             T1_RESULT,
             T2_RESULT,
             T3_RESULT,
             CONFIRM_RESULT,
             FORM_VERSION
      FROM ohasis_warehouse.form_a
      UNION ALL
      SELECT REC_ID,
             PATIENT_ID,
             RECORD_DATE,
             TEST_RESULT AS T0_RESULT,
             ''          AS T1_RESULT,
             ''          AS T2_RESULT,
             ''          AS T3_RESULT,
             ''          AS CONFIRM_RESULT,
             FORM_VERSION
      FROM ohasis_warehouse.form_cfbs) AS hts
         LEFT JOIN ohasis_warehouse.id_registry AS id_reg ON hts.PATIENT_ID = id_reg.PATIENT_ID
         LEFT JOIN ohasis_warehouse.rec_link AS link ON hts.REC_ID = link.SOURCE_REC
WHERE link.SOURCE_REC IS NULL
  AND LEFT(COALESCE(NULLIF(NULLIF(CONFIRM_RESULT, '4_Pending'), '5_Duplicate'), T3_RESULT, T2_RESULT, T1_RESULT,
                    T0_RESULT, ''), 1) <> '1';
