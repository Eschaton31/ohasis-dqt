SELECT *
FROM (SELECT data.CENTRAL_ID,
             data.REC_ID,
             data.RECORD_DATE                                                 AS VISIT_DATE,
             ROW_NUMBER() OVER (PARTITION BY CENTRAL_ID ORDER BY RECORD_DATE) AS VISIT_NUM
      FROM (SELECT COALESCE(id_reg.CENTRAL_ID, hts_data.PATIENT_ID) AS CENTRAL_ID,
                   hts_data.REC_ID,
                   hts_data.RECORD_DATE
            FROM (SELECT REC_ID, RECORD_DATE, PATIENT_ID, MED_PREP_PX, SERVICE_PREP_REFER
                  FROM ohasis_warehouse.form_hts
                  UNION ALL
                  SELECT REC_ID, RECORD_DATE, PATIENT_ID, MED_PREP_PX, NULL AS SERVICE_PREP_REFER
                  FROM ohasis_warehouse.form_a
                  UNION ALL
                  SELECT REC_ID, RECORD_DATE, PATIENT_ID, NULL AS MED_PREP_PX, SERVICE_PREP_REFER
                  FROM ohasis_warehouse.form_cfbs) AS hts_data
                     LEFT JOIN ohasis_warehouse.id_registry AS id_reg ON hts_data.PATIENT_ID = id_reg.PATIENT_ID
            WHERE (hts_data.MED_PREP_PX = '1_Yes' OR hts_data.SERVICE_PREP_REFER = '1_Yes')
              AND RECORD_DATE <= ?) AS data) AS prepstart
WHERE VISIT_NUM = 1;