SELECT IF(id_registry.CENTRAL_ID IS NULL, hts_data.PATIENT_ID, id_registry.CENTRAL_ID) AS CENTRAL_ID,
       hts_data.*
FROM ohasis_warehouse.form_a AS hts_data
         LEFT JOIN ohasis_warehouse.id_registry ON hts_data.PATIENT_ID = id_registry.PATIENT_ID
WHERE (hts_data.T0_RESULT LIKE '1%' OR hts_data.T0_RESULT IS NULL OR hts_data.REC_ID IN (SELECT REC_ID FROM ohasis_warehouse.dx_new))
  AND COALESCE(id_registry.CENTRAL_ID, hts_data.PATIENT_ID) IN (SELECT CENTRAL_ID FROM ohasis_warehouse.dx_new)
  AND hts_data.FORM_VERSION IS NOT NULL;
