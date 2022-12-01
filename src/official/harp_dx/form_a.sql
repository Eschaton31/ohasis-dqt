SELECT IF(id_registry.CENTRAL_ID IS NULL, hts_data.PATIENT_ID, id_registry.CENTRAL_ID) AS CENTRAL_ID,
       hts_data.*
FROM ohasis_warehouse.form_a AS hts_data
         LEFT JOIN ohasis_warehouse.id_registry ON hts_data.PATIENT_ID = id_registry.PATIENT_ID
WHERE LEFT(hts_data.CONFIRM_RESULT, 1) = '1'
  AND hts_data.REC_ID NOT IN (SELECT REC_ID FROM ohasis_warehouse.harp_dx_old)
  AND IF(id_registry.CENTRAL_ID IS NULL, hts_data.PATIENT_ID, id_registry.CENTRAL_ID) NOT IN
      (SELECT CENTRAL_ID FROM ohasis_warehouse.harp_dx_old);