SELECT IF(id_registry.CENTRAL_ID IS NULL, hts_data.PATIENT_ID, id_registry.CENTRAL_ID) AS CENTRAL_ID,
       hts_data.*
FROM ohasis_warehouse.form_cfbs AS hts_data
         LEFT JOIN ohasis_warehouse.id_registry ON hts_data.PATIENT_ID = id_registry.PATIENT_ID
LIMIT 0;