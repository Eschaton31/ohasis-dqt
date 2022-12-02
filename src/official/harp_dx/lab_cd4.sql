SELECT IF(id_registry.CENTRAL_ID IS NULL, cd4_data.PATIENT_ID, id_registry.CENTRAL_ID) AS CENTRAL_ID,
       cd4_data.CD4_DATE,
       cd4_data.CD4_RESULT
FROM ohasis_lake.lab_cd4 AS cd4_data
         LEFT JOIN ohasis_warehouse.id_registry ON cd4_data.PATIENT_ID = id_registry.PATIENT_ID
WHERE cd4_data.DELETED_AT IS NULL AND CD4_DATE < ?;