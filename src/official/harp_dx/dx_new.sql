SELECT COALESCE(id.CENTRAL_ID, pii.PATIENT_ID) AS CENTRAL_ID,
       pii.PATIENT_ID,
       pii.CREATED_BY,
       pii.UPDATED_BY,
       pii.DELETED_BY,
       pii.FACI_ID,
       pii.SUB_FACI_ID,
       pii.RECORD_DATE,
       pii.DISEASE,
       pii.MODULE,
       pii.PRIME,
       pii.CONFIRMATORY_CODE,
       pii.UIC,
       pii.PHILHEALTH_NO,
       pii.SEX,
       pii.BIRTHDATE,
       pii.PATIENT_CODE,
       pii.PHILSYS_ID,
       pii.FIRST,
       pii.MIDDLE,
       pii.LAST,
       pii.SUFFIX,
       hts.*
FROM ohasis_lake.px_hiv_testing AS hts
         JOIN ohasis_lake.px_pii AS pii ON hts.REC_ID = pii.REC_ID
         LEFT JOIN ohasis_warehouse.id_registry AS id ON pii.PATIENT_ID = id.PATIENT_ID
WHERE (hts.CONFIRM_RESULT LIKE '1%'
    OR hts.CONFIRM_RESULT LIKE '5%')
  AND COALESCE(id.CENTRAL_ID, pii.PATIENT_ID) NOT IN (SELECT CENTRAL_ID FROM ohasis_warehouse.harp_dx_old WHERE CENTRAL_ID IS NOT NULL);
