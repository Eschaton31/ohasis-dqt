SELECT IF(id_registry.CENTRAL_ID IS NULL, pii.PATIENT_ID, id_registry.CENTRAL_ID) AS CENTRAL_ID,
       COALESCE(hts_data.REC_ID, pii.REC_ID)                                      AS REC_ID,
       pii.PATIENT_ID,
       pii.CREATED_BY,
       COALESCE(hts_data.CREATED_AT, pii.CREATED_AT)                              AS CREATED_AT,
       pii.UPDATED_BY,
       COALESCE(hts_data.UPDATED_AT, pii.UPDATED_AT)                              AS UPDATED_AT,
       pii.DELETED_BY,
       COALESCE(hts_data.DELETED_AT, pii.DELETED_AT)                              AS DELETED_AT,
       COALESCE(hts_data.SNAPSHOT, pii.SNAPSHOT)                                  AS SNAPSHOT,
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
       hts_data.CONFIRM_FACI,
       hts_data.CONFIRM_SUB_FACI,
       hts_data.CONFIRM_TYPE,
       hts_data.CONFIRM_CODE,
       hts_data.SPECIMEN_REFER_TYPE,
       hts_data.SPECIMEN_SOURCE,
       hts_data.SPECIMEN_SUB_SOURCE,
       hts_data.CONFIRM_RESULT,
       hts_data.SIGNATORY_1,
       hts_data.SIGNATORY_2,
       hts_data.SIGNATORY_3,
       hts_data.DATE_RELEASE,
       hts_data.DATE_CONFIRM,
       hts_data.IDNUM,
       hts_data.T0_DATE,
       hts_data.T0_RESULT,
       hts_data.T1_DATE,
       hts_data.T1_KIT,
       hts_data.T1_RESULT,
       hts_data.T2_DATE,
       hts_data.T2_KIT,
       hts_data.T2_RESULT,
       hts_data.T3_DATE,
       hts_data.T3_KIT,
       hts_data.T3_RESULT
FROM ohasis_lake.px_pii AS pii
         JOIN ohasis_lake.px_hiv_testing AS hts_data ON pii.REC_ID = hts_data.REC_ID
         LEFT JOIN ohasis_warehouse.id_registry ON pii.PATIENT_ID = id_registry.PATIENT_ID
WHERE LEFT(hts_data.CONFIRM_RESULT, 1) = '1'
  AND hts_data.REC_ID NOT IN (SELECT REC_ID FROM ohasis_warehouse.harp_dx_old)
  AND IF(id_registry.CENTRAL_ID IS NULL, pii.PATIENT_ID, id_registry.CENTRAL_ID) NOT IN
      (SELECT CENTRAL_ID FROM ohasis_warehouse.harp_dx_old);