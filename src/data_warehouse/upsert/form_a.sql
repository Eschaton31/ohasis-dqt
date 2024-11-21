SELECT pii.REC_ID,
       pii.PATIENT_ID,
       pii.FORM_VERSION,
       pii.CREATED_BY,
       pii.CREATED_AT,
       pii.UPDATED_BY,
       pii.UPDATED_AT,
       pii.DELETED_BY,
       pii.DELETED_AT,
       pii.SNAPSHOT,
       pii.FACI_ID,
       COALESCE(pii.SUB_FACI_ID, '') AS SUB_FACI_ID,
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
       pii.AGE,
       pii.AGE_MO,
       pii.GENDER_AFFIRM_THERAPY,
       CASE LEFT(pii.SELF_IDENT, 1)
           WHEN '1' THEN '1_Man'
           WHEN '2' THEN '2_Woman'
           WHEN '3' THEN '3_Other'
           END                       AS SELF_IDENT,
       pii.SELF_IDENT_OTHER,
       pii.NATIONALITY,
       pii.EDUC_LEVEL,
       pii.CIVIL_STATUS,
       pii.LIVING_WITH_PARTNER,
       pii.CHILDREN,
       pii.CURR_PSGC_REG,
       pii.CURR_PSGC_PROV,
       pii.CURR_PSGC_MUNC,
       pii.CURR_ADDR,
       pii.PERM_PSGC_REG,
       pii.PERM_PSGC_PROV,
       pii.PERM_PSGC_MUNC,
       pii.PERM_ADDR,
       pii.BIRTH_PSGC_REG,
       pii.BIRTH_PSGC_PROV,
       pii.BIRTH_PSGC_MUNC,
       pii.BIRTH_ADDR,
       ob.IS_PREGNANT,
       test_hiv.CONFIRM_FACI,
       IF(LEFT(test_hiv.CONFIRM_SUB_FACI, 6) = test_hiv.CONFIRM_FACI, test_hiv.CONFIRM_SUB_FACI,
          '')                        AS CONFIRM_SUB_FACI,
       test_hiv.CONFIRM_TYPE,
       test_hiv.CONFIRM_CODE,
       test_hiv.SPECIMEN_REFER_TYPE,
       test_hiv.SPECIMEN_SOURCE,
       IF(LEFT(test_hiv.SPECIMEN_SUB_SOURCE, 6) = test_hiv.SPECIMEN_SOURCE, test_hiv.SPECIMEN_SUB_SOURCE,
          '')                        AS SPECIMEN_SUB_SOURCE,
       test_hiv.CONFIRM_RESULT,
       test_hiv.SIGNATORY_1,
       test_hiv.SIGNATORY_2,
       test_hiv.SIGNATORY_3,
       test_hiv.DATE_RELEASE,
       test_hiv.DATE_CONFIRM,
       test_hiv.IDNUM,
       work.WORK,
       work.IS_EMPLOYED,
       work.IS_STUDENT,
       work.IS_OFW,
       ofw.OFW_YR_RET,
       ofw.OFW_STATION,
       ofw.OFW_COUNTRY,
       CASE
           WHEN risk.RISK_HIV_MOTHER LIKE '%Yes%' THEN '1_Yes'
           WHEN risk.RISK_HIV_MOTHER LIKE '%No%' THEN '0_No'
           ELSE NULL END             AS EXPOSE_HIV_MOTHER,
       risk.RISK_SEX_M_AV_NOCONDOM   AS EXPOSE_SEX_M_NOCONDOM,
       risk.RISK_SEX_F_AV_NOCONDOM   AS EXPOSE_SEX_F_NOCONDOM,
       risk.RISK_SEX_HIV             AS EXPOSE_SEX_HIV,
       risk.RISK_SEX_PAYING          AS EXPOSE_SEX_PAYING,
       risk.RISK_SEX_PAYMENT         AS EXPOSE_SEX_PAYMENT,
       risk.RISK_DRUG_INJECT         AS EXPOSE_DRUG_INJECT,
       risk.RISK_BLOOD_TRANSFUSE     AS EXPOSE_BLOOD_TRANSFUSE,
       risk.RISK_OCCUPATION          AS EXPOSE_OCCUPATION,
       risk.RISK_TATTOO              AS EXPOSE_TATTOO,
       risk.RISK_STI                 AS EXPOSE_STI,
       risk_profile.AGE_FIRST_SEX,
       risk_profile.AGE_FIRST_INJECT,
       risk_profile.NUM_M_PARTNER,
       risk_profile.YR_LAST_M,
       risk_profile.NUM_F_PARTNER,
       risk_profile.YR_LAST_F,
       med_profile.MED_TB_PX,
       med_profile.MED_IS_PREGNANT,
       med_profile.MED_HEP_B,
       med_profile.MED_HEP_C,
       med_profile.MED_CBS_REACTIVE,
       med_profile.MED_PREP_PX,
       test_reason.TEST_REASON_HIV_EXPOSE,
       test_reason.TEST_REASON_PHYSICIAN,
       test_reason.TEST_REASON_RETEST,
       test_reason.TEST_REASON_EMPLOY_OFW,
       test_reason.TEST_REASON_EMPLOY_LOCAL,
       test_reason.TEST_REASON_INSURANCE,
       test_reason.TEST_REASON_NO_REASON,
       test_reason.TEST_REASON_OTHER_TEXT,
       test_previous.PREV_TESTED,
       test_previous.PREV_TEST_DATE,
       test_previous.PREV_TEST_FACI,
       test_previous.PREV_TEST_RESULT,
       staging.CLINICAL_PIC,
       staging.SYMPTOMS,
       staging.WHO_CLASS,
       service.CLIENT_TYPE,
       test_hiv.T0_DATE,
       test_hiv.T0_RESULT,
       CASE
           WHEN service.SERVICE_FACI IS NULL AND test_hiv.SPECIMEN_SOURCE IS NOT NULL THEN test_hiv.SPECIMEN_SOURCE
           WHEN service.SERVICE_FACI IS NOT NULL THEN service.SERVICE_FACI
           ELSE COALESCE(pii.FACI_ID, '')
           END                       AS SERVICE_FACI,
       CASE
           WHEN service.SERVICE_FACI IS NULL AND LEFT(test_hiv.SPECIMEN_SUB_SOURCE, 6) = test_hiv.SPECIMEN_SOURCE
               THEN test_hiv.SPECIMEN_SUB_SOURCE
           WHEN service.SERVICE_FACI IS NULL AND LEFT(test_hiv.SPECIMEN_SUB_SOURCE, 6) <> test_hiv.SPECIMEN_SOURCE
               THEN ''
           WHEN service.SERVICE_FACI IS NOT NULL AND LEFT(service.SERVICE_SUB_FACI, 6) = service.SERVICE_FACI
               THEN service.SERVICE_SUB_FACI
           WHEN service.SERVICE_FACI IS NOT NULL AND LEFT(service.SERVICE_SUB_FACI, 6) <> service.SERVICE_FACI THEN ''
           ELSE COALESCE(pii.SUB_FACI_ID, '')
           END                       AS SERVICE_SUB_FACI,
       service.SERVICE_BY,
       service.REFER_TYPE,
       service.CLINIC_NOTES,
       service.COUNSEL_NOTES,
       test_hiv.T1_DATE,
       test_hiv.T1_KIT,
       test_hiv.T1_RESULT,
       test_hiv.T2_DATE,
       test_hiv.T2_KIT,
       test_hiv.T2_RESULT,
       test_hiv.T3_DATE,
       test_hiv.T3_KIT,
       test_hiv.T3_RESULT,
       test_hiv.DATE_COLLECT,
       test_hiv.DATE_RECEIVE,
       CASE
           WHEN pii.SEX = '1_Male' AND risk.RISK_SEX_M_AV_NOCONDOM LIKE '%Yes%' THEN 1
           WHEN pii.SEX = '1_Male' AND risk_profile.NUM_M_PARTNER > 0 THEN 1
           WHEN pii.SEX = '1_Male' AND risk_profile.YR_LAST_M IS NOT NULL THEN 1
           ELSE 0
           END                       AS FORMA_MSM,
       CASE
           WHEN pii.SEX = '1_Male' AND LEFT(pii.SELF_IDENT, 1) = '2' THEN 1
           WHEN pii.SEX = '1_Male' AND LEFT(pii.SELF_IDENT, 1) = '3' THEN 1
           ELSE 0
           END                       AS FORMA_TGW
FROM ohasis_lake.px_pii AS pii
         LEFT JOIN ohasis_lake.px_faci_info AS service ON pii.REC_ID = service.REC_ID
         LEFT JOIN ohasis_lake.px_ob AS ob ON pii.REC_ID = ob.REC_ID
         LEFT JOIN ohasis_lake.px_hiv_testing AS test_hiv ON pii.REC_ID = test_hiv.REC_ID
         LEFT JOIN ohasis_lake.px_occupation AS work ON pii.REC_ID = work.REC_ID
         LEFT JOIN ohasis_lake.px_ofw AS ofw ON pii.REC_ID = ofw.REC_ID
         LEFT JOIN ohasis_lake.px_risk AS risk ON pii.REC_ID = risk.REC_ID
         LEFT JOIN ohasis_lake.px_expose_profile AS risk_profile ON pii.REC_ID = risk_profile.REC_ID
         LEFT JOIN ohasis_lake.px_test_reason AS test_reason ON pii.REC_ID = test_reason.REC_ID
         LEFT JOIN ohasis_lake.px_test_previous AS test_previous ON pii.REC_ID = test_previous.REC_ID
         LEFT JOIN ohasis_lake.px_med_profile AS med_profile ON pii.REC_ID = med_profile.REC_ID
         LEFT JOIN ohasis_lake.px_staging AS staging ON pii.REC_ID = staging.REC_ID
WHERE pii.DISEASE = 'HIV'
  AND pii.MODULE = '2_Testing'
  AND (FORM_VERSION LIKE 'Form A %' OR pii.FORM_VERSION IS NULL)
  AND ((pii.CREATED_AT BETWEEN ? AND ?) OR
       (pii.UPDATED_AT BETWEEN ? AND ?) OR
       (pii.DELETED_AT BETWEEN ? AND ?));
-- ID_COLS: REC_ID;
-- DELETE: DELETED_AT IS NOT NULL OR FORM_VERSION NOT LIKE 'Form A %';
