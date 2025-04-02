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
       CASE LEFT(pii.SERVICE_TYPE, 6)
           WHEN '101103' THEN 'CBS'
           WHEN '101104' THEN 'FBS'
           END                       AS HIV_SERVICE_TYPE,
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
       pii.NATIONALITY_OTHER,
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
       pii.SERVICE_PSGC_REG          AS HIV_SERVICE_PSGC_REG,
       pii.SERVICE_PSGC_PROV         AS HIV_SERVICE_PSGC_PROV,
       pii.SERVICE_PSGC_MUNC         AS HIV_SERVICE_PSGC_MUNC,
       pii.SERVICE_ADDR              AS HIV_SERVICE_ADDR,
       pii.SERVICE_TYPE              AS MODALITY,
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
       service.CLINIC_NOTES,
       service.COUNSEL_NOTES,
       cfbs.SCREEN_AGREED,
       cfbs.SCREEN_REFER,
       cfbs.PARTNER_REFERRAL_FACI,
       pii.CLIENT_EMAIL,
       pii.CLIENT_MOBILE,
       consent.VERBAL_CONSENT,
       consent.SIGNATURE_ESIG,
       consent.SIGNATURE_NAME,
       risk.RISK_SEX_PAYMENT,
       risk.RISK_SEX_PAYMENT_DATE,
       risk.RISK_SEX_EVER,
       risk.RISK_SEX_HIV,
       risk.RISK_SEX_HIV_DATE,
       risk.RISK_CONDOMLESS_ANAL,
       risk.RISK_CONDOMLESS_ANAL_DATE,
       risk.RISK_CONDOMLESS_VAGINAL,
       risk.RISK_CONDOMLESS_VAGINAL_DATE,
       risk.RISK_SEX_ORAL_ANAL       AS RISK_M_SEX_ORAL_ANAL,
       risk.RISK_DRUG_INJECT         AS RISK_NEEDLE_SHARE,
       risk.RISK_DRUG_INJECT_DATE    AS RISK_NEEDLE_SHARE_DATE,
       risk.RISK_ILLICIT_DRUGS,
       risk.RISK_ILLICIT_DRUGS_DATE,
       risk.RISK_DRUG_INJECT,
       risk_profile.NUM_M_PARTNER,
       risk_profile.NUM_F_PARTNER,
       test_previous.PREV_TESTED,
       test_previous.PREV_TEST_DATE,
       test_previous.PREV_TEST_FACI,
       test_previous.PREV_TEST_RESULT,
       test_hiv.T0_DATE              AS TEST_DATE,
       test_hiv.T0_RESULT            AS TEST_RESULT,
       service_other.SERVICE_RISK_COUNSEL,
       service_other.SERVICE_HIV_101,
       service_other.SERVICE_IEC_MATS,
       service_other.SERVICE_PREP_REFER,
       service_other.SERVICE_GIVEN_CONDOMS,
       service_other.SERVICE_GIVEN_LUBES,
       test_refuse.TEST_REFUSE_NO_TIME,
       test_refuse.TEST_REFUSE_NO_CURE,
       test_refuse.TEST_REFUSE_FEAR_RESULT,
       test_refuse.TEST_REFUSE_FEAR_DISCLOSE,
       test_refuse.TEST_REFUSE_FEAR_MSM,
       test_refuse.TEST_REFUSE_OTHER,
       test_refuse.TEST_REFUSE_OTHER_TEXT,
       CASE
           WHEN pii.SEX = '1_Male' AND risk.RISK_CONDOMLESS_ANAL LIKE '%Yes%' THEN 1
           WHEN pii.SEX = '1_Male' AND risk.RISK_CONDOMLESS_ANAL_DATE IS NOT NULL THEN 1
           ELSE 0
           END                       AS CFBS_MSM,
       CASE
           WHEN pii.SEX = '1_Male' AND LEFT(pii.SELF_IDENT, 1) = '2' THEN 1
           WHEN pii.SEX = '1_Male' AND LEFT(pii.SELF_IDENT, 1) = '3' THEN 1
           ELSE 0
           END                       AS CFBS_TGW,
       NULL                          AS CFBS_PWID,
       NULL                          AS CFBS_FSW,
       NULL                          AS CFBS_GENPOP
FROM ohasis_lake.px_pii AS pii
         LEFT JOIN ohasis_lake.px_faci_info AS service ON pii.REC_ID = service.REC_ID
         LEFT JOIN ohasis_lake.px_hiv_testing AS test_hiv ON pii.REC_ID = test_hiv.REC_ID
         LEFT JOIN ohasis_lake.px_risk AS risk ON pii.REC_ID = risk.REC_ID
         LEFT JOIN ohasis_lake.px_expose_profile AS risk_profile ON pii.REC_ID = risk_profile.REC_ID
         LEFT JOIN ohasis_lake.px_test_previous AS test_previous ON pii.REC_ID = test_previous.REC_ID
         LEFT JOIN ohasis_lake.px_cfbs AS cfbs ON pii.REC_ID = cfbs.REC_ID
         LEFT JOIN ohasis_lake.px_consent AS consent ON pii.REC_ID = consent.REC_ID
         LEFT JOIN ohasis_lake.px_other_service AS service_other ON pii.REC_ID = service_other.REC_ID
         LEFT JOIN ohasis_lake.px_test_refuse AS test_refuse ON pii.REC_ID = test_refuse.REC_ID
WHERE pii.DISEASE = 'HIV'
  AND pii.MODULE = '2_Testing'
  AND pii.FORM_VERSION LIKE 'CFBS Form %'
  AND ((pii.CREATED_AT BETWEEN ? AND ?) OR
       (pii.UPDATED_AT BETWEEN ? AND ?) OR
       (pii.DELETED_AT BETWEEN ? AND ?));
-- ID_COLS: REC_ID;
-- DELETE: DELETED_AT IS NOT NULL OR FORM_VERSION <> 'CFBS Form (v2020)';
