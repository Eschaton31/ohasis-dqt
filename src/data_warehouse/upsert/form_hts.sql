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
       pii.AGE,
       pii.AGE_MO,
       pii.GENDER_AFFIRM_THERAPY,
       CASE LEFT(pii.SELF_IDENT, 1)
           WHEN '1' THEN '1_Man'
           WHEN '2' THEN '2_Woman'
           WHEN '3' THEN '3_Other'
           END                             AS SELF_IDENT,
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
       pii.SERVICE_PSGC_REG                AS HIV_SERVICE_PSGC_REG,
       pii.SERVICE_PSGC_PROV               AS HIV_SERVICE_PSGC_PROV,
       pii.SERVICE_PSGC_MUNC               AS HIV_SERVICE_PSGC_MUNC,
       pii.SERVICE_ADDR                    AS HIV_SERVICE_ADDR,
       ob.IS_PREGNANT,
       test_hiv.CONFIRM_FACI,
       test_hiv.CONFIRM_SUB_FACI,
       test_hiv.CONFIRM_TYPE,
       test_hiv.CONFIRM_CODE,
       test_hiv.SPECIMEN_REFER_TYPE,
       test_hiv.SPECIMEN_SOURCE,
       test_hiv.SPECIMEN_SUB_SOURCE,
       test_hiv.CONFIRM_RESULT,
       test_hiv.SIGNATORY_1,
       test_hiv.SIGNATORY_2,
       test_hiv.SIGNATORY_3,
       test_hiv.DATE_RELEASE,
       test_hiv.DATE_CONFIRM,
       test_hiv.IDNUM,
       pii.CLIENT_MOBILE,
       pii.CLIENT_EMAIL,
       consent.VERBAL_CONSENT,
       consent.SIGNATURE_ESIG,
       consent.SIGNATURE_NAME,
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
           ELSE NULL END                   AS EXPOSE_HIV_MOTHER,
       CASE
           WHEN risk.RISK_SEX_M LIKE '%Yes%' THEN '1_Yes'
           WHEN risk.RISK_SEX_M LIKE '%No%' THEN '0_No'
           ELSE NULL END                   AS EXPOSE_SEX_M,
       CASE
           WHEN risk.RISK_SEX_M_AV LIKE '%Yes%' THEN '1_Yes'
           WHEN risk.RISK_SEX_M_AV LIKE '%No%' THEN '0_No'
           ELSE NULL END                   AS EXPOSE_SEX_M_AV,
       risk.RISK_SEX_M_AV_DATE             AS EXPOSE_SEX_M_AV_DATE,
       CASE
           WHEN risk.RISK_SEX_M_AV_NOCONDOM LIKE '%Yes%' THEN '1_Yes'
           WHEN risk.RISK_SEX_M_AV_NOCONDOM LIKE '%No%' THEN '0_No'
           ELSE NULL END                   AS EXPOSE_SEX_M_AV_NOCONDOM,
       risk.RISK_SEX_M_AV_NOCONDOM_DATE    AS EXPOSE_SEX_M_AV_NOCONDOM_DATE,
       CASE
           WHEN risk.RISK_SEX_F LIKE '%Yes%' THEN '1_Yes'
           WHEN risk.RISK_SEX_F LIKE '%No%' THEN '0_No'
           ELSE NULL END                   AS EXPOSE_SEX_F,
       CASE
           WHEN risk.RISK_SEX_F_AV LIKE '%Yes%' THEN '1_Yes'
           WHEN risk.RISK_SEX_F_AV LIKE '%No%' THEN '0_No'
           ELSE NULL END                   AS EXPOSE_SEX_F_AV,
       risk.RISK_SEX_F_AV_DATE             AS EXPOSE_SEX_F_AV_DATE,
       CASE
           WHEN risk.RISK_SEX_F_AV_NOCONDOM LIKE '%Yes%' THEN '1_Yes'
           WHEN risk.RISK_SEX_F_AV_NOCONDOM LIKE '%No%' THEN '0_No'
           ELSE NULL END                   AS EXPOSE_SEX_F_AV_NOCONDOM,
       risk.RISK_SEX_F_AV_NOCONDOM_DATE    AS EXPOSE_SEX_F_AV_NOCONDOM_DATE,
       CASE
           WHEN risk.RISK_SEX_PAYING LIKE '%Yes%' THEN '1_Yes'
           WHEN risk.RISK_SEX_PAYING LIKE '%No%' THEN '0_No'
           ELSE NULL END                   AS EXPOSE_SEX_PAYING,
       risk.RISK_SEX_PAYING_DATE           AS EXPOSE_SEX_PAYING_DATE,
       CASE
           WHEN risk.RISK_SEX_PAYMENT LIKE '%Yes%' THEN '1_Yes'
           WHEN risk.RISK_SEX_PAYMENT LIKE '%No%' THEN '0_No'
           ELSE NULL END                   AS EXPOSE_SEX_PAYMENT,
       risk.RISK_SEX_PAYMENT_DATE          AS EXPOSE_SEX_PAYMENT_DATE,
       CASE
           WHEN risk.RISK_SEX_DRUGS LIKE '%Yes%' THEN '1_Yes'
           WHEN risk.RISK_SEX_DRUGS LIKE '%No%' THEN '0_No'
           ELSE NULL END                   AS EXPOSE_SEX_DRUGS,
       risk.RISK_SEX_DRUGS_DATE            AS EXPOSE_SEX_DRUGS_DATE,
       CASE
           WHEN risk.RISK_DRUG_INJECT LIKE '%Yes%' THEN '1_Yes'
           WHEN risk.RISK_DRUG_INJECT LIKE '%No%' THEN '0_No'
           ELSE NULL END                   AS EXPOSE_DRUG_INJECT,
       risk.RISK_DRUG_INJECT_DATE          AS EXPOSE_DRUG_INJECT_DATE,
       CASE
           WHEN risk.RISK_BLOOD_TRANSFUSE LIKE '%Yes%' THEN '1_Yes'
           WHEN risk.RISK_BLOOD_TRANSFUSE LIKE '%No%' THEN '0_No'
           ELSE NULL END                   AS EXPOSE_BLOOD_TRANSFUSE,
       risk.RISK_BLOOD_TRANSFUSE_DATE      AS EXPOSE_BLOOD_TRANSFUSE_DATE,
       CASE
           WHEN risk.RISK_OCCUPATION LIKE '%Yes%' THEN '1_Yes'
           WHEN risk.RISK_OCCUPATION LIKE '%No%' THEN '0_No'
           ELSE NULL END                   AS EXPOSE_OCCUPATION,
       risk.RISK_OCCUPATION_DATE           AS EXPOSE_OCCUPATION_DATE,
       test_reason.TEST_REASON_HIV_EXPOSE,
       test_reason.TEST_REASON_PHYSICIAN,
       test_reason.TEST_REASON_PEER_ED,
       test_reason.TEST_REASON_EMPLOY_OFW,
       test_reason.TEST_REASON_EMPLOY_LOCAL,
       test_reason.TEST_REASON_TEXT_EMAIL,
       test_reason.TEST_REASON_INSURANCE,
       test_reason.TEST_REASON_OTHER_TEXT,
       test_previous.PREV_TESTED,
       test_previous.PREV_TEST_DATE,
       test_previous.PREV_TEST_FACI,
       test_previous.PREV_TEST_RESULT,
       med_profile.MED_TB_PX,
       med_profile.MED_STI,
       med_profile.MED_HEP_B,
       med_profile.MED_HEP_C,
       med_profile.MED_PREP_PX,
       med_profile.MED_PEP_PX,
       staging.CLINICAL_PIC,
       staging.SYMPTOMS,
       staging.WHO_CLASS,
       service.CLIENT_TYPE,
       reach.REACH_CLINICAL,
       reach.REACH_ONLINE,
       reach.REACH_INDEX_TESTING,
       reach.REACH_SSNT,
       reach.REACH_VENUE,
       cfbs.SCREEN_AGREED,
       test_refuse.TEST_REFUSE_OTHER_TEXT,
       service.MODALITY,
       test_hiv.T0_DATE,
       test_hiv.T0_RESULT,
       linkage.REFER_ART,
       linkage.REFER_CONFIRM,
       linkage.REFER_RETEST,
       linkage.RETEST_MOS,
       linkage.RETEST_WKS,
       linkage.RETEST_DATE,
       service_other.SERVICE_HIV_101,
       service_other.SERVICE_IEC_MATS,
       service_other.SERVICE_RISK_COUNSEL,
       service_other.SERVICE_PREP_REFER,
       service_other.SERVICE_SSNT_OFFER,
       service_other.SERVICE_SSNT_ACCEPT,
       service_other.SERVICE_GIVEN_CONDOMS AS SERVICE_CONDOMS,
       service_other.SERVICE_GIVEN_LUBES   AS SERVICE_LUBES,
       service.SERVICE_FACI,
       service.SERVICE_SUB_FACI,
       service.SERVICE_BY,
       service.PROVIDER_TYPE,
       service.PROVIDER_TYPE_OTHER,
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
           WHEN pii.SEX = '1_Male' AND risk.RISK_SEX_M LIKE '%Yes%' THEN 1
           WHEN pii.SEX = '1_Male' AND risk.RISK_SEX_M_AV_DATE IS NOT NULL THEN 1
           WHEN pii.SEX = '1_Male' AND risk.RISK_SEX_M_AV_NOCONDOM_DATE IS NOT NULL THEN 1
           ELSE 0
           END                             AS HTS_MSM,
       CASE
           WHEN pii.SEX = '1_Male' AND LEFT(pii.SELF_IDENT, 1) = '2' THEN 1
           WHEN pii.SEX = '1_Male' AND LEFT(pii.SELF_IDENT, 1) = '3' THEN 1
           ELSE 0
           END                             AS HTS_TGW
FROM ohasis_lake.px_pii AS pii
         LEFT JOIN ohasis_lake.px_faci_info AS service ON pii.REC_ID = service.REC_ID
         LEFT JOIN ohasis_lake.px_ob AS ob ON pii.REC_ID = ob.REC_ID
         LEFT JOIN ohasis_lake.px_hiv_testing AS test_hiv ON pii.REC_ID = test_hiv.REC_ID
         LEFT JOIN ohasis_lake.px_consent AS consent ON pii.REC_ID = consent.REC_ID
         LEFT JOIN ohasis_lake.px_occupation AS work ON pii.REC_ID = work.REC_ID
         LEFT JOIN ohasis_lake.px_ofw AS ofw ON pii.REC_ID = ofw.REC_ID
         LEFT JOIN ohasis_lake.px_risk AS risk ON pii.REC_ID = risk.REC_ID
         LEFT JOIN ohasis_lake.px_test_reason AS test_reason ON pii.REC_ID = test_reason.REC_ID
         LEFT JOIN ohasis_lake.px_test_refuse AS test_refuse ON pii.REC_ID = test_refuse.REC_ID
         LEFT JOIN ohasis_lake.px_test_previous AS test_previous ON pii.REC_ID = test_previous.REC_ID
         LEFT JOIN ohasis_lake.px_med_profile AS med_profile ON pii.REC_ID = med_profile.REC_ID
         LEFT JOIN ohasis_lake.px_staging AS staging ON pii.REC_ID = staging.REC_ID
         LEFT JOIN ohasis_lake.px_cfbs AS cfbs ON pii.REC_ID = cfbs.REC_ID
         LEFT JOIN ohasis_lake.px_reach AS reach ON pii.REC_ID = reach.REC_ID
         LEFT JOIN ohasis_lake.px_linkage AS linkage ON pii.REC_ID = linkage.REC_ID
         LEFT JOIN ohasis_lake.px_other_service AS service_other ON pii.REC_ID = service_other.REC_ID
WHERE FORM_VERSION LIKE 'HTS Form%'
  AND ((pii.CREATED_AT BETWEEN ? AND ?) OR
       (pii.UPDATED_AT BETWEEN ? AND ?) OR
       (pii.DELETED_AT BETWEEN ? AND ?));
-- ID_COLS: REC_ID;
-- DELETE: DELETED_AT IS NOT NULL OR FORM_VERSION <> 'HTS Form (v2021)';