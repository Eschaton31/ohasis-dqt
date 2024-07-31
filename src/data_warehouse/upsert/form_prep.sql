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
       CASE
           WHEN pii.RECORD_DATE = DATE(meds.DISP_DATE) THEN pii.RECORD_DATE
           WHEN pii.RECORD_DATE != DATE(meds.DISP_DATE) AND DATE(meds.DISP_DATE) != '0000-00-00'
               THEN DATE(meds.DISP_DATE)
           WHEN pii.RECORD_DATE IS NULL AND meds.DISP_DATE IS NOT NULL THEN DATE(meds.DISP_DATE)
           ELSE pii.RECORD_DATE
           END                             AS VISIT_DATE,
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
       pii.CLIENT_MOBILE,
       pii.CLIENT_EMAIL,
       pii.AGE,
       pii.AGE_MO,
       pii.GENDER_AFFIRM_THERAPY,
       pii.SELF_IDENT,
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
       service.SERVICE_FACI,
       service.SERVICE_SUB_FACI,
       service.SERVICE_BY,
       service.CLIENT_TYPE,
       service.CLINIC_NOTES,
       service.COUNSEL_NOTES,
       service.STI_DIAGNOSIS,
       expose_profile.WEEK_AVG_SEX,
       kp.KP_PDL,
       kp.KP_SW,
       kp.KP_TG,
       kp.KP_PWID,
       kp.KP_MSM,
       kp.KP_OFW,
       kp.KP_PARTNER,
       kp.KP_OTHER,
       risk.RISK_CONDOMLESS_ANAL,
       risk.RISK_STI,
       risk.RISK_HIV_VL_UNKNOWN,
       risk.RISK_SEX_TRANSACT              AS RISK_TRANSACT_SEX,
       risk.RISK_HIV_UNKNOWN,
       risk.RISK_CONDOMLESS_VAGINAL,
       risk.RISK_DRUG_INJECT,
       risk.RISK_PEP,
       risk.RISK_SEX_DRUGS                 AS RISK_DRUG_SEX,
       lab.LAB_HBSAG_DATE,
       lab.LAB_HBSAG_RESULT,
       lab.LAB_CREA_DATE,
       lab.LAB_CREA_RESULT,
       lab.LAB_CREA_CLEARANCE,
       lab.LAB_SYPH_DATE,
       lab.LAB_SYPH_RESULT,
       lab.LAB_SYPH_TITER,
       vitals.WEIGHT,
       vitals.BODY_TEMP,
       ars.ARS_SX_FEVER,
       ars.ARS_SX_SORE_THROAT,
       ars.ARS_SX_DIARRHEA,
       ars.ARS_SX_SWOLLEN_LYMPH,
       ars.ARS_SX_SWOLLEN_TONSILS,
       ars.ARS_SX_RASH,
       ars.ARS_SX_MUSCLE_PAINS,
       ars.ARS_SX_OTHER,
       ars.ARS_SX_OTHER_TEXT,
       ars.ARS_SX_NONE,
       sti.STI_SX_PAIN_URINE,
       sti.STI_SX_DISCHARGE_URETHRAL,
       sti.STI_SX_WARTS_GENITAL,
       sti.STI_SX_ULCER_GENITAL,
       sti.STI_SX_ULCER_ORAL,
       sti.STI_SX_PAIN_ABDOMEN,
       sti.STI_SX_DISCHARGE_ANAL,
       sti.STI_SX_DISCHARGE_VAGINAL,
       sti.STI_SX_SWOLLEN_SCROTUM,
       sti.STI_SX_OTHER,
       sti.STI_SX_OTHER_TEXT,
       sti.STI_SX_NONE,
       prep.PRE_INIT_HIV_NR,
       prep.PRE_INIT_WEIGHT,
       prep.PRE_INIT_NO_ARS,
       prep.PRE_INIT_CREA_CLEAR,
       prep.PRE_INIT_NO_ARV_ALLERGY,
       prep.PREP_HIV_DATE,
       prep.PREP_STATUS,
       prep.PREP_VISIT,
       prep.ELIGIBLE_BEHAVIOR,
       prep.ELIGIBLE_PREP,
       prep.PREP_REQUESTED,
       prep.PREP_PLAN,
       prep.PREP_TYPE,
       prep.FIRST_TIME,
       prep.PREP_CONTINUED,
       prep.PREP_TYPE_LAST_VISIT,
       prep.PREP_SHIFT,
       prep.PREP_MISSED,
       prep.PREP_SIDE_EFFECTS,
       prep.PREP_SIDE_EFFECTS_SPECIFY,
       CASE work.IS_EMPLOYED
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           ELSE work.IS_EMPLOYED END       AS IS_EMPLOYED,
       CASE work.IS_STUDENT
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           ELSE work.IS_STUDENT END        AS IS_STUDENT,
       prep.FINANCE_CAPACITY,
       meds.REC_ID_GRP,
       meds.FACI_DISP,
       meds.SUB_FACI_DISP,
       CASE
           WHEN meds.MEDICINE_SUMMARY IS NOT NULL THEN meds.MEDICINE_SUMMARY
           WHEN meds.MEDICINE_SUMMARY IS NULL
               AND (UPPER(CONCAT_WS(': ', CLINIC_NOTES, COUNSEL_NOTES)) REGEXP 'TDF'
                   OR UPPER(CONCAT_WS(': ', CLINIC_NOTES, COUNSEL_NOTES)) REGEXP 'FTC'
                   OR UPPER(CONCAT_WS(': ', CLINIC_NOTES, COUNSEL_NOTES)) REGEXP 'TENOFO'
                   OR UPPER(CONCAT_WS(': ', CLINIC_NOTES, COUNSEL_NOTES)) REGEXP 'EMTRICI') THEN 'TDF/FTC'
           ELSE NULL
           END                             AS MEDICINE_SUMMARY,
       meds.DISP_DATE,
       CASE
           WHEN meds.MEDICINE_SUMMARY IS NOT NULL THEN meds.LATEST_NEXT_DATE
           WHEN meds.MEDICINE_SUMMARY IS NULL
               AND (UPPER(CONCAT_WS(': ', CLINIC_NOTES, COUNSEL_NOTES)) REGEXP 'TDF'
                   OR UPPER(CONCAT_WS(': ', CLINIC_NOTES, COUNSEL_NOTES)) REGEXP 'FTC'
                   OR UPPER(CONCAT_WS(': ', CLINIC_NOTES, COUNSEL_NOTES)) REGEXP 'TENOFO'
                   OR UPPER(CONCAT_WS(': ', CLINIC_NOTES, COUNSEL_NOTES)) REGEXP 'EMTRICI') THEN DATE_ADD(RECORD_DATE, INTERVAL 30 DAY)
           ELSE NULL
           END                             AS LATEST_NEXT_DATE,
       CASE
           WHEN meds.MEDICINE_SUMMARY IS NOT NULL THEN COALESCE(ROUND((LENGTH(meds.MEDICINE_SUMMARY) - LENGTH(REPLACE(meds.MEDICINE_SUMMARY, '+', ''))) /
                      LENGTH('+')) + 1, 0)
           WHEN meds.MEDICINE_SUMMARY IS NULL
               AND (UPPER(CONCAT_WS(': ', CLINIC_NOTES, COUNSEL_NOTES)) REGEXP 'TDF'
                   OR UPPER(CONCAT_WS(': ', CLINIC_NOTES, COUNSEL_NOTES)) REGEXP 'FTC'
                   OR UPPER(CONCAT_WS(': ', CLINIC_NOTES, COUNSEL_NOTES)) REGEXP 'TENOFO'
                   OR UPPER(CONCAT_WS(': ', CLINIC_NOTES, COUNSEL_NOTES)) REGEXP 'EMTRICI') THEN 1
           ELSE NULL
           END                             AS NUM_OF_DRUGS,
       CASE
           WHEN meds.MEDICINE_SUMMARY IS NOT NULL THEN 'PrEP'
           WHEN meds.MEDICINE_SUMMARY IS NULL
               AND (UPPER(CONCAT_WS(': ', CLINIC_NOTES, COUNSEL_NOTES)) REGEXP 'TDF'
                   OR UPPER(CONCAT_WS(': ', CLINIC_NOTES, COUNSEL_NOTES)) REGEXP 'FTC'
                   OR UPPER(CONCAT_WS(': ', CLINIC_NOTES, COUNSEL_NOTES)) REGEXP 'TENOFO'
                   OR UPPER(CONCAT_WS(': ', CLINIC_NOTES, COUNSEL_NOTES)) REGEXP 'EMTRICI') THEN 'PrEP'
           WHEN LEFT(PREP_STATUS, 1) = '1' THEN 'PrEP'
           WHEN LEFT(PREP_CONTINUED, 1) = '1' THEN 'PrEP'
           ELSE 'Visit'
           END                             AS PREP_RECORD
FROM ohasis_lake.px_pii AS pii
         LEFT JOIN ohasis_lake.px_faci_info AS service ON pii.REC_ID = service.REC_ID
         LEFT JOIN ohasis_lake.px_key_pop AS kp ON pii.REC_ID = kp.REC_ID
         LEFT JOIN ohasis_lake.px_risk AS risk ON pii.REC_ID = risk.REC_ID
         LEFT JOIN ohasis_lake.lab_wide AS lab ON pii.REC_ID = lab.REC_ID
         LEFT JOIN ohasis_lake.px_vitals AS vitals ON pii.REC_ID = vitals.REC_ID
         LEFT JOIN ohasis_lake.px_ars_sx AS ars ON pii.REC_ID = ars.REC_ID
         LEFT JOIN ohasis_lake.px_sti_sx AS sti ON pii.REC_ID = sti.REC_ID
         LEFT JOIN ohasis_lake.px_prep AS prep ON pii.REC_ID = prep.REC_ID
         LEFT JOIN ohasis_lake.px_occupation AS work ON pii.REC_ID = work.REC_ID
         LEFT JOIN ohasis_lake.px_expose_profile AS expose_profile ON pii.REC_ID = expose_profile.REC_ID
         LEFT JOIN (SELECT disp.REC_ID,
                           disp.REC_ID_GRP,
                           disp.FACI_ID                                                     AS FACI_DISP,
                           disp.SUB_FACI_ID                                                 AS SUB_FACI_DISP,
                           GROUP_CONCAT(DISTINCT meds.SHORT ORDER BY `ORDER` SEPARATOR '+') AS MEDICINE_SUMMARY,
                           MAX(DISP_DATE)                                                   AS DISP_DATE,
                           MAX(NEXT_DATE)                                                   AS LATEST_NEXT_DATE
                    FROM ohasis_lake.disp_meds AS disp
                             JOIN ohasis_lake.ref_items AS meds ON disp.MEDICINE = meds.ITEM
                    GROUP BY disp.REC_ID, disp.REC_ID_GRP) AS meds
                   ON pii.REC_ID = meds.REC_ID
WHERE pii.DISEASE = 'HIV'
  AND MODULE = '6_Prevention'
  AND FORM_VERSION LIKE 'PrEP%'
  AND ((pii.CREATED_AT BETWEEN ? AND ?) OR
       (pii.UPDATED_AT BETWEEN ? AND ?) OR
       (pii.DELETED_AT BETWEEN ? AND ?));;
-- ID_COLS: REC_ID, REC_ID_GRP;
-- DELETE: DELETED_AT IS NOT NULL OR FORM_VERSION NOT LIKE 'PrEP%':
