SELECT rec.REC_ID,
       rec.PATIENT_ID,
       CONCAT(form.FORM, ' (', form.VERSION, ')')                                                      AS FORM_VERSION,
       rec.CREATED_BY,
       rec.CREATED_AT,
       rec.UPDATED_BY,
       rec.UPDATED_AT,
       rec.DELETED_BY,
       rec.DELETED_AT,
       GREATEST(COALESCE(rec.DELETED_AT, 0), COALESCE(rec.UPDATED_AT, 0), COALESCE(rec.CREATED_AT, 0)) AS SNAPSHOT,
       rec.FACI_ID,
       rec.SUB_FACI_ID,
       rec.RECORD_DATE,
       CASE
           WHEN (DISEASE = '101000') THEN 'HIV'
           WHEN (DISEASE = '102000') THEN 'Hepatitis B'
           WHEN (DISEASE = '103000') THEN 'Hepatitis C'
           ELSE DISEASE
           END                                                                                         AS DISEASE,
       CASE
           WHEN (MODULE = '0') THEN '0_Client Add/Update'
           WHEN (MODULE = '1') THEN '1_General'
           WHEN (MODULE = '2') THEN '2_Testing'
           WHEN (MODULE = '3') THEN '3_Treatment'
           WHEN (MODULE = '4') THEN '4_Modality'
           WHEN (MODULE = '6') THEN '6_Prevention'
           ELSE MODULE
           END                                                                                         AS MODULE,
       rec.PRIME,
       info.CONFIRMATORY_CODE,
       info.UIC,
       info.PHILHEALTH_NO,
       CASE
           WHEN (SEX = 1) THEN '1_Male'
           WHEN (SEX = 2) THEN '2_Female'
           ELSE (CAST(SEX AS CHAR))
           END                                                                                         AS SEX,
       info.BIRTHDATE,
       info.PATIENT_CODE,
       info.PHILSYS_ID,
       name.FIRST,
       name.MIDDLE,
       name.LAST,
       name.SUFFIX,
       CASE
           WHEN (SERVICE_TYPE = '*00001') THEN 'Mortality'
           WHEN (SERVICE_TYPE = '101101') THEN 'HIV FBT'
           WHEN (SERVICE_TYPE = '101102') THEN 'HIV FBT'
           WHEN (SERVICE_TYPE = '101103') THEN 'CBS'
           WHEN (SERVICE_TYPE = '101104') THEN 'FBS'
           WHEN (SERVICE_TYPE = '101105') THEN 'ST'
           WHEN (SERVICE_TYPE = '101201') THEN 'ART'
           WHEN (SERVICE_TYPE = '101301') THEN 'PrEP'
           WHEN (SERVICE_TYPE = '101303') THEN 'PMTCT-N'
           WHEN (SERVICE_TYPE = '101304') THEN 'Reach'
           WHEN (SERVICE_TYPE = '') THEN NULL
           ELSE SERVICE_TYPE
           END                                                                                         AS SERVICE_TYPE,
       profile.AGE,
       profile.AGE_MO,
       profile.GENDER_AFFIRM_THERAPY,
       CASE
           WHEN (SELF_IDENT = 1) THEN '1_Male'
           WHEN (SELF_IDENT = 2) THEN '2_Female'
           WHEN (SELF_IDENT = 3) THEN '3_Other'
           ELSE NULL
           END                                                                                         AS SELF_IDENT,
       profile.SELF_IDENT_OTHER,
       country.COUNTRY_NAME                                                                            AS NATIONALITY,
       profile.NATIONALITY_OTHER,
       CASE
           WHEN (EDUC_LEVEL = 1) THEN '1_None'
           WHEN (EDUC_LEVEL = 2) THEN '2_Elementary'
           WHEN (EDUC_LEVEL = 3) THEN '3_High School'
           WHEN (EDUC_LEVEL = 4) THEN '4_College'
           WHEN (EDUC_LEVEL = 5) THEN '5_Vocational'
           WHEN (EDUC_LEVEL = 6) THEN '6_Post-Graduate'
           WHEN (EDUC_LEVEL = 7) THEN '7_Pre-school'
           ELSE NULL
           END                                                                                         AS EDUC_LEVEL,
       CASE
           WHEN (CIVIL_STATUS = 1) THEN '1_Single'
           WHEN (CIVIL_STATUS = 2) THEN '2_Married'
           WHEN (CIVIL_STATUS = 3) THEN '3_Separated'
           WHEN (CIVIL_STATUS = 4) THEN '4_Widowed'
           WHEN (CIVIL_STATUS = 5) THEN '5_Divorced'
           ELSE NULL
           END                                                                                         AS CIVIL_STATUS,
       profile.LIVING_WITH_PARTNER,
       profile.CHILDREN,
       curr.ADDR_REG                                                                                   AS CURR_PSGC_REG,
       curr.ADDR_PROV                                                                                  AS CURR_PSGC_PROV,
       curr.ADDR_MUNC                                                                                  AS CURR_PSGC_MUNC,
       curr.ADDR_TEXT                                                                                  AS CURR_ADDR,
       perm.ADDR_REG                                                                                   AS PERM_PSGC_REG,
       perm.ADDR_PROV                                                                                  AS PERM_PSGC_PROV,
       perm.ADDR_MUNC                                                                                  AS PERM_PSGC_MUNC,
       perm.ADDR_TEXT                                                                                  AS PERM_ADDR,
       birth.ADDR_REG                                                                                  AS BIRTH_PSGC_REG,
       birth.ADDR_PROV                                                                                 AS BIRTH_PSGC_PROV,
       birth.ADDR_MUNC                                                                                 AS BIRTH_PSGC_MUNC,
       birth.ADDR_TEXT                                                                                 AS BIRTH_ADDR,
       death.ADDR_REG                                                                                  AS DEATH_PSGC_REG,
       death.ADDR_PROV                                                                                 AS DEATH_PSGC_PROV,
       death.ADDR_MUNC                                                                                 AS DEATH_PSGC_MUNC,
       death.ADDR_TEXT                                                                                 AS DEATH_ADDR,
       location.ADDR_REG                                                                               AS SERVICE_PSGC_REG,
       location.ADDR_PROV                                                                              AS SERVICE_PSGC_PROV,
       location.ADDR_MUNC                                                                              AS SERVICE_PSGC_MUNC,
       location.ADDR_TEXT                                                                              AS SERVICE_ADDR
FROM ohasis_interim.px_record AS rec
         JOIN ohasis_interim.px_info AS info ON rec.REC_ID = info.REC_ID
         JOIN ohasis_interim.px_name AS name ON rec.REC_ID = name.REC_ID
         LEFT JOIN ohasis_interim.px_form AS form ON rec.REC_ID = form.REC_ID
         LEFT JOIN ohasis_interim.px_profile AS profile ON rec.REC_ID = profile.REC_ID
         LEFT JOIN ohasis_interim.addr_country AS country ON profile.NATIONALITY = country.COUNTRY_CODE
         LEFT JOIN ohasis_interim.px_faci AS service ON rec.REC_ID = service.REC_ID
         LEFT JOIN ohasis_interim.px_addr AS perm ON rec.REC_ID = perm.REC_ID AND perm.ADDR_TYPE = 2
         LEFT JOIN ohasis_interim.px_addr AS curr ON rec.REC_ID = curr.REC_ID AND curr.ADDR_TYPE = 1
         LEFT JOIN ohasis_interim.px_addr AS birth ON rec.REC_ID = birth.REC_ID AND birth.ADDR_TYPE = 3
         LEFT JOIN ohasis_interim.px_addr AS death ON rec.REC_ID = death.REC_ID AND death.ADDR_TYPE = 4
         LEFT JOIN ohasis_interim.px_addr AS location ON rec.REC_ID = location.REC_ID AND location.ADDR_TYPE = 5
WHERE service.SERVICE_TYPE NOT IN ('101102', '101106')
  AND ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?));
-- ID_COLS: REC_ID