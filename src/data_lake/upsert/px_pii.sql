SELECT rec.REC_ID,
       rec.PATIENT_ID,
       CONCAT(form.FORM, ' (v', form.VERSION, ')')                                                     AS FORM_VERSION,
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
       MAX(IF(contact.CONTACT_TYPE = 1, contact.CONTACT, NULL))                                        AS CLIENT_MOBILE,
       MAX(IF(contact.CONTACT_TYPE = 2, contact.CONTACT, NULL))                                        AS CLIENT_EMAIL,
       MAX(CASE
               WHEN (service.SERVICE_TYPE = '*00001') THEN 'Mortality'
               WHEN (service.SERVICE_TYPE = '101101') THEN 'HIV FBT'
               WHEN (service.SERVICE_TYPE = '101102') THEN 'HIV FBT'
               WHEN (service.SERVICE_TYPE = '101103') THEN 'CBS'
               WHEN (service.SERVICE_TYPE = '101104') THEN 'FBS'
               WHEN (service.SERVICE_TYPE = '101105') THEN 'ST'
               WHEN (service.SERVICE_TYPE = '101106') THEN 'HIV FBT'
               WHEN (service.SERVICE_TYPE = '101201') THEN 'ART'
               WHEN (service.SERVICE_TYPE = '101301') THEN 'PrEP'
               WHEN (service.SERVICE_TYPE = '101303') THEN 'PMTCT-N'
               WHEN (service.SERVICE_TYPE = '101304') THEN 'Reach'
               WHEN (service.SERVICE_TYPE = '') THEN NULL
               ELSE service.SERVICE_TYPE
           END)                                                                                        AS SERVICE_TYPE,
       profile.AGE,
       profile.AGE_MO,
       CASE profile.GENDER_AFFIRM_THERAPY
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           END                                                                                         AS GENDER_AFFIRM_THERAPY,
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
       CASE profile.LIVING_WITH_PARTNER
           WHEN 1 THEN '1_Yes'
           WHEN 0 THEN '0_No'
           ELSE NULL
           END                                                                                         AS LIVING_WITH_PARTNER,
       profile.CHILDREN,
       MAX(IF(addr.ADDR_TYPE = 1, addr.ADDR_REG, NULL))                                                AS CURR_PSGC_REG,
       MAX(IF(addr.ADDR_TYPE = 1, addr.ADDR_PROV, NULL))                                               AS CURR_PSGC_PROV,
       MAX(IF(addr.ADDR_TYPE = 1, addr.ADDR_MUNC, NULL))                                               AS CURR_PSGC_MUNC,
       MAX(IF(addr.ADDR_TYPE = 1, addr.ADDR_TEXT, NULL))                                               AS CURR_ADDR,
       MAX(IF(addr.ADDR_TYPE = 2, addr.ADDR_REG, NULL))                                                AS PERM_PSGC_REG,
       MAX(IF(addr.ADDR_TYPE = 2, addr.ADDR_PROV, NULL))                                               AS PERM_PSGC_PROV,
       MAX(IF(addr.ADDR_TYPE = 2, addr.ADDR_MUNC, NULL))                                               AS PERM_PSGC_MUNC,
       MAX(IF(addr.ADDR_TYPE = 2, addr.ADDR_TEXT, NULL))                                               AS PERM_ADDR,
       MAX(IF(addr.ADDR_TYPE = 3, addr.ADDR_REG, NULL))                                                AS BIRTH_PSGC_REG,
       MAX(IF(addr.ADDR_TYPE = 3, addr.ADDR_PROV, NULL))                                               AS BIRTH_PSGC_PROV,
       MAX(IF(addr.ADDR_TYPE = 3, addr.ADDR_MUNC, NULL))                                               AS BIRTH_PSGC_MUNC,
       MAX(IF(addr.ADDR_TYPE = 3, addr.ADDR_TEXT, NULL))                                               AS BIRTH_ADDR,
       MAX(IF(addr.ADDR_TYPE = 4, addr.ADDR_REG, NULL))                                                AS DEATH_PSGC_REG,
       MAX(IF(addr.ADDR_TYPE = 4, addr.ADDR_PROV, NULL))                                               AS DEATH_PSGC_PROV,
       MAX(IF(addr.ADDR_TYPE = 4, addr.ADDR_MUNC, NULL))                                               AS DEATH_PSGC_MUNC,
       MAX(IF(addr.ADDR_TYPE = 4, addr.ADDR_TEXT, NULL))                                               AS DEATH_ADDR,
       MAX(IF(addr.ADDR_TYPE = 5, addr.ADDR_REG, NULL))                                                AS SERVICE_PSGC_REG,
       MAX(IF(addr.ADDR_TYPE = 5, addr.ADDR_PROV, NULL))                                               AS SERVICE_PSGC_PROV,
       MAX(IF(addr.ADDR_TYPE = 5, addr.ADDR_MUNC, NULL))                                               AS SERVICE_PSGC_MUNC,
       MAX(IF(addr.ADDR_TYPE = 5, addr.ADDR_TEXT, NULL))                                               AS SERVICE_ADDR
FROM ohasis_interim.px_record AS rec
         LEFT JOIN ohasis_interim.px_info AS info ON rec.REC_ID = info.REC_ID
         LEFT JOIN ohasis_interim.px_name AS name ON rec.REC_ID = name.REC_ID
         LEFT JOIN ohasis_interim.px_form AS form ON rec.REC_ID = form.REC_ID
         LEFT JOIN ohasis_interim.px_profile AS profile ON rec.REC_ID = profile.REC_ID
         LEFT JOIN ohasis_interim.px_faci AS service ON rec.REC_ID = service.REC_ID
         LEFT JOIN ohasis_interim.px_addr AS addr ON rec.REC_ID = addr.REC_ID
         LEFT JOIN ohasis_interim.px_contact AS contact ON rec.REC_ID = contact.REC_ID
         LEFT JOIN ohasis_interim.addr_country AS country ON profile.NATIONALITY = country.COUNTRY_CODE
WHERE ((rec.CREATED_AT BETWEEN ? AND ?) OR
       (rec.UPDATED_AT BETWEEN ? AND ?) OR
       (rec.DELETED_AT BETWEEN ? AND ?))
GROUP BY rec.REC_ID;
-- ID_COLS: REC_ID;