source("src/official/dsa/protects-upscale/01_load_reqs.R")

con   <- ohasis$conn("lw")
forms <- QB$new(con)
forms$where(function(query = QB$new(con)) {
   query$whereBetween('RECORD_DATE', c(min, max), "or")
   query$whereBetween('DATE_CONFIRM', c(min, max), "or")
   query$whereBetween('T0_DATE', c(min, max), "or")
   query$whereBetween('T1_DATE', c(min, max), "or")
   query$whereBetween('T2_DATE', c(min, max), "or")
   query$whereBetween('T3_DATE', c(min, max), "or")
   query$whereNested
})
forms$where(function(query = QB$new(con)) {
   query$whereIn('FACI_ID', sites$FACI_ID, boolean = "or")
   query$whereIn('SERVICE_FACI', sites$FACI_ID, boolean = "or")
   query$whereNested
})

forms$from("ohasis_warehouse.form_hts")
hts <- forms$get()

forms$from("ohasis_warehouse.form_a")
a <- forms$get()

cfbs <- QB$new(con)$
   from("ohasis_warehouse.form_cfbs")$
   limit(0)$
   get()

id_reg <- QB$new(con)$
   from("ohasis_warehouse.id_registry")$
   select(PATIENT_ID, CENTRAL_ID)$
   get()
dbDisconnect(con)

dx         <- read_dta(hs_data("harp_dx", "reg", 2024, 5)) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   mutate(
      confirm_branch = NA_character_
   ) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(CONFIRM_FACI = "confirmlab", CONFIRM_SUB_FACI = "confirm_branch")
   ) %>%
   ohasis$get_faci(
      list(confirm_lab = c("CONFIRM_FACI", "CONFIRM_SUB_FACI")),
      "name"
   )
dead       <- read_dta(hs_data("harp_dead", "reg", 2024, 5)) %>%
   get_cid(id_reg, PATIENT_ID)
tx_reg     <- read_dta(hs_data("harp_tx", "reg", 2024, 5)) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(FACI_ID = "artstart_hub", SUB_FACI_ID = "artstart_branch")
   ) %>%
   mutate(
      ART_FACI     = FACI_ID,
      ART_SUB_FACI = SUB_FACI_ID
   ) %>%
   ohasis$get_faci(
      list(tx_hub = c("FACI_ID", "SUB_FACI_ID")),
      "name",
      c("tx_reg", "tx_prov", "tx_munc")
   )
tx_out     <- read_dta(hs_data("harp_tx", "outcome", 2024, 5)) %>%
   select(-any_of("CENTRAL_ID")) %>%
   left_join(y = tx_reg %>% select(art_id, CENTRAL_ID), by = join_by(art_id)) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(FACI_ID = "realhub", SUB_FACI_ID = "realhub_branch")
   ) %>%
   mutate(
      ART_FACI     = FACI_ID,
      ART_SUB_FACI = SUB_FACI_ID
   ) %>%
   select(-tx_reg, -tx_prov, -tx_munc) %>%
   ohasis$get_faci(
      list(tx_hub = c("FACI_ID", "SUB_FACI_ID")),
      "name",
      c("tx_reg", "tx_prov", "tx_munc")
   )
prep_curr  <- read_dta(hs_data("prep", "outcome", 2024, 5)) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(FACI_ID = "faci", SUB_FACI_ID = "branch")
   ) %>%
   mutate(
      PREP_FACI     = FACI_ID,
      PREP_SUB_FACI = SUB_FACI_ID
   ) %>%
   ohasis$get_faci(
      list(site_name = c("FACI_ID", "SUB_FACI_ID")),
      "name",
   )
prep_start <- read_dta("H:/_R/library/prep/20240611_prepstart_2024-05.dta") %>%
   get_cid(id_reg, PATIENT_ID) %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      list(FACI_ID = "prepstart_faci", SUB_FACI_ID = "prepstart_branch")
   ) %>%
   mutate(
      PREP_FACI     = FACI_ID,
      PREP_SUB_FACI = SUB_FACI_ID
   ) %>%
   ohasis$get_faci(
      list(site_name = c("FACI_ID", "SUB_FACI_ID")),
      "name",
   )

testing <- process_hts(hts, a, cfbs) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   get_latest_pii(
      "CENTRAL_ID",
      c(
         "BIRTHDATE",
         "SEX",
         "SELF_IDENT",
         "SELF_IDENT_OTHER",
         "CIVIL_STATUS",
         "NATIONALITY",
         "EDUC_LEVEL",
         "CURR_PSGC_REG",
         "CURR_PSGC_PROV",
         "CURR_PSGC_MUNC",
         "PERM_PSGC_REG",
         "PERM_PSGC_PROV",
         "PERM_PSGC_MUNC",
         "BIRTH_PSGC_REG",
         "BIRTH_PSGC_PROV",
         "BIRTH_PSGC_MUNC"
      )
   ) %>%
   convert_hts("name") %>%
   generate_gender_identity(SEX, SELF_IDENT, SELF_IDENT_OTHER, gender_identity) %>%
   select(
      REC_ID,
      CENTRAL_ID,
      PATIENT_ID,
      FORM_VERSION,
      CREATED_BY,
      CREATED_AT,
      UPDATED_BY,
      UPDATED_AT,
      REPORT_FACI,
      RECORD_DATE,
      UIC,
      SEX,
      BIRTHDATE,
      AGE,
      AGE_MO,
      SELF_IDENT,
      SELF_IDENT_OTHER,
      NATIONALITY,
      EDUC_LEVEL,
      CIVIL_STATUS,
      IS_STUDENT,
      LIVING_WITH_PARTNER,
      CHILDREN,
      CURR_REG,
      CURR_PROV,
      CURR_MUNC,
      PERM_REG,
      PERM_PROV,
      PERM_MUNC,
      BIRTH_REG,
      BIRTH_PROV,
      BIRTH_MUNC,
      IS_PREGNANT,
      VERBAL_CONSENT,
      SIGNATURE_ESIG,
      SIGNATURE_NAME,
      WORK,
      IS_EMPLOYED,
      IS_OFW,
      OFW_YR_RET,
      OFW_STATION,
      OFW_COUNTRY,
      EXPOSE_HIV_MOTHER,
      EXPOSE_SEX_M,
      EXPOSE_SEX_M_AV_DATE,
      EXPOSE_SEX_M_AV_NOCONDOM_DATE,
      EXPOSE_SEX_F,
      EXPOSE_SEX_F_AV_DATE,
      EXPOSE_SEX_F_AV_NOCONDOM_DATE,
      EXPOSE_SEX_PAYING,
      EXPOSE_SEX_PAYING_DATE,
      EXPOSE_SEX_PAYMENT,
      EXPOSE_SEX_PAYMENT_DATE,
      EXPOSE_SEX_DRUGS,
      EXPOSE_SEX_DRUGS_DATE,
      EXPOSE_DRUG_INJECT,
      EXPOSE_DRUG_INJECT_DATE,
      EXPOSE_BLOOD_TRANSFUSE,
      EXPOSE_BLOOD_TRANSFUSE_DATE,
      EXPOSE_OCCUPATION,
      EXPOSE_OCCUPATION_DATE,
      TEST_REASON_HIV_EXPOSE,
      TEST_REASON_PHYSICIAN,
      TEST_REASON_PEER_ED,
      TEST_REASON_EMPLOY_OFW,
      TEST_REASON_EMPLOY_LOCAL,
      TEST_REASON_TEXT_EMAIL,
      TEST_REASON_INSURANCE,
      TEST_REASON_OTHER_TEXT,
      PREV_TESTED,
      PREV_TEST_DATE,
      PREV_TEST_FACI,
      PREV_TEST_RESULT,
      MED_TB_PX,
      MED_STI,
      MED_HEP_B,
      MED_HEP_C,
      MED_PREP_PX,
      MED_PEP_PX,
      CLINICAL_PIC,
      SYMPTOMS,
      WHO_CLASS,
      REACH_CLINICAL,
      REACH_ONLINE,
      REACH_INDEX_TESTING,
      REACH_SSNT,
      REACH_VENUE,
      TEST_REFUSE_OTHER_TEXT,
      REFER_ART,
      REFER_CONFIRM,
      REFER_RETEST,
      RETEST_MOS,
      RETEST_WKS,
      RETEST_DATE,
      SERVICE_HIV_101,
      SERVICE_IEC_MATS,
      SERVICE_RISK_COUNSEL,
      SERVICE_PREP_REFER,
      SERVICE_SSNT_OFFER,
      SERVICE_SSNT_ACCEPT,
      SERVICE_CONDOMS,
      SERVICE_LUBES,
      CBS_REG,
      CBS_PROV,
      CBS_MUNC,
      CBS_VENUE,
      HTS_REG,
      HTS_PROV,
      HTS_MUNC,
      HTS_FACI,
      HTS_PROVIDER,
      HTS_PROVIDER_TYPE,
      HTS_PROVIDER_TYPE_OTHER,
      EXPOSE_SEX_M_NOCONDOM,
      EXPOSE_SEX_F_NOCONDOM,
      EXPOSE_SEX_HIV,
      EXPOSE_TATTOO,
      EXPOSE_STI,
      AGE_FIRST_SEX,
      AGE_FIRST_INJECT,
      NUM_M_PARTNER,
      YR_LAST_M,
      NUM_F_PARTNER,
      YR_LAST_F,
      MED_IS_PREGNANT,
      MED_CBS_REACTIVE,
      TEST_REASON_RETEST,
      TEST_REASON_NO_REASON,
      EXPOSE_SEX_EVER,
      EXPOSE_M_SEX_ORAL_ANAL,
      EXPOSE_CONDOMLESS_ANAL,
      EXPOSE_CONDOMLESS_ANAL_DATE,
      EXPOSE_CONDOMLESS_VAGINAL,
      EXPOSE_CONDOMLESS_VAGINAL_DATE,
      EXPOSE_NEEDLE_SHARE,
      EXPOSE_NEEDLE_SHARE_DATE,
      EXPOSE_ILLICIT_DRUGS,
      EXPOSE_ILLICIT_DRUGS_DATE,
      EXPOSE_SEX_HIV_DATE,
      TEST_REFUSE_NO_TIME,
      TEST_REFUSE_OTHER,
      TEST_REFUSE_NO_CURE,
      TEST_REFUSE_FEAR_RESULT,
      TEST_REFUSE_FEAR_DISCLOSE,
      TEST_REFUSE_FEAR_MSM,
      gender_identity,
      hts_date,
      hts_result,
      hts_modality,
      test_agreed,
      hts_client_type,
      risk_motherhashiv,
      risk_sexwithf,
      risk_sexwithf_nocdm,
      risk_sexwithm,
      risk_sexwithm_nocdm,
      risk_payingforsex,
      risk_paymentforsex,
      risk_sexwithhiv,
      risk_injectdrug,
      risk_needlestick,
      risk_bloodtransfuse,
      risk_illicitdrug,
      risk_chemsex,
      risk_tattoo,
      risk_sti,
      online_app  = ONLINE_APP,
      sexual_risk = SEXUAL_RISK,
      kap_unknown,
      kap_msm,
      kap_heterom,
      kap_heterof,
      kap_pwid,
      kap_pip,
      kap_pdl,
   ) %>%
   mutate_at(
      .vars = vars(
         SEX,
         SELF_IDENT,
         EDUC_LEVEL,
         CIVIL_STATUS,
         LIVING_WITH_PARTNER,
         IS_PREGNANT,
         VERBAL_CONSENT,
         SIGNATURE_ESIG,
         SIGNATURE_NAME,
         IS_STUDENT,
         IS_EMPLOYED,
         IS_OFW,
         OFW_STATION,
         TEST_REASON_HIV_EXPOSE,
         TEST_REASON_PHYSICIAN,
         TEST_REASON_PEER_ED,
         TEST_REASON_EMPLOY_OFW,
         TEST_REASON_EMPLOY_LOCAL,
         TEST_REASON_TEXT_EMAIL,
         TEST_REASON_INSURANCE,
         PREV_TESTED,
         PREV_TEST_RESULT,
         MED_TB_PX,
         MED_STI,
         MED_HEP_B,
         MED_HEP_C,
         MED_PREP_PX,
         MED_PEP_PX,
         CLINICAL_PIC,
         WHO_CLASS,
         REACH_CLINICAL,
         REACH_ONLINE,
         REACH_INDEX_TESTING,
         REACH_SSNT,
         REACH_VENUE,
         REFER_ART,
         REFER_CONFIRM,
         REFER_RETEST,
         SERVICE_HIV_101,
         SERVICE_IEC_MATS,
         SERVICE_RISK_COUNSEL,
         SERVICE_PREP_REFER,
         SERVICE_SSNT_OFFER,
         SERVICE_SSNT_ACCEPT,
         HTS_PROVIDER_TYPE,
         MED_IS_PREGNANT,
         MED_CBS_REACTIVE,
         TEST_REASON_RETEST,
         TEST_REASON_NO_REASON,
         EXPOSE_HIV_MOTHER,
         EXPOSE_SEX_M,
         EXPOSE_SEX_F,
         EXPOSE_SEX_PAYING,
         EXPOSE_SEX_PAYMENT,
         EXPOSE_SEX_DRUGS,
         EXPOSE_DRUG_INJECT,
         EXPOSE_BLOOD_TRANSFUSE,
         EXPOSE_OCCUPATION,
         EXPOSE_SEX_M_NOCONDOM,
         EXPOSE_SEX_F_NOCONDOM,
         EXPOSE_SEX_HIV,
         EXPOSE_TATTOO,
         EXPOSE_STI
      ),
      ~str_to_title(remove_code(.)) %>%
         str_replace_all("\\bHiv\\b", "HIV") %>%
         str_replace_all("\\Cbs\\b", "CBS")
   ) %>%
   left_join(
      y  = dx %>%
         mutate(
            reactive_date = coalesce(blood_extract_date, specimen_receipt_date, test_date, t0_date, visit_date, confirm_date) %>% as.Date()
         ) %>%
         select(
            CENTRAL_ID,
            reactive_date,
            confirm_hiv_class = class2022,
            confirm_date,
            confirm_lab,
            confirm_code      = labcode2,
         ),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = dead %>%
         select(
            CENTRAL_ID,
            date_of_death,
         ) %>%
         mutate(
            reported_dead = 1
         ),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = tx_reg %>%
         select(
            CENTRAL_ID,
            artstart_hub = tx_hub
         ),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = tx_out %>%
         select(
            CENTRAL_ID,
            artstart_date,
            art_latest_hub        = tx_hub,
            art_latest_ffupdate   = latest_ffupdate,
            art_latest_nextpickup = latest_nextpickup,
            art_latest_regimen    = latest_regimen,
            art_outcome           = outcome,
         ),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = prep_start %>%
         filter(!is.na(prepstart_date)) %>%
         select(
            CENTRAL_ID,
            prepstart_date,
            prepstart_hub = site_name,
         ),
      by = join_by(CENTRAL_ID)
   ) %>%
   left_join(
      y  = prep_curr %>%
         filter(!is.na(prepstart_date)) %>%
         mutate(
            preplast_given = if_else(!is.na(latest_regimen), 1, 0, 0)
         ) %>%
         select(
            CENTRAL_ID,
            preplast_hub   = site_name,
            preplast_visit = latest_ffupdate,
            preplast_given,
         ),
      by = join_by(CENTRAL_ID)
   ) %>%
   mutate_if(
      .predicate = is.Date,
      ~if_else(. < -25567, NA_Date_, ., .)
   ) %>%
   mutate(
      WHO_CLASS        = toupper(WHO_CLASS),
      PREV_TEST_RESULT = case_when(
         PREV_TEST_RESULT == "Positive" ~ "Reactive",
         PREV_TEST_RESULT == "Negative" ~ "Non-Reactive",
         TRUE ~ PREV_TEST_RESULT
      )
   )

variables <- read_sheet("1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "hts")
dict      <- data_dictionary(testing, variables)

write_sheet(dict, "1OXWxDffKNVrAeoFPI6FIEcoCN1Zrku6W_eXYd-J4Tzc", "final-hts")
