##  Append testing data  -------------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]


.log_info("Setting scope of the reports")
start <- input(prompt = "What is the start date for the reports? (YYYY-MM-DD)")
end   <- input(prompt = "What is the end date for the reports? (YYYY-MM-DD)")

# open connections
.log_info("Opening connections.")
lw_conn <- ohasis$conn("lw")

##  Download relevant form data ------------------------------------------------

# Form A + HTS Forms
.log_info("Getting Form A data.")
form_a <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_a")) %>%
   filter(
      RECORD_DATE >= start,
      RECORD_DATE <= end
   ) %>%
   mutate(
      FORM        = "Form A",
      MODALITY    = "101101_FBT",
      TEST_AGREED = "1_Yes"
   ) %>%
   select(
      REC_ID,
      FORM,
      PATIENT_ID,
      UIC,
      SEX,
      FACI_ID,
      SELF_IDENT,
      SELF_IDENT_OTHER,
      TEST_FACI     = SERVICE_FACI,
      TEST_SUB_FACI = SERVICE_SUB_FACI,
      SERVICE_BY,
      RECORD_DATE,
      TEST_MSM      = FORMA_MSM,
      TEST_TGW      = FORMA_TGW,
      EXPOSE_SEX_M_NOCONDOM,
      NUM_M_PARTNER,
      NUM_F_PARTNER,
      MODALITY,
      TEST_AGREED,
      TEST_DATE     = T0_DATE,
      TEST_RESULT   = T0_RESULT,
      T1_DATE,
      T1_RESULT,
      DATE_CONFIRM,
      CONFIRM_RESULT,
      CLINIC_NOTES,
      COUNSEL_NOTES,
      CREATED_BY
   ) %>%
   collect()

.log_info("Getting HTS Form data.")
form_hts <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_hts")) %>%
   filter(
      RECORD_DATE >= start,
      RECORD_DATE <= end
   ) %>%
   mutate(FORM = "HTS Form") %>%
   select(
      REC_ID,
      FORM,
      PATIENT_ID,
      UIC,
      SEX,
      FACI_ID,
      SELF_IDENT,
      SELF_IDENT_OTHER,
      TEST_FACI     = SERVICE_FACI,
      TEST_SUB_FACI = SERVICE_SUB_FACI,
      SERVICE_BY,
      RECORD_DATE,
      TEST_MSM      = HTS_MSM,
      TEST_TGW      = HTS_TGW,
      HIV_SERVICE_ADDR,
      EXPOSE_SEX_M_AV_DATE,
      EXPOSE_SEX_M_AV_NOCONDOM_DATE,
      EXPOSE_SEX_F_AV_DATE,
      EXPOSE_SEX_F_AV_NOCONDOM_DATE,
      TEST_AGREED   = SCREEN_AGREED,
      MODALITY,
      TEST_DATE     = T0_DATE,
      TEST_RESULT   = T0_RESULT,
      T1_DATE,
      T1_RESULT,
      DATE_CONFIRM,
      CONFIRM_RESULT,
      REACH_CLINICAL,
      REACH_ONLINE,
      REACH_INDEX_TESTING,
      REACH_SSNT,
      REACH_VENUE,
      CLINIC_NOTES,
      COUNSEL_NOTES,
      CREATED_BY
   ) %>%
   collect()

.log_info("Getting CFBS Form data.")
form_cfbs <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_cfbs")) %>%
   filter(
      RECORD_DATE >= start,
      RECORD_DATE <= end
   ) %>%
   mutate(
      FORM = "CFBS Form"
   ) %>%
   select(
      REC_ID,
      FORM,
      PATIENT_ID,
      UIC,
      SEX,
      FACI_ID,
      SELF_IDENT,
      SELF_IDENT_OTHER,
      TEST_FACI     = SERVICE_FACI,
      TEST_SUB_FACI = SERVICE_SUB_FACI,
      SERVICE_BY,
      RECORD_DATE,
      TEST_MSM      = CFBS_MSM,
      TEST_TGW      = CFBS_TGW,
      NUM_F_PARTNER,
      NUM_M_PARTNER,
      HIV_SERVICE_ADDR,
      RISK_M_SEX_ORAL_ANAL,
      RISK_CONDOMLESS_ANAL,
      RISK_CONDOMLESS_ANAL_DATE,
      RISK_CONDOMLESS_VAGINAL,
      RISK_CONDOMLESS_VAGINAL_DATE,
      TEST_AGREED   = SCREEN_AGREED,
      MODALITY,
      TEST_DATE,
      TEST_RESULT,
      CLINIC_NOTES,
      COUNSEL_NOTES,
      CREATED_BY
   ) %>%
   collect()

.log_info("Getting ever PrEP data.")
everprep <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_prep")) %>%
   filter(!is.na(MEDICINE_SUMMARY)) %>%
   collect() %>%
   distinct(PATIENT_ID) %>%
   left_join(
      y  = id_registry,
      by = 'PATIENT_ID'
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      )
   ) %>%
   distinct(CENTRAL_ID) %>%
   mutate(EVERONPREP = "Y")

.log_info("Getting latest ART visit data.")
latest_art <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "art_last")) %>%
   select(CENTRAL_ID, REC_ID, LATEST_VISIT = VISIT_DATE) %>%
   left_join(
      y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_art_bc")) %>%
         select(
            REC_ID,
            VISIT_DATE,
            FACI_ID,
            SERVICE_FACI,
            SERVICE_SUB_FACI
         ),
      by = "REC_ID"
   ) %>%
   collect() %>%
   filter(LATEST_VISIT == VISIT_DATE) %>%
   mutate(
      SERVICE_SUB_FACI = if_else(
         condition = is.na(SERVICE_FACI),
         true      = "",
         false     = SERVICE_SUB_FACI
      ),
      SERVICE_SUB_FACI = if_else(
         condition = is.na(SERVICE_SUB_FACI),
         true      = "",
         false     = SERVICE_SUB_FACI
      ),
      SERVICE_FACI     = if_else(
         condition = is.na(SERVICE_FACI),
         true      = FACI_ID,
         false     = SERVICE_FACI
      ),
   ) %>%
   left_join(
      y  = ohasis$ref_faci %>%
         select(SERVICE_FACI = FACI_ID, SERVICE_SUB_FACI = SUB_FACI_ID, TX_REGION = FACI_NAME_REG, TX_FACI = FACI_NAME),
      by = c("SERVICE_FACI", "SERVICE_SUB_FACI")
   ) %>%
   distinct_all()

.log_info("Getting earliest positive result data.")
earliest_pos <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_a")) %>%
   union_all(tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_hts"))) %>%
   # keep only positive results
   filter(substr(CONFIRM_RESULT, 1, 1) == "1") %>%
   # get latest central ids
   left_join(
      y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "id_registry")) %>%
         select(CENTRAL_ID, PATIENT_ID),
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   ) %>%
   select(CENTRAL_ID, DATE_CONFIRM) %>%
   collect() %>%
   arrange(DATE_CONFIRM) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   select(CENTRAL_ID, DATE_CONFIRM)

##  Combine testing data  ------------------------------------------------------

.log_info("Creating unified reach dataset.")
reach <- form_a %>%
   bind_rows(form_hts) %>%
   bind_rows(form_cfbs) %>%
   # get cid for merging
   left_join(
      y  = id_registry,
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
      MODALITY   = case_when(
         StrLeft(MODALITY, 6) == "101101" ~ "FBT",
         StrLeft(MODALITY, 6) == "101103" ~ "CBS",
         StrLeft(MODALITY, 6) == "101104" ~ "Non-laboratory FBT",
         StrLeft(MODALITY, 6) == "101105" ~ "Self-testing",
         StrLeft(MODALITY, 6) == "101304" ~ "Reach",
      ),
      .before    = 1
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~if_else(!is.na(.) & stri_detect_fixed(., '_'), substr(., stri_locate_first_fixed(., '_') + 1, 1000), .)
   ) %>%
   # unified facility variable for matching w/ DSA signed
   mutate(
      HTS_SUB_FACI = if_else(
         condition = !is.na(TEST_FACI),
         true      = TEST_SUB_FACI,
         false     = NA_character_
      ),
      HTS_SUB_FACI = if_else(
         condition = !is.na(HTS_SUB_FACI) & StrLeft(HTS_SUB_FACI, 6) == TEST_FACI,
         true      = HTS_SUB_FACI,
         false     = NA_character_
      ),
      HTS_FACI     = if_else(
         condition = !is.na(TEST_FACI),
         true      = TEST_FACI,
         false     = FACI_ID
      ),
   ) %>%
   left_join(
      y  = ohasis$ref_faci %>%
         mutate(
            SUB_FACI_ID = if_else(
               condition = SUB_FACI_ID != "",
               true      = SUB_FACI_ID,
               false     = NA_character_
            ),
         ) %>%
         select(
            HTS_FACI     = FACI_ID,
            HTS_SUB_FACI = SUB_FACI_ID,
            HTS_SITE     = FACI_NAME,
            HTS_REG      = FACI_NHSSS_REG,
            HTS_PROV     = FACI_NHSSS_PROV,
            HTS_MUNC     = FACI_NHSSS_MUNC,
         ),
      by = c("HTS_FACI", "HTS_SUB_FACI")
   ) %>%
   select(-FACI_ID, -HTS_SUB_FACI, -HTS_FACI, -TEST_FACI)

##  Combine testing data and generate taggings ---------------------------------

# combine all testing forms
.log_info("Generating logsheet.")
gf$logsheet <- reach %>%
   rename(FINAL_RESULT = CONFIRM_RESULT) %>%
   select(-contains("CONFIRM")) %>%
   # registry data for old dx
   left_join(
      y  = nhsss$harp_dx$official$new %>%
         mutate(
            CENTRAL_MSM = if_else(
               condition = sex == "MALE" & sexhow %in% c("HOMOSEXUAL", "BISEXUAL"),
               true      = 1,
               false     = 0,
               missing   = 0
            ),
            CENTRAL_TGW = if_else(
               condition = sex == "MALE" & self_identity %in% c("FEMALE", "WOMAN"),
               true      = 1,
               false     = 0,
               missing   = 0
            )
         ) %>%
         select(
            CENTRAL_ID,
            CENTRAL_SEX       = sex,
            CENTRAL_BIRTHDATE = bdate,
            CENTRAL_MSM,
            CENTRAL_TGW,
            confirm_date,
            transmit
         ),
      by = "CENTRAL_ID"
   ) %>%
   # unified facility variable for matching w/ DSA signed
   mutate(
      GF_SUB_FACI = if_else(
         condition = !is.na(TEST_FACI),
         true      = TEST_SUB_FACI,
         false     = ""
      ),
      GF_SUB_FACI = if_else(
         condition = !is.na(GF_SUB_FACI),
         true      = GF_SUB_FACI,
         false     = ""
      ),
      GF_FACI     = if_else(
         condition = !is.na(TEST_FACI),
         true      = TEST_FACI,
         false     = FACI_ID
      ),
   ) %>%
   inner_join(
      y  = gf$sites %>%
         filter(WITH_DSA == 1) %>%
         select(GF_FACI = FACI_ID),
      by = "GF_FACI"
   ) %>%
   # get everonprep
   left_join(
      y  = everprep,
      by = "CENTRAL_ID"
   ) %>%
   # generate indicators
   mutate(
      # tagging for specific columns
      reach_online     = if_else(
         condition = REACH_ONLINE == 1,
         true      = "online",
         false     = NA_character_,
         missing   = NA_character_
      ),
      reach_clinical   = if_else(
         condition = REACH_CLINICAL == 1,
         true      = "clinical",
         false     = NA_character_,
         missing   = NA_character_
      ),
      reach_index      = if_else(
         condition = REACH_INDEX_TESTING == 1,
         true      = "index",
         false     = NA_character_,
         missing   = NA_character_
      ),
      reach_ssnt       = if_else(
         condition = REACH_SSNT == 1,
         true      = "SSNT",
         false     = NA_character_,
         missing   = NA_character_
      ),
      reach_physical   = if_else(
         condition = REACH_VENUE == 1,
         true      = "physical",
         false     = NA_character_,
         missing   = NA_character_
      ),

      # special TGW tagging for GF
      TEST_TGW         = if_else(
         condition = StrLeft(SEX, 1) == "1" & StrLeft(SELF_IDENT, 1) == "2",
         true      = 1,
         false     = 0,
         missing   = 0
      ),

      # final KAP
      KAP              = case_when(
         !is.na(transmit) &
            CENTRAL_MSM == 1 &
            CENTRAL_TGW == 0 ~ "MSM",
         !is.na(transmit) &
            CENTRAL_MSM == 1 &
            CENTRAL_TGW == 1 ~ "TGW",
         is.na(transmit) &
            TEST_MSM == 1 &
            (is.na(TEST_TGW) | TEST_TGW == 0) ~ "MSM",
         is.na(transmit) & TEST_MSM == 1 & TEST_TGW == 1 ~ "TGW",
         TRUE ~ "OTHERS"
      ),

      # reference for p12m of sexual risk
      RECORD_P12M      = RECORD_DATE %m-% months(12),
      DATE_LAST_M      = case_when(
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ EXPOSE_SEX_M_AV_DATE,
         RECORD_P12M <= EXPOSE_SEX_M_AV_NOCONDOM_DATE & is.na(EXPOSE_SEX_M_AV_DATE) ~ EXPOSE_SEX_M_AV_NOCONDOM_DATE,
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE > EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ EXPOSE_SEX_M_AV_DATE,
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE < EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ EXPOSE_SEX_M_AV_NOCONDOM_DATE,
      ),
      SEX_ORAL         = case_when(
         StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "1" ~ "Y",
         StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "2" ~ "Y",
         StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "0" ~ "N",
         TRUE ~ "N"
      ),
      SEX_ANAL_RECEIVE = case_when(
         StrLeft(RISK_CONDOMLESS_ANAL, 1) == "1" ~ "Y",
         StrLeft(RISK_CONDOMLESS_ANAL, 1) == "2" ~ "Y",
         StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "1" ~ "Y",
         StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "2" ~ "Y",
         StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "0" ~ "N",
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ "Y",
         RECORD_P12M <= EXPOSE_SEX_M_AV_NOCONDOM_DATE & is.na(EXPOSE_SEX_M_AV_DATE) ~ "Y",
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE > EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "Y",
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE < EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "Y",
         TRUE ~ "N"
      ),
      SEX_ANAL_INSERT  = case_when(
         StrLeft(RISK_CONDOMLESS_ANAL, 1) == "1" ~ "Y",
         StrLeft(RISK_CONDOMLESS_ANAL, 1) == "2" ~ "Y",
         StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "1" ~ "Y",
         StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "2" ~ "Y",
         StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "0" ~ "N",
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ "Y",
         RECORD_P12M <= EXPOSE_SEX_M_AV_NOCONDOM_DATE & is.na(EXPOSE_SEX_M_AV_DATE) ~ "Y",
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE > EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "Y",
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE < EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "Y",
         TRUE ~ "N"
      ),
      SEX_VAGINAL      = case_when(
         StrLeft(RISK_CONDOMLESS_VAGINAL, 1) == "1" ~ "Y",
         StrLeft(RISK_CONDOMLESS_VAGINAL, 1) == "0" ~ "N",
         StrLeft(RISK_CONDOMLESS_VAGINAL, 1) == "2" ~ "N",
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE ~ "Y",
         RECORD_P12M <= EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "Y",
         RECORD_P12M <= EXPOSE_SEX_F_AV_DATE ~ "Y",
         RECORD_P12M <= EXPOSE_SEX_F_AV_NOCONDOM_DATE ~ "Y",
         !is.na(EXPOSE_SEX_M_AV_DATE) ~ "Y",
         !is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ "Y",
         !is.na(EXPOSE_SEX_F_AV_DATE) ~ "Y",
         !is.na(EXPOSE_SEX_F_AV_NOCONDOM_DATE) ~ "Y",
         TRUE ~ "N"
      ),
      CONDOM_STAGE     = "N/A",
      TESTED           = case_when(
         StrLeft(TEST_AGREED, 1) == "0" ~ "Not Tested",
         StrLeft(TEST_AGREED, 1) == "1" & StrLeft(MODALITY, 6) == "101101" ~ "Facility/Clinic (by MedTech)",
         StrLeft(TEST_AGREED, 1) == "1" & StrLeft(MODALITY, 6) == "101104" ~ "Outreach/Community (by MedTech)",
         StrLeft(TEST_AGREED, 1) == "1" & StrLeft(MODALITY, 6) == "101103" ~ "CBS",
         StrLeft(TEST_AGREED, 1) == "1" & StrLeft(MODALITY, 6) == "101105" ~ "Self-testing / Self-testing (No Result)",
         StrLeft(MODALITY, 6) == "101304" ~ "Not Tested",
      ),
      EVERONPREP       = if_else(is.na(EVERONPREP), "N", "Y"),
      REACTIVE         = case_when(
         StrLeft(TEST_RESULT, 1) == "1" ~ "Y",
         StrLeft(T1_RESULT, 1) == "1" ~ "Y",
         StrLeft(TEST_RESULT, 1) == "2" ~ "N",
         StrLeft(TEST_RESULT, 1) == "3" ~ "N",
         StrLeft(T1_RESULT, 1) == "2" ~ "N",
         StrLeft(T1_RESULT, 1) == "3" ~ "N",
         StrLeft(FINAL_RESULT, 1) == "1" ~ "Y",
         StrLeft(FINAL_RESULT, 1) == "2" ~ "N",
         StrLeft(FINAL_RESULT, 1) == "3" ~ "N",
         StrLeft(FINAL_RESULT, 1) == "4" ~ "N",
         TRUE ~ "N"
      ),
      TEST_DATE        = case_when(
         is.na(TEST_DATE) & !is.na(TEST_RESULT) ~ RECORD_DATE,
         is.na(TEST_DATE) & !is.na(T1_RESULT) ~ RECORD_DATE,
         TRUE ~ as.Date(TEST_DATE)
      ),
      DOH_FORM         = glue::glue("DOH-EB Form: {FORM}")
   ) %>%
   unite(
      col   = "VENUE_TYPE",
      sep   = " ",
      starts_with("reach_", ignore.case = FALSE),
      na.rm = TRUE
   ) %>%
   unite(
      col   = "REMARKS",
      sep   = " ",
      ends_with("_NOTES", ignore.case = FALSE),
      na.rm = TRUE
   ) %>%
   filter(KAP != "OTHERS") %>%
   left_join(
      y  = reach %>%
         filter(!is.na(T1_RESULT)) %>%
         arrange(desc(T1_DATE)) %>%
         distinct(CENTRAL_ID) %>%
         mutate(T1_COMPLETE = "Y"),
      by = 'CENTRAL_ID'
   ) %>%
   left_join(
      y  = earliest_pos,
      by = 'CENTRAL_ID'
   ) %>%
   left_join(
      y  = nhsss$harp_tx$official$new_reg %>%
         select(CENTRAL_ID, ARTSTART_DATE = artstart_date),
      by = 'CENTRAL_ID'
   ) %>%
   left_join(
      y  = latest_art %>%
         select(CENTRAL_ID, TX_FACI, TX_REGION),
      by = 'CENTRAL_ID'
   ) %>%
   mutate(
      T1_COMPLETE    = if_else(
         condition = !is.na(T1_COMPLETE),
         true      = T1_COMPLETE,
         false     = "N"
      ),
      POSITIVE       = case_when(
         !is.na(transmit) ~ "Y",
         !is.na(DATE_CONFIRM) ~ "Y",
         TRUE ~ "N"
      ),
      DATE_CONFIRMED = case_when(
         !is.na(transmit) ~ confirm_date,
         !is.na(DATE_CONFIRM) ~ as.Date(DATE_CONFIRM)
      ),
      SELF_IDENT     = case_when(
         StrLeft(SELF_IDENT, 1) == "1" ~ "MAN",
         StrLeft(SELF_IDENT, 1) == "2" ~ "WOMAN",
         StrLeft(SELF_IDENT, 1) == "3" ~ "OTHER",
      )
   ) %>%
   left_join(
      y  = ohasis$ref_faci %>%
         select(
            GF_FACI     = FACI_ID,
            GF_SUB_FACI = SUB_FACI_ID,
            GF_SITE     = FACI_NAME,
            GF_REG      = FACI_NAME_REG,
            GF_PROV     = FACI_NAME_PROV,
            GF_MUNC     = FACI_NAME_MUNC,
         ),
      by = c("GF_FACI", "GF_SUB_FACI")
   ) %>%
   distinct_all() %>%
   left_join(
      y  = ohasis$ref_staff %>%
         select(SERVICE_BY = STAFF_ID, PROVIDER = STAFF_NAME),
      by = "SERVICE_BY"
   ) %>%
   distinct_all() %>%
   left_join(
      y  = ohasis$ref_staff %>%
         select(CREATED_BY = STAFF_ID, ENCODER = STAFF_NAME),
      by = "CREATED_BY"
   ) %>%
   distinct_all() %>%
   # format dates
   mutate_at(
      .vars = vars(RECORD_DATE, DATE_LAST_M, TEST_DATE, ARTSTART_DATE, DATE_CONFIRMED),
      ~as.Date(.)
   ) %>%
   arrange(RECORD_DATE, DATE_CONFIRMED) %>%
   distinct(CENTRAL_ID, GF_SITE, RECORD_DATE, MODALITY, .keep_all = TRUE) %>%
   arrange(GF_SITE, RECORD_DATE) %>%
   select(
      ohasis_id            = CENTRAL_ID,
      site_region          = GF_REG,
      site_province        = GF_PROV,
      site_muncity         = GF_MUNC,
      site_name            = GF_SITE,
      provider_name        = PROVIDER,
      encoder_name         = ENCODER,
      reach_date           = RECORD_DATE,
      venue_type           = VENUE_TYPE,
      venue                = HIV_SERVICE_ADDR,
      uic                  = UIC,
      kap_type             = KAP,
      self_identity        = SELF_IDENT,
      self_identity_text   = SELF_IDENT_OTHER,
      date_last_sex_msm    = DATE_LAST_M,
      sextype_oral         = SEX_ORAL,
      sextype_anal_insert  = SEX_ANAL_INSERT,
      sextype_anal_receive = SEX_ANAL_RECEIVE,
      sextype_vaginal      = SEX_VAGINAL,
      num_sex_partner_m    = NUM_M_PARTNER,
      num_sex_partner_f    = NUM_F_PARTNER,
      stage_condom_use     = CONDOM_STAGE,
      tested               = TESTED,
      test_date            = TEST_DATE,
      reactive             = REACTIVE,
      everonprep           = EVERONPREP,
      t1_complete          = T1_COMPLETE,
      remarks              = REMARKS,
      doh_eb_form          = DOH_FORM,
      confirmed_positive   = POSITIVE, ,
      confirm_date         = DATE_CONFIRMED,
      artstart_date        = ARTSTART_DATE,
      tx_hub               = TX_FACI,
      tx_region            = TX_REGION
   )

gf$dir  <- glue(r"(H:/Data Sharing/PSFI/{ohasis$ym})")
gf$file <- glue(r"({gf$dir}/{format(Sys.time(), "%Y%m%d")}_gf-logsheet_{ohasis$yr}-{ohasis$mo}.dta)")
check_dir(gf$dir)

write_dta(
   gf$logsheet,
   gf$file
)

stata(
   glue(r"(
u "{gf$file}", clear

ds, has(type string)
foreach var in `r(varlist)' {{
   loc type : type `var'
   loc len = substr("`type'", 4, 1000)

   cap form `var' %-`len's
}}

form *date* *DATE* %tdCCYY-NN-DD
compress
sa "{gf$file}", replace

   )")
)
