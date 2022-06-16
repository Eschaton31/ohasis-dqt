##  Append testing data  -------------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

##  Download relevant form data ------------------------------------------------

# Form A + HTS Forms
.log_info("Getting Form A data.")
form_a <- gf$forms$form_a %>%
   filter(
      RECORD_DATE >= as.Date(gf$coverage$min),
      RECORD_DATE <= as.Date(gf$coverage$max)
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
   )

.log_info("Getting HTS Form data.")
form_hts <- gf$forms$form_hts %>%
   filter(
      RECORD_DATE >= as.Date(gf$coverage$min),
      RECORD_DATE <= as.Date(gf$coverage$max)
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
   )

.log_info("Getting CFBS Form data.")
form_cfbs <- gf$forms$form_cfbs %>%
   filter(
      RECORD_DATE >= as.Date(gf$coverage$min),
      RECORD_DATE <= as.Date(gf$coverage$max)
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
   )

##  Combine testing data  ------------------------------------------------------

.log_info("Creating unified reach dataset.")
reach <- form_a %>%
   bind_rows(form_hts) %>%
   bind_rows(form_cfbs) %>%
   # get cid for merging
   left_join(
      y  = gf$forms$id_registry,
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID      = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),

      # tag those without form faci
      use_record_faci = if_else(
         condition = is.na(TEST_FACI),
         true      = 1,
         false     = 0
      ),
      TEST_FACI       = if_else(
         condition = use_record_faci == 1,
         true      = FACI_ID,
         false     = TEST_FACI
      ),
      .before         = 1
   ) %>%
   rename(
      GF_FACI     = TEST_FACI,
      GF_SUB_FACI = TEST_SUB_FACI
   ) %>%
   # unified facility variable for matching w/ DSA signed
   mutate(
      # tag those without service faci
      use_record_faci = if_else(
         condition = is.na(GF_FACI),
         true      = 1,
         false     = 0
      ),
      GF_FACI         = if_else(
         condition = use_record_faci == 1,
         true      = GF_FACI,
         false     = FACI_ID
      ),
   ) %>%
   select(-FACI_ID) %>%
   mutate_at(
      .var = vars(
         names(select(
            .,
            -REC_ID,
            -TEST_AGREED,
            -MODALITY,
            -starts_with("EXPOSE"),
            -starts_with("RISK"),
            -contains("RESULT")
         ) %>% select_if(is.character))
      ),
      ~remove_code(.)
   )

##  Facilities -----------------------------------------------------------------

.log_info("Attaching facility names (OHASIS versions).")
# art faci
reach <- ohasis$get_faci(
   reach,
   list("GF_SITE" = c("GF_FACI", "GF_SUB_FACI")),
   "name",
   c("GF_REG", "GF_PROV", "GF_MUNC")
)

##  Staff / Users --------------------------------------------------------------

.log_info("Attaching user names (OHASIS versions).")
# art faci
reach <- ohasis$get_staff(reach, c("ENCODER" = "CREATED_BY"))
reach <- ohasis$get_staff(reach, c("PROVIDER" = "SERVICE_BY"))

##  Combine testing data and generate taggings ---------------------------------

# combine all testing forms
.log_info("Generating logsheet.")
gf$logsheet$ohasis <- reach %>%
   rename(FINAL_RESULT = CONFIRM_RESULT) %>%
   select(-contains("CONFIRM")) %>%
   # registry data for old dx
   left_join(
      y  = gf$harp$dx %>%
         mutate(
            CENTRAL_MSM  = if_else(
               condition = sex == "MALE" & sexhow %in% c("HOMOSEXUAL", "BISEXUAL"),
               true      = 1,
               false     = 0,
               missing   = 0
            ),
            CENTRAL_TGW  = if_else(
               condition = sex == "MALE" & self_identity %in% c("FEMALE", "WOMAN"),
               true      = 1,
               false     = 0,
               missing   = 0
            ),
            CENTRAL_PWID = if_else(
               condition = transmit == "IVDU",
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
            CENTRAL_PWID,
            confirm_date,
            transmit,
            sexhow
         ),
      by = "CENTRAL_ID"
   ) %>%
   left_join(
      y  = gf$sites %>%
         filter(!is.na(FACI_ID)) %>%
         select(
            GF_FACI = FACI_ID
         ) %>%
         distinct_all() %>%
         add_row(GF_FACI = "130605") %>%
         bind_rows(
            ohasis$ref_faci %>%
               filter(
                  !(FACI_NHSSS_REG %in% c("CARAGA", "ARMM", "1", "2", "5", "8"))
               ) %>%
               inner_join(
                  y  = gf$forms$service_art %>% select(FACI_ID),
                  by = "FACI_ID"
               ) %>%
               select(GF_FACI = FACI_ID)
         ) %>%
         distinct_all() %>%
         mutate(site_gf_2022 = 1),
      by = "GF_FACI"
   ) %>%
   # get everonprep
   left_join(
      y  = gf$harp$prep$new_reg %>%
         left_join(
            y  = gf$harp$prep$new_outcome %>%
               select(-CENTRAL_ID),
            by = "prep_id"
         ) %>%
         select(
            CENTRAL_ID,
            prepstart_date
         ),
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

      SEX              = case_when(
         CENTRAL_SEX == "MALE" ~ "M",
         CENTRAL_SEX == "FEMALE" ~ "F",
         TRUE ~ StrLeft(SEX, 1)
      ),

      # final KAP
      KAP              = case_when(
         !is.na(transmit) &
            CENTRAL_MSM == 1 &
            CENTRAL_TGW == 0 ~ "MSM",
         !is.na(transmit) &
            CENTRAL_MSM == 1 &
            CENTRAL_TGW == 1 ~ "TGW",
         !is.na(transmit) &
            CENTRAL_PWID == 1 ~ "PWID",
         is.na(transmit) &
            TEST_MSM == 1 &
            (is.na(TEST_TGW) | TEST_TGW == 0) ~ "MSM",
         is.na(transmit) & TEST_MSM == 1 & TEST_TGW == 1 ~ "TGW",
         CENTRAL_SEX == "MALE" & sexhow == "HETEROSEXUAL" ~ "Hetero Male",
         CENTRAL_SEX == "FEMALE" & sexhow == "HETEROSEXUAL" ~ "Hetero Female",
         TRUE ~ "OTHERS"
      ),

      # reference for p12m of sexual risk
      RECORD_P6M       = RECORD_DATE %m-% months(6),
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
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ "Y",
         RECORD_P12M <= EXPOSE_SEX_M_AV_NOCONDOM_DATE & is.na(EXPOSE_SEX_M_AV_DATE) ~ "Y",
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE > EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "Y",
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE < EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "Y",
         StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "1" ~ "Y",
         StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "2" ~ "Y",
         StrLeft(RISK_CONDOMLESS_ANAL, 1) == "1" ~ "Y",
         StrLeft(RISK_CONDOMLESS_ANAL, 1) == "2" ~ "Y",
         StrLeft(EXPOSE_SEX_M_NOCONDOM, 1) == "1" ~ "Y",
         TRUE ~ "N"
      ),
      SEX_ANAL_INSERT  = case_when(
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ "Y",
         RECORD_P12M <= EXPOSE_SEX_M_AV_NOCONDOM_DATE & is.na(EXPOSE_SEX_M_AV_DATE) ~ "Y",
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE > EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "Y",
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE < EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "Y",
         StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "1" ~ "Y",
         StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "2" ~ "Y",
         StrLeft(RISK_CONDOMLESS_ANAL, 1) == "1" ~ "Y",
         StrLeft(RISK_CONDOMLESS_ANAL, 1) == "2" ~ "Y",
         StrLeft(EXPOSE_SEX_M_NOCONDOM, 1) == "1" ~ "Y",
         TRUE ~ "N"
      ),
      analp12m         = case_when(
         RECORD_P6M <= EXPOSE_SEX_M_AV_DATE & is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ "w/in 6m",
         RECORD_P6M <= EXPOSE_SEX_M_AV_NOCONDOM_DATE & is.na(EXPOSE_SEX_M_AV_DATE) ~ "w/in 6m",
         RECORD_P6M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE > EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "w/in 6m",
         RECORD_P6M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE < EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "w/in 6m",
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ "w/in 12m",
         RECORD_P12M <= EXPOSE_SEX_M_AV_NOCONDOM_DATE & is.na(EXPOSE_SEX_M_AV_DATE) ~ "w/in 12m",
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE > EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "w/in 12m",
         RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE < EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "w/in 12m",
         RECORD_P12M > EXPOSE_SEX_M_AV_DATE & is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ ">p12m",
         RECORD_P12M > EXPOSE_SEX_M_AV_NOCONDOM_DATE & is.na(EXPOSE_SEX_M_AV_DATE) ~ ">p12m",
         RECORD_P12M > EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE > EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ ">p12m",
         RECORD_P12M > EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE < EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ ">p12m",
         StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "1" ~ "w/in 12m",
         StrLeft(RISK_M_SEX_ORAL_ANAL, 1) == "2" ~ ">p12m",
         StrLeft(EXPOSE_SEX_M_NOCONDOM, 1) == "2" ~ ">p12m",
         StrLeft(RISK_CONDOMLESS_ANAL, 1) == "1" ~ "w/in 12m",
         StrLeft(RISK_CONDOMLESS_ANAL, 1) == "2" ~ ">p12m",
         StrLeft(EXPOSE_SEX_M_NOCONDOM, 1) == "1" ~ "w/in 12m",
         SEX_ANAL_INSERT == "Y" | SEX_ANAL_RECEIVE == "Y" ~ ">p12m",
         TRUE ~ "(no data)"
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
         StrLeft(MODALITY, 6) == "101101" ~ "Facility/Clinic (by MedTech)",
         StrLeft(MODALITY, 6) == "101104" ~ "Outreach/Community (by MedTech)",
         StrLeft(MODALITY, 6) == "101103" ~ "CBS",
         StrLeft(MODALITY, 6) == "101105" ~ "Self-testing / Self-testing (No Result)",
         !is.na(TEST_DATE) &
            is.na(MODALITY) &
            GF_FACI == "130605" ~ "CBS",
         !is.na(TEST_DATE) &
            is.na(MODALITY) &
            GF_FACI != "130605" ~ "Facility/Clinic (by MedTech)",
         StrLeft(MODALITY, 6) == "101304" ~ "Not Tested",
         is.na(TEST_DATE) ~ "Not Tested",
         TRUE ~ MODALITY
      ),
      EVERONPREP       = if_else(!is.na(prepstart_date), "N", "Y"),
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
   # filter(KAP != "OTHERS") %>%
   # left_join(
   #    y  = reach %>%
   #       filter(!is.na(T1_RESULT)) %>%
   #       arrange(desc(T1_DATE)) %>%
   #       distinct(CENTRAL_ID) %>%
   #       mutate(T1_COMPLETE = "Y"),
   #    by = "CENTRAL_ID"
   # ) %>%
   left_join(
      y  = form_a %>%
         bind_rows(form_hts) %>%
         # get cid for merging
         left_join(
            y  = gf$forms$id_registry,
            by = "PATIENT_ID"
         ) %>%
         mutate(
            CENTRAL_ID = if_else(
               condition = is.na(CENTRAL_ID),
               true      = PATIENT_ID,
               false     = CENTRAL_ID
            ),

            use_test   = case_when(
               !is.na(TEST_RESULT) ~ "t0",
               is.na(TEST_RESULT) & !is.na(T1_RESULT) ~ "t1",
               is.na(TEST_RESULT) &
                  is.na(T1_RESULT) &
                  !is.na(CONFIRM_RESULT) ~ "confirm",
            ),
            T1_DATE    = case_when(
               use_test == "t0" ~ as.Date(TEST_DATE),
               use_test == "t1" ~ as.Date(T1_DATE),
               use_test == "confirm" ~ as.Date(RECORD_DATE),
               TRUE ~ RECORD_DATE
            ),
            T1_RESULT  = case_when(
               use_test == "t0" ~ StrLeft(TEST_RESULT, 1),
               use_test == "t1" ~ StrLeft(T1_RESULT, 1),
               use_test == "confirm" ~ StrLeft(CONFIRM_RESULT, 1),
               TRUE ~ NA_character_
            ),
         ) %>%
         filter(
            !is.na(T1_RESULT),
            StrLeft(MODALITY, 6) == "101101"
         ) %>%
         arrange(desc(T1_DATE)) %>%
         distinct(CENTRAL_ID, .keep_all = TRUE) %>%
         select(
            CENTRAL_ID,
            T1_GF_DATE   = T1_DATE,
            T1_GF_RESULT = T1_RESULT
         ),
      by = "CENTRAL_ID"
   ) %>%
   left_join(
      y  = gf$harp$tx$new_reg %>%
         select(
            CENTRAL_ID,
            ARTSTART_DATE = artstart_date,
            art_id
         ),
      by = "CENTRAL_ID"
   ) %>%
   left_join(
      y  = gf$harp$tx$new_outcome %>%
         left_join(
            y  = ohasis$ref_faci_code %>%
               mutate(
                  FACI_CODE     = case_when(
                     stri_detect_regex(SUB_FACI_CODE, "^HASH") ~ "HASH",
                     stri_detect_regex(SUB_FACI_CODE, "^SAIL") ~ "SAIL",
                     stri_detect_regex(SUB_FACI_CODE, "^TLY") ~ "TLY",
                     TRUE ~ FACI_CODE
                  ),
                  SUB_FACI_CODE = if_else(
                     condition = nchar(SUB_FACI_CODE) == 3,
                     true      = NA_character_,
                     false     = SUB_FACI_CODE
                  ),
                  SUB_FACI_CODE = case_when(
                     FACI_CODE == "HASH" & is.na(SUB_FACI_CODE) ~ "HASH-QC",
                     FACI_CODE == "TLY" & is.na(SUB_FACI_CODE) ~ "TLY-ANGLO",
                     FACI_CODE == "SHP" & is.na(SUB_FACI_CODE) ~ "SHIP-MAKATI",
                     TRUE ~ SUB_FACI_CODE
                  ),
               ) %>%
               select(
                  FACI_ID,
                  realhub        = FACI_CODE,
                  realhub_branch = SUB_FACI_CODE,
                  TX_FACI        = FACI_NAME,
                  TX_REGION      = FACI_NAME_REG,
               ) %>%
               distinct_all(),
            by = c("realhub", "realhub_branch")
         ) %>%
         select(
            TX_FACI,
            TX_REGION,
            art_id
         ),
      by = "art_id"
   ) %>%
   mutate(
      T1_COMPLETE = if_else(
         condition = RECORD_DATE <= T1_GF_DATE,
         true      = "Y",
         false     = "N",
         missing   = "N"
      )
   ) %>%
   mutate(
      T1_COMPLETE    = if_else(
         condition = !is.na(T1_COMPLETE),
         true      = T1_COMPLETE,
         false     = "N"
      ),
      POSITIVE       = case_when(
         !is.na(transmit) ~ "Y",
         # !is.na(DATE_CONFIRM) ~ "Y",
         TRUE ~ "N"
      ),
      DATE_CONFIRMED = confirm_date,
      SELF_IDENT     = case_when(
         StrLeft(SELF_IDENT, 1) == "1" ~ "MAN",
         StrLeft(SELF_IDENT, 1) == "2" ~ "WOMAN",
         StrLeft(SELF_IDENT, 1) == "3" ~ "OTHER",
      ),
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
      ohasis_record        = REC_ID,
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
      tx_region            = TX_REGION,
      analp12m,
      RECORD_P12M,
      starts_with("RISK"),
      starts_with("EXPOSE"),
      sex                  = SEX,
      site_gf_2022
   )

.log_success("Done!")
rm(list = setdiff(ls(), currEnv))