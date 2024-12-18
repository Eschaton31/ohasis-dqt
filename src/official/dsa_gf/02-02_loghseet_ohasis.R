##  Append testing data  -------------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

##  Combine testing data  ------------------------------------------------------

.log_info("Creating unified reach dataset.")
hts_data <- process_hts(gf$forms$form_hts, gf$forms$form_a, gf$forms$form_cfbs) %>%
   get_cid(gf$forms$id_registry, PATIENT_ID)
reach    <- hts_data %>%
   mutate(reach_date = hts_date) %>%
   filter(
      reach_date >= as.Date(gf$coverage$min),
      reach_date <= as.Date(gf$coverage$max)
   ) %>%
   rename(
      TEST_FACI     = SERVICE_FACI,
      TEST_SUB_FACI = SERVICE_SUB_FACI
   ) %>%
   mutate(
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
   mutate(
      GF_FACI_2 = GF_FACI,
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
   rename(GF_FACI = GF_FACI_2) %>%
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
            GF_FACI    = FACI_ID,
            LS_SUBTYPE = `Clinic Type`,
            with_dsa   = starts_with("DSA (")
         ) %>%
         distinct_all() %>%
         add_row(GF_FACI = "130605", LS_SUBTYPE = "CBO") %>%
         bind_rows(
            ohasis$ref_faci %>%
               filter(
                  !(FACI_NHSSS_REG %in% c("CARAGA", "ARMM", "1", "2", "5", "8"))
               ) %>%
               inner_join(
                  y  = gf$forms$service_art %>% select(FACI_ID),
                  by = "FACI_ID"
               ) %>%
               anti_join(
                  y  = gf$sites %>%
                     filter(!is.na(FACI_ID)) %>%
                     select(FACI_ID),
                  by = "FACI_ID"
               ) %>%
               select(GF_FACI = FACI_ID)
         ) %>%
         distinct_all() %>%
         mutate(
            with_dsa     = case_when(
               with_dsa == TRUE ~ 1,
               with_dsa == FALSE ~ 0,
               TRUE ~ 0
            ),
            site_gf_2022 = 1,
            LS_SUBTYPE   = if_else(
               condition = is.na(LS_SUBTYPE),
               true      = "Treatment Hub",
               false     = LS_SUBTYPE,
               missing   = LS_SUBTYPE
            )
         ) %>%
         mutate(
            LS_SUBTYPE = if_else(
               condition = GF_FACI %in% c("130001", "070021"),
               true      = "TLY",
               false     = LS_SUBTYPE,
               missing   = LS_SUBTYPE
            )
         ) %>%
         distinct_all(),
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
         condition = str_left(SEX, 1) == "1" & str_left(SELF_IDENT, 1) == "2",
         true      = 1,
         false     = 0,
         missing   = 0
      ),

      SEX              = case_when(
         CENTRAL_SEX == "MALE" ~ "M",
         CENTRAL_SEX == "FEMALE" ~ "F",
         TRUE ~ substr(SEX, 3, 3)
      ),

      SELF_IDENT       = str_left(SELF_IDENT, 1),
      SELF_IDENT       = case_when(
         SELF_IDENT == "1" ~ "M",
         SELF_IDENT == "2" ~ "F",
         SELF_IDENT == "3" ~ "O",
      ),


      msm              = if_else(SEX == "M" & grepl("yes-", risk_sexwithm), 1, 0),
      tgw              = if_else(SEX == "M" & SELF_IDENT == "F", 1, 0),
      pwid             = if_else(grepl("yes-", risk_injectdrug), 1, 0),
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
         # is.na(transmit) &
         #    TEST_MSM == 1 &
         #    (is.na(TEST_TGW) | TEST_TGW == 0) ~ "MSM",
         # is.na(transmit) & TEST_MSM == 1 & TEST_TGW == 1 ~ "TGW",
         is.na(transmit) & msm == 1 & tgw == 0 ~ "MSM",
         is.na(transmit) & msm == 1 & tgw == 1 ~ "TGW",
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
         str_left(EXPOSE_M_SEX_ORAL_ANAL, 1) == "1" ~ "Y",
         str_left(EXPOSE_M_SEX_ORAL_ANAL, 1) == "2" ~ "Y",
         str_left(EXPOSE_M_SEX_ORAL_ANAL, 1) == "0" ~ "N",
         TRUE ~ "N"
      ),
      SEX_ANAL_RECEIVE = case_when(
         risk_sexwithm %in% c("yes-p01m", "yes-p03m", "yes-p06m", "yes-p12m") ~ "Y",
         # RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ "Y",
         # RECORD_P12M <= EXPOSE_SEX_M_AV_NOCONDOM_DATE & is.na(EXPOSE_SEX_M_AV_DATE) ~ "Y",
         # RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE > EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "Y",
         # RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE < EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "Y",
         # str_left(EXPOSE_M_SEX_ORAL_ANAL, 1) == "1" ~ "Y",
         # str_left(EXPOSE_M_SEX_ORAL_ANAL, 1) == "2" ~ "Y",
         # str_left(EXPOSE_CONDOMLESS_ANAL, 1) == "1" ~ "Y",
         # str_left(EXPOSE_CONDOMLESS_ANAL, 1) == "2" ~ "Y",
         # str_left(EXPOSE_SEX_M_NOCONDOM, 1) == "1" ~ "Y",
         TRUE ~ "N"
      ),
      SEX_ANAL_INSERT  = case_when(
         risk_sexwithm %in% c("yes-p01m", "yes-p03m", "yes-p06m", "yes-p12m") ~ "Y",
         # RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ "Y",
         # RECORD_P12M <= EXPOSE_SEX_M_AV_NOCONDOM_DATE & is.na(EXPOSE_SEX_M_AV_DATE) ~ "Y",
         # RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE > EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "Y",
         # RECORD_P12M <= EXPOSE_SEX_M_AV_DATE & EXPOSE_SEX_M_AV_DATE < EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "Y",
         # str_left(EXPOSE_M_SEX_ORAL_ANAL, 1) == "1" ~ "Y",
         # str_left(EXPOSE_M_SEX_ORAL_ANAL, 1) == "2" ~ "Y",
         # str_left(EXPOSE_CONDOMLESS_ANAL, 1) == "1" ~ "Y",
         # str_left(EXPOSE_CONDOMLESS_ANAL, 1) == "2" ~ "Y",
         # str_left(EXPOSE_SEX_M_NOCONDOM, 1) == "1" ~ "Y",
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
         str_left(EXPOSE_M_SEX_ORAL_ANAL, 1) == "1" ~ "w/in 12m",
         str_left(EXPOSE_M_SEX_ORAL_ANAL, 1) == "2" ~ ">p12m",
         str_left(EXPOSE_SEX_M_NOCONDOM, 1) == "2" ~ ">p12m",
         str_left(EXPOSE_CONDOMLESS_ANAL, 1) == "1" ~ "w/in 12m",
         str_left(EXPOSE_CONDOMLESS_ANAL, 1) == "2" ~ ">p12m",
         str_left(EXPOSE_SEX_M_NOCONDOM, 1) == "1" ~ "w/in 12m",
         SEX_ANAL_INSERT == "Y" | SEX_ANAL_RECEIVE == "Y" ~ ">p12m",
         TRUE ~ "(no data)"
      ),
      SEX_VAGINAL      = case_when(
         risk_sexwithf %in% c("yes-p01m", "yes-p03m", "yes-p06m", "yes-p12m") ~ "Y",
         # str_left(EXPOSE_CONDOMLESS_VAGINAL, 1) == "1" ~ "Y",
         #  str_left(EXPOSE_CONDOMLESS_VAGINAL, 1) == "0" ~ "N",
         #  str_left(EXPOSE_CONDOMLESS_VAGINAL, 1) == "2" ~ "N",
         #  RECORD_P12M <= EXPOSE_SEX_M_AV_DATE ~ "Y",
         #  RECORD_P12M <= EXPOSE_SEX_M_AV_NOCONDOM_DATE ~ "Y",
         #  RECORD_P12M <= EXPOSE_SEX_F_AV_DATE ~ "Y",
         #  RECORD_P12M <= EXPOSE_SEX_F_AV_NOCONDOM_DATE ~ "Y",
         #  !is.na(EXPOSE_SEX_M_AV_DATE) ~ "Y",
         #  !is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ "Y",
         #  !is.na(EXPOSE_SEX_F_AV_DATE) ~ "Y",
         #  !is.na(EXPOSE_SEX_F_AV_NOCONDOM_DATE) ~ "Y",
         TRUE ~ "N"
      ),
      CONDOM_STAGE     = "N/A",
      TESTED           = case_when(
         str_left(SCREEN_AGREED, 1) == "0" ~ "Not Tested",
         hts_modality == "FBT" ~ "Facility/Clinic (by MedTech)",
         hts_modality == "FBS" ~ "Outreach/Community (by MedTech)",
         hts_modality == "CBS" ~ "CBS",
         hts_modality == "ST" ~ "Self-testing / Self-testing (No Result)",
         # !is.na(TEST_DATE) &
         #    is.na(MODALITY) &
         #    GF_FACI == "130605" ~ "CBS",
         # !is.na(TEST_DATE) &
         #    is.na(MODALITY) &
         #    GF_FACI != "130605" ~ "Facility/Clinic (by MedTech)",
         hts_modality == "REACH" ~ "Not Tested",
         is.na(hts_date) ~ "Not Tested",
         TRUE ~ hts_modality
      ),
      EVERONPREP       = if_else(!is.na(prepstart_date), "N", "Y"),
      REACTIVE         = case_when(
         hts_result == "R" ~ "Y",
         # str_left(TEST_RESULT, 1) == "1" ~ "Y",
         # str_left(T1_RESULT, 1) == "1" ~ "Y",
         # str_left(TEST_RESULT, 1) == "2" ~ "N",
         # str_left(TEST_RESULT, 1) == "3" ~ "N",
         # str_left(T1_RESULT, 1) == "2" ~ "N",
         # str_left(T1_RESULT, 1) == "3" ~ "N",
         # str_left(FINAL_RESULT, 1) == "1" ~ "Y",
         # str_left(FINAL_RESULT, 1) == "2" ~ "N",
         # str_left(FINAL_RESULT, 1) == "3" ~ "N",
         # str_left(FINAL_RESULT, 1) == "4" ~ "N",
         TRUE ~ "N"
      ),
      TEST_DATE        = case_when(
         # is.na(TEST_DATE) & !is.na(TEST_RESULT) ~ RECORD_DATE,
         # is.na(TEST_DATE) & !is.na(T1_RESULT) ~ RECORD_DATE,
         TESTED != "Not Tested" ~ hts_date
      ),
      # FORM             = case_when(
      #    src == "hts2021" ~ "HTS Form",
      #    src == "a2017" ~ "Form A",
      #    src == "cfbs2020" ~ "CFBS Form",
      # ),
      FORM             = case_when(
         FORM_VERSION == "HTS Form (v2021)" ~ "HTS Form",
         FORM_VERSION == "Form A (v2017)" ~ "Form A",
         FORM_VERSION == "CFBS Form (v2020)" ~ "CFBS Form",
      ),
      DOH_FORM         = glue::glue("DOH-EB Form: {FORM}"),
   ) %>%
   unite(
      col   = "VENUE_TYPE",
      sep   = " ",
      starts_with("reach_", ignore.case = FALSE) & !matches("reach_date"),
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
      y  = hts_data %>%
         mutate(
            # use_test   = case_when(
            #    !is.na(TEST_RESULT) ~ "t0",
            #    is.na(TEST_RESULT) & !is.na(T1_RESULT) ~ "t1",
            #    is.na(TEST_RESULT) &
            #       is.na(T1_RESULT) &
            #       !is.na(CONFIRM_RESULT) ~ "confirm",
            # ),
            # T1_DATE    = case_when(
            #    use_test == "t0" ~ as.Date(TEST_DATE),
            #    use_test == "t1" ~ as.Date(T1_DATE),
            #    use_test == "confirm" ~ as.Date(RECORD_DATE),
            #    TRUE ~ RECORD_DATE
            # ),
            # T1_RESULT  = case_when(
            #    use_test == "t0" ~ str_left(TEST_RESULT, 1),
            #    use_test == "t1" ~ str_left(T1_RESULT, 1),
            #    use_test == "confirm" ~ str_left(CONFIRM_RESULT, 1),
            #    TRUE ~ NA_character_
            # ),
            T1_DATE   = hts_date,
            T1_RESULT = hts_result
         ) %>%
         filter(
            T1_RESULT != "(no data)",
            hts_modality == "FBT"
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
         condition = reach_date <= T1_GF_DATE,
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
         SELF_IDENT == "M" ~ "MAN",
         SELF_IDENT == "F" ~ "WOMAN",
         SELF_IDENT == "O" ~ "OTHER",
      ),
   ) %>%
   distinct_all() %>%
   # format dates
   mutate_at(
      .vars = vars(RECORD_DATE, DATE_LAST_M, TEST_DATE, ARTSTART_DATE, DATE_CONFIRMED),
      ~as.Date(.)
   ) %>%
   arrange(RECORD_DATE, DATE_CONFIRMED) %>%
   distinct(CENTRAL_ID, GF_SITE, RECORD_DATE, hts_modality, .keep_all = TRUE) %>%
   arrange(GF_SITE, reach_date) %>%
   # mutate(
   #    reach_date = case_when(
   #       TEST_DATE >= 2020 ~ TEST_DATE,
   #       TRUE ~ RECORD_DATE
   #    )
   # ) %>%
   filter(reach_date >= as.Date(gf$coverage$min)) %>%
   select(
      ohasis_id            = CENTRAL_ID,
      ohasis_record        = REC_ID,
      ls_subtype           = LS_SUBTYPE,
      site_region          = GF_REG,
      site_province        = GF_PROV,
      site_muncity         = GF_MUNC,
      site_name            = GF_SITE,
      provider_name        = PROVIDER,
      encoder_name         = ENCODER,
      reach_date,
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
      with_dsa,
      RECORD_P12M,
      starts_with("RISK"),
      starts_with("EXPOSE"),
      sex                  = SEX,
      site_gf_2022
   )

.log_success("Done!")
rm(list = setdiff(ls(), currEnv))