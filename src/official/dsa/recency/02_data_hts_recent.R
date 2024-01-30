##  Initial Cleaning -----------------------------------------------------------

clean_data <- function(forms, harp) {
   local_gs4_quiet()
   log_info("Processing recent tests.")
   trace_validation <- read_sheet("1wmCemVcAtp5nUEpti1qacU7nFQqi8GE0_qT7PE0WtC0", .name_repair = "unique_quiet")

   data <- forms$hiv_recency %>%
      left_join(
         y  = process_hts(forms$form_hts, forms$form_a, forms$form_cfbs) %>%
            select(
               -any_of(c(
                  "RECORD_DATE",
                  "CONFIRM_FACI",
                  "CONFIRM_SUB_FACI",
                  "SPECIMEN_SOURCE",
                  "SPECIMEN_SUB_SOURCE",
                  "CONFIRM_CODE",
                  "DATE_COLLECT",
                  "DATE_RECEIVE",
                  "DATE_CONFIRM"
               ))
            ),
         by = join_by(REC_ID)
      ) %>%
      distinct(REC_ID, .keep_all = TRUE) %>%
      get_cid(forms$id_reg, PATIENT_ID)

   hts_risk <- data %>%
      select(
         REC_ID,
         contains("risk", ignore.case = FALSE)
      ) %>%
      pivot_longer(
         cols = contains("risk", ignore.case = FALSE)
      ) %>%
      group_by(REC_ID) %>%
      summarise(
         risks = stri_c(collapse = ", ", unique(sort(value)))
      )

   data %<>%
      mutate_at(
         .vars = vars(FIRST, MIDDLE, LAST, SUFFIX),
         ~coalesce(clean_pii(.), "")
      ) %>%
      mutate_at(
         .vars = vars(PATIENT_CODE, UIC, PHILHEALTH_NO, PHILSYS_ID, CLIENT_MOBILE, CLIENT_EMAIL),
         ~clean_pii(.)
      ) %>%
      mutate_if(
         .predicate = is.POSIXct,
         ~as.Date(.)
      ) %>%
      mutate_if(
         .predicate = is.Date,
         ~if_else(. <= -25567, NA_Date_, ., .)
      ) %>%
      mutate(
         RT_OFFER_DATE   = coalesce(DATE_COLLECT, RECORD_DATE),

         # name
         STANDARD_FIRST  = stri_trans_general(FIRST, "latin-ascii"),
         name            = str_squish(stri_c(LAST, ", ", FIRST, " ", MIDDLE, " ", SUFFIX)),

         # Permanent
         PERM_PSGC_PROV  = if_else(StrLeft(PERM_PSGC_REG, 2) == "99", "999900000", PERM_PSGC_PROV, PERM_PSGC_PROV),
         PERM_PSGC_MUNC  = if_else(StrLeft(PERM_PSGC_REG, 2) == "99", "999999000", PERM_PSGC_MUNC, PERM_PSGC_MUNC),
         use_curr        = if_else(
            condition = !is.na(CURR_PSGC_MUNC) & (is.na(PERM_PSGC_MUNC) | StrLeft(PERM_PSGC_MUNC, 2) == "99"),
            true      = 1,
            false     = 0
         ),
         PERM_PSGC_REG   = if_else(
            condition = use_curr == 1,
            true      = CURR_PSGC_REG,
            false     = PERM_PSGC_REG
         ),
         PERM_PSGC_PROV  = if_else(
            condition = use_curr == 1,
            true      = CURR_PSGC_PROV,
            false     = PERM_PSGC_PROV
         ),
         PERM_PSGC_MUNC  = if_else(
            condition = use_curr == 1,
            true      = CURR_PSGC_MUNC,
            false     = PERM_PSGC_MUNC
         ),

         # Age
         AGE_DTA         = calc_age(BIRTHDATE, RT_OFFER_DATE),
         AGE             = coalesce(AGE, AGE_MO / 12, AGE_DTA),

         # FORM_VERSION (HTS)
         FORM_VERSION    = if_else(FORM_VERSION == " (vNA)", NA_character_, FORM_VERSION),

         # provider type (HTS)
         PROVIDER_TYPE   = as.integer(keep_code(PROVIDER_TYPE)),

         # other services (HTS)
         GIVEN_SSNT      = case_when(
            SERVICE_SSNT_ACCEPT == 1 ~ "Accepted",
            SERVICE_SSNT_OFFER == 1 ~ "Offered",
         ),

         # combi prev (HTS)
         SERVICE_CONDOMS = if_else(SERVICE_CONDOMS == 0, NA_integer_, as.integer(SERVICE_CONDOMS), NA_integer_),
         SERVICE_LUBES   = if_else(SERVICE_LUBES == 0, NA_integer_, as.integer(SERVICE_LUBES), NA_integer_),
      ) %>%
      mutate(
         FORM_ENCODED = if_else(!is.na(FORM_VERSION), '1_Yes', "0_No", "0_No"),
         .after       = RT_RESULT
      ) %>%
      mutate(
         CBS_VENUE      = toupper(str_squish(HIV_SERVICE_ADDR)),
         ONLINE_APP     = case_when(
            grepl("GRINDR", CBS_VENUE) ~ "GRINDR",
            grepl("GRNDR", CBS_VENUE) ~ "GRINDR",
            grepl("GRINDER", CBS_VENUE) ~ "GRINDR",
            grepl("TWITTER", CBS_VENUE) ~ "TWITTER",
            grepl("FACEBOOK", CBS_VENUE) ~ "FACEBOOK",
            grepl("MESSENGER", CBS_VENUE) ~ "FACEBOOK",
            grepl("\\bFB\\b", CBS_VENUE) ~ "FACEBOOK",
            grepl("\\bGR\\b", CBS_VENUE) ~ "GRINDR",
         ),
         REACH_ONLINE   = if_else(!is.na(ONLINE_APP), "1_Yes", REACH_ONLINE, REACH_ONLINE),
         REACH_CLINICAL = if_else(
            condition = if_all(starts_with("REACH_"), ~is.na(.)) & hts_modality == "FBT",
            true      = "1_Yes",
            false     = REACH_CLINICAL,
            missing   = REACH_CLINICAL
         ),
      ) %>%
      select(-matches("risks")) %>%
      left_join(hts_risk, join_by(REC_ID)) %>%
      mutate(
         SEXUAL_RISK = case_when(
            str_detect(risk_sexwithm, "yes") & str_detect(risk_sexwithf, "yes") ~ "M+F",
            str_detect(risk_sexwithm, "yes") & !str_detect(risk_sexwithf, "yes") ~ "M",
            !str_detect(risk_sexwithm, "yes") & str_detect(risk_sexwithf, "yes") ~ "F",
         ),
         kap_unknown = if_else(coalesce(risks, "(no data)") == "(no data)", "(no data)", NA_character_),
         kap_msm     = if_else(SEX == "MALE" & SEXUAL_RISK %in% c("M", "M+F"), "MSM", NA_character_),
         kap_heterom = if_else(SEX == "MALE" & SEXUAL_RISK == "F", "Hetero Male", NA_character_),
         kap_heterof = if_else(SEX == "FEMALE" & !is.na(SEXUAL_RISK), "Hetero Female", NA_character_),
         kap_pwid    = if_else(str_detect(risk_injectdrug, "yes"), "PWID", NA_character_),
         kap_pip     = if_else(str_detect(risk_paymentforsex, "yes"), "PIP", NA_character_),
         kap_pdl     = case_when(
            StrLeft(CLIENT_TYPE, 1) == "7" ~ "PDL",
            StrLeft(CLIENT_TYPE, 1) == "7" ~ "PDL",
         ),
      ) %>%
      rename(
         CREATED                 = CREATED_BY,
         UPDATED                 = UPDATED_BY,
         HTS_PROVIDER_TYPE       = PROVIDER_TYPE,
         HTS_PROVIDER_TYPE_OTHER = PROVIDER_TYPE_OTHER,
      ) %>%
      left_join(trace_validation, join_by(CONFIRM_CODE)) %>%
      left_join(
         y  = harp$dx %>%
            select(
               idnum,
               CENTRAL_ID,
               HARP_INCLUSION_DATE,
               HARP_CONFIRM_DATE = confirm_date,
               HARP_CONFIRM_CODE = labcode2,
            ),
         by = join_by(CENTRAL_ID)
      ) %>%
      left_join(harp$tx %>% select(CENTRAL_ID, ART_START_DATE = artstart_date), join_by(CENTRAL_ID)) %>%
      left_join(harp$prep %>% select(CENTRAL_ID, PREP_START_DATE = prepstart_date), join_by(CENTRAL_ID)) %>%
      relocate(HARP_INCLUSION_DATE, .after = DATE_CONFIRM) %>%
      relocate(RT_AGREED_ACTUAL, RT_VALIDATION_REMARKS, .before = RT_AGREED) %>%
      mutate(
         RT_AGREED       = case_when(
            RT_AGREED_ACTUAL == "Y" ~ "1_Yes",
            RT_AGREED_ACTUAL == "N" ~ "0_No",
            TRUE ~ RT_AGREED
         ),
         RT_INCLUDED     = case_when(
            CONFIRM_CODE != HARP_CONFIRM_CODE & year(HARP_CONFIRM_DATE) < year(DATE_CONFIRM) ~ 0,
            interval(ART_START_DATE, hts_date) / days(1) > 28 ~ 0,
            AGE < 15 ~ 0,
            !is.na(RT_AGREED) ~ 1,
            !is.na(RT_RESULT) ~ 1,
            AGE >= 15 ~ 1,
            TRUE ~ 1
         ),
         TAT_TEST_ENROLL = interval(RT_OFFER_DATE, ART_START_DATE) / days(1),
         TAT_TEST_ENROLL = case_when(
            TAT_TEST_ENROLL < 0 ~ "0) Tx before dx",
            TAT_TEST_ENROLL == 0 ~ "1) Same day",
            TAT_TEST_ENROLL >= 1 & TAT_TEST_ENROLL <= 14 ~ "2) RAI (w/in 14 days)",
            TAT_TEST_ENROLL >= 15 & TAT_TEST_ENROLL <= 30 ~ "3) W/in 30days",
            TAT_TEST_ENROLL >= 31 ~ "4) More than 30 days",
            is.na(HARP_CONFIRM_DATE) & !is.na(idnum) ~ "5) More than 30 days",
            is.na(ART_START_DATE) ~ "(not yet enrolled)",
            TRUE ~ "(not yet confirmed)",
         )
      )

   return(data)
}

##  Sorting confirmatory results -----------------------------------------------

prioritize_reports <- function(data) {
   log_info("Add inclusion criteria.")
   data %<>%
      filter(RT_INCLUDED == 1) %>%
      arrange(RT_RESULT, RT_AGREED, CONFIRM_RESULT, RECORD_DATE, DATE_CONFIRM) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE)

   return(data)
}

##  Adding CD4 results ---------------------------------------------------------

get_cd4 <- function(data, forms) {
   log_info("Attaching baseline cd4.")
   lab_cd4 <- forms$lab_cd4 %>%
      mutate(
         CD4_RESULT = str_replace_all(CD4_RESULT, "[:alpha:]", ""),
         CD4_RESULT = str_replace_all(CD4_RESULT, " ", ""),
         CD4_RESULT = str_replace_all(CD4_RESULT, "<", ""),
         CD4_RESULT = suppress_warnings(as.numeric(CD4_RESULT), "NAs introduced")
      ) %>%
      get_cid(forms$id_reg, PATIENT_ID)

   data %<>%
      # get cd4 data
      # TODO: attach max dates for filtering of cd4 data
      left_join(
         y  = lab_cd4 %>%
            select(
               CD4_DATE,
               CD4_RESULT,
               CENTRAL_ID
            ),
         by = join_by(CENTRAL_ID)
      ) %>%
      mutate(
         # calculate distance from confirmatory date
         CD4_DATE     = as.Date(CD4_DATE),
         CD4_CONFIRM  = interval(CD4_DATE, RT_OFFER_DATE) / days(1),

         # baseline is within 182 days
         BASELINE_CD4 = if_else(
            CD4_CONFIRM >= -182 & CD4_CONFIRM <= 182,
            1,
            0
         ),

         # make values absolute to take date nearest to confirmatory
         CD4_CONFIRM  = abs(CD4_CONFIRM),
      ) %>%
      arrange(REC_ID, CD4_CONFIRM) %>%
      distinct(REC_ID, .keep_all = TRUE) %>%
      arrange(desc(CONFIRM_TYPE), CONFIRM_CODE)

   return(data)
}

##  Generate subset variables --------------------------------------------------

standardize_data <- function(initial) {
   log_info("Converting to final HARP variables.")
   data <- initial %>%
      mutate(
         # tagging vars
         male                   = if_else(
            condition = StrLeft(SEX, 1) == "1",
            true      = 1,
            false     = 0
         ),
         female                 = if_else(
            condition = StrLeft(SEX, 1) == "2",
            true      = 1,
            false     = 0
         ),

         # demographics
         SEX                    = remove_code(stri_trans_toupper(SEX)),
         SELF_IDENT             = remove_code(stri_trans_toupper(SELF_IDENT)),
         SELF_IDENT             = case_when(
            SELF_IDENT == "OTHER" ~ "OTHERS",
            SELF_IDENT == "MAN" ~ "MALE",
            SELF_IDENT == "WOMAN" ~ "FEMALE",
            SELF_IDENT == "MALE" ~ "MALE",
            SELF_IDENT == "FEMALE" ~ "FEMALE",
            TRUE ~ SELF_IDENT
         ),
         SELF_IDENT_OTHER       = stri_trans_toupper(SELF_IDENT_OTHER),
         SELF_IDENT_OTHER_SIEVE = str_replace_all(SELF_IDENT_OTHER, "[^[:alnum:]]", ""),

         CIVIL_STATUS           = stri_trans_toupper(CIVIL_STATUS),

         # clinical pic
         WHO_CLASS              = as.integer(keep_code(WHO_CLASS)),

         CLINICAL_PIC           = case_when(
            StrLeft(CLINICAL_PIC, 1) == "1" ~ "0_Asymptomatic",
            StrLeft(CLINICAL_PIC, 1) == "2" ~ "1_Symptomatic",
         ),

         OFW_STATION            = case_when(
            StrLeft(OFW_STATION, 1) == "1" ~ "1_On ship",
            StrLeft(OFW_STATION, 1) == "2" ~ "2_Land",
         ),

         REFER_TYPE             = case_when(
            StrLeft(REFER_TYPE, 1) == "1" ~ "1",
            StrLeft(REFER_TYPE, 1) == "2" ~ "1",
         )
      ) %>%
      # exposure history
      mutate_at(
         .vars = vars(starts_with("EXPOSE_") & !contains("DATE")),
         ~if_else(
            condition = !is.na(.),
            true      = StrLeft(., 1),
            false     = NA_character_,
         ) %>% as.integer()
      ) %>%
      # medical history
      mutate_at(
         .vars = vars(starts_with("MED_")),
         ~if_else(
            condition = !is.na(.),
            true      = StrLeft(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # test reason
      mutate_at(
         .vars = vars(starts_with("TEST_REASON") & !matches("_OTHER")),
         ~if_else(
            condition = !is.na(.),
            true      = StrLeft(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # mode of reach (HTS)
      mutate_at(
         .vars = vars(starts_with("REACH_")),
         ~if_else(
            condition = !is.na(.),
            true      = StrLeft(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # mode of reach (HTS)
      mutate_at(
         .vars = vars(starts_with("REFER")),
         ~if_else(
            condition = !is.na(.),
            true      = StrLeft(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # services provided (HTS)
      mutate_at(
         .vars = vars(starts_with("SERVICE_") & !ends_with("FACI")),
         ~if_else(
            condition = !is.na(.),
            true      = StrLeft(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      generate_gender_identity(SEX, SELF_IDENT, SELF_IDENT_OTHER, gender_identity) %>%
      process_vl("RT_VL_RESULT", "RT_VL_RESULT_CLEAN") %>%
      relocate(RT_VL_RESULT_CLEAN, .after = RT_VL_RESULT) %>%
      mutate_at(
         .vars = vars(SPECIMEN_SOURCE, SERVICE_FACI, CONFIRM_FACI, SPECIMEN_SUB_SOURCE, SERVICE_SUB_FACI, CONFIRM_SUB_FACI),
         ~na_if(as.character(.), "0")
      ) %>%
      mutate(
         RITA_RESULT = case_when(
            RT_VL_RESULT_CLEAN >= 1000 ~ "1_Recent",
            RT_VL_RESULT_CLEAN < 1000 ~ "2_Long-term",
         ),
         RT_FACI     = coalesce(SPECIMEN_SOURCE, SERVICE_FACI, CONFIRM_FACI),
         RT_SUB_FACI = coalesce(SPECIMEN_SUB_SOURCE, SERVICE_SUB_FACI, CONFIRM_SUB_FACI),
         .after      = RT_VL_RESULT_CLEAN,
      )

   return(data)
}

##  Modes of transmission ------------------------------------------------------

tag_mot <- function(data, params) {
   log_info("Generating mode of transmission.")
   data %<>%
      # mode of transmission
      mutate(
         # for mot
         motherisi1 = case_when(
            EXPOSE_HIV_MOTHER > 0 ~ 1,
            TRUE ~ 0
         ),
         sexwithf   = case_when(
            EXPOSE_SEX_F > 0 ~ 1,                      # HTS Form
            !is.na(EXPOSE_SEX_F_AV_DATE) ~ 1,          # HTS Form
            !is.na(EXPOSE_SEX_F_AV_NOCONDOM_DATE) ~ 1, # HTS Form
            EXPOSE_SEX_F_NOCONDOM > 0 ~ 1,
            TRUE ~ 0
         ),
         sexwithm   = case_when(
            EXPOSE_SEX_M > 0 ~ 1,                      # HTS Form
            !is.na(EXPOSE_SEX_M_AV_DATE) ~ 1,          # HTS Form
            !is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ 1, # HTS Form
            EXPOSE_SEX_M_NOCONDOM > 0 ~ 1,
            TRUE ~ 0
         ),
         sexwithpro = case_when(
            EXPOSE_SEX_PAYING > 0 ~ 1,
            TRUE ~ 0
         ),
         regularlya = case_when(
            EXPOSE_SEX_PAYMENT > 0 ~ 1,
            TRUE ~ 0
         ),
         injectdrug = case_when(
            EXPOSE_DRUG_INJECT > 0 ~ 1,
            TRUE ~ 0
         ),
         chemsex    = case_when(
            EXPOSE_SEX_DRUGS > 0 ~ 1, # HTS Form
            TRUE ~ 0
         ),
         receivedbt = case_when(
            EXPOSE_BLOOD_TRANSFUSE > 0 ~ 1,
            TRUE ~ 0
         ),
         sti        = case_when(
            EXPOSE_STI > 0 ~ 1,
            TRUE ~ 0
         ),
         needlepri1 = case_when(
            EXPOSE_OCCUPATION > 0 ~ 1,
            TRUE ~ 0
         ),

         mot        = 0,
         # m->m only
         mot        = case_when(
            male == 1 & EXPOSE_SEX_M_NOCONDOM == 1 ~ 1,
            male == 1 & YR_LAST_M >= params$p10y ~ 1,
            male == 1 & year(EXPOSE_SEX_M_AV_DATE) >= params$p10y ~ 1,          # HTS Form
            male == 1 & year(EXPOSE_SEX_M_AV_NOCONDOM_DATE) >= params$p10y ~ 1, # HTS Form
            TRUE ~ mot
         ),

         # m->m+f
         mot        = case_when(
            mot == 1 & EXPOSE_SEX_F_NOCONDOM == 1 ~ 2,
            mot == 1 & YR_LAST_F >= params$p10y ~ 2,
            mot == 1 & year(EXPOSE_SEX_F_AV_DATE) >= params$p10y ~ 2,          # HTS Form
            mot == 1 & year(EXPOSE_SEX_F_AV_NOCONDOM_DATE) >= params$p10y ~ 2, # HTS Form
            TRUE ~ mot
         ),

         # m->f only
         mot        = case_when(
            male == 1 & mot == 0 & EXPOSE_SEX_F_NOCONDOM == 1 ~ 3,
            male == 1 &
               mot == 0 &
               YR_LAST_F >= params$p10y ~ 3,
            male == 1 &
               mot == 0 &
               year(EXPOSE_SEX_F_AV_DATE) >= params$p10y ~ 3,          # HTS Form
            male == 1 &
               mot == 0 &
               year(EXPOSE_SEX_F_AV_NOCONDOM_DATE) >= params$p10y ~ 3, # HTS Form
            TRUE ~ mot
         ),

         # f->m
         mot        = case_when(
            female == 1 & EXPOSE_SEX_M_NOCONDOM == 1 ~ 4,
            female == 1 & YR_LAST_M >= params$p10y ~ 4,
            female == 1 & year(EXPOSE_SEX_M_AV_DATE) >= params$p10y ~ 4,          # HTS Form
            female == 1 & year(EXPOSE_SEX_M_AV_NOCONDOM_DATE) >= params$p10y ~ 4, # HTS Form
            TRUE ~ mot
         ),

         # ivdu
         mot        = case_when(
            EXPOSE_DRUG_INJECT > 0 & StrLeft(PERM_PSGC_PROV, 4) == "0722" ~ 5,
            TRUE ~ mot
         ),

         # vertical
         mot        = case_when(
            mot == 0 & motherisi1 == 1 ~ 6,
            TRUE ~ mot
         ),

         # m->m-f hx
         mot        = case_when(
            male == 1 &
               mot == 0 &
               NUM_M_PARTNER > 0 &
               is.na(YR_LAST_M) ~ 11,
            male == 1 &
               mot == 0 &
               YR_LAST_M >= params$p10y ~ 11,
            male == 1 &
               mot == 0 &
               !is.na(EXPOSE_SEX_M_AV_DATE) ~ 11,                     # HTS Form
            male == 1 &
               mot == 0 &
               !is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ 11,            # HTS Form
            male == 1 & mot == 0 & EXPOSE_SEX_M > 0 ~ 11,             # HTS Form
            TRUE ~ mot
         ),

         # m->m+f hx
         mot        = case_when(
            mot == 1 & NUM_F_PARTNER > 0 & is.na(YR_LAST_F) ~ 21,
            mot == 3 & NUM_M_PARTNER > 0 & is.na(YR_LAST_M) ~ 21,
            mot == 11 & NUM_F_PARTNER > 0 & is.na(YR_LAST_F) ~ 21,
            mot == 11 & YR_LAST_F >= params$p10y ~ 21,
            mot == 11 & !is.na(EXPOSE_SEX_F_AV_DATE) ~ 21,          # HTS Form,
            mot == 11 & !is.na(EXPOSE_SEX_F_AV_NOCONDOM_DATE) ~ 21, # HTS Form,
            mot == 11 & EXPOSE_SEX_F > 0 ~ 21,                      # HTS Form,
            TRUE ~ mot
         ),

         # m->f hx
         mot        = case_when(
            male == 1 &
               mot == 0 &
               NUM_F_PARTNER > 0 &
               is.na(YR_LAST_F) ~ 31,
            male == 1 &
               mot == 0 &
               YR_LAST_F >= params$p10y ~ 31,
            male == 1 &
               mot == 0 &
               !is.na(EXPOSE_SEX_F_AV_DATE) ~ 31,                     # HTS Form,
            male == 1 &
               mot == 0 &
               !is.na(EXPOSE_SEX_F_AV_NOCONDOM_DATE) ~ 31,            # HTS Form,
            male == 1 & mot == 0 & EXPOSE_SEX_F > 0 ~ 31,             # HTS Form,
            TRUE ~ mot
         ),

         # f->m hx
         mot        = case_when(
            female == 1 &
               mot == 0 &
               NUM_M_PARTNER > 0 &
               is.na(YR_LAST_M) ~ 41,
            female == 1 &
               mot == 0 &
               YR_LAST_M >= params$p10y ~ 41,
            female == 1 &
               mot == 0 &
               !is.na(EXPOSE_SEX_M_AV_DATE) ~ 41,              # HTS Form,
            female == 1 &
               mot == 0 &
               !is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE) ~ 41,     # HTS Form,
            female == 1 & mot == 0 & EXPOSE_SEX_M > 0 ~ 41,    # HTS Form,
            TRUE ~ mot
         ),

         # ivdu hx
         mot        = case_when(
            injectdrug > 0 & StrLeft(PERM_PSGC_PROV, 4) == "0722" ~ 51,
            TRUE ~ mot
         ),

         # mtct
         mot        = case_when(
            mot == 0 & AGE < 5 ~ 61,
            TRUE ~ mot
         ),

         # all else fails
         mot        = case_when(
            male == 1 & mot == 0 & NUM_M_PARTNER > 0 ~ 1,
            TRUE ~ mot
         ),
         mot        = case_when(
            mot == 1 & NUM_F_PARTNER > 0 ~ 2,
            TRUE ~ mot
         ),

         # needlestick
         mot        = case_when(
            mot == 0 & needlepri1 == 1 ~ 7,
            TRUE ~ mot
         ),

         # transfusion
         mot        = case_when(
            mot == 0 & receivedbt == 1 ~ 8,
            TRUE ~ mot
         ),

         # no data
         mot        = case_when(
            mot == 0 ~ 9,
            TRUE ~ mot
         ),

         # f->f
         mot        = case_when(
            female == 1 & mot == 0 & NUM_F_PARTNER > 0 ~ 10,
            female == 1 & mot == 0 & !is.na(YR_LAST_F) > 0 ~ 10,
            female == 1 &
               mot == 0 &
               !is.na(EXPOSE_SEX_F_AV_DATE) ~ 10,
            female == 1 &
               mot == 0 &
               !is.na(EXPOSE_SEX_F_AV_NOCONDOM_DATE) ~ 10,
            female == 1 & mot == 0 & EXPOSE_SEX_F > 0 ~ 10,
            TRUE ~ mot
         ),

         # # clean mot_09
         # mot                  = case_when(
         #    mot %in% c(7, 8) ~ 9,
         #    TRUE ~ mot
         # ),

         # transmit
         transmit   = case_when(
            mot %in% c(1, 2, 3, 4, 11, 21, 31, 41) ~ "SEX",
            mot %in% c(5, 51) ~ "IVDU",
            mot %in% c(6, 61) ~ "PERINATAL",
            mot %in% c(8, 9, 10) ~ "UNKNOWN",
            mot == 7 ~ "OTHERS",
         ),

         # sexhow
         sexhow     = case_when(
            mot %in% c(2, 21) ~ "BISEXUAL",
            mot %in% c(3, 4, 31, 41) ~ "HETEROSEXUAL",
            mot %in% c(1, 11) ~ "HOMOSEXUAL",
         ),
      )

   return(data)
}

##  Advanced HIV disease -------------------------------------------------------

tag_class <- function(data) {
   local_gs4_quiet()
   log_info("Tagging AHD.")
   class_corr <- read_sheet("1gNLaYTULQij4lzPyiadm_tofgtXZxcwJ5BDVZSyH7Rc")

   data %<>%
      mutate(
         # WHO Case Definition of advanced HIV classification
         # refined ahd
         BASELINE_CD4         = case_when(
            CD4_RESULT >= 500 ~ 1,
            CD4_RESULT >= 350 & CD4_RESULT < 500 ~ 2,
            CD4_RESULT >= 200 & CD4_RESULT < 350 ~ 3,
            CD4_RESULT >= 50 & CD4_RESULT < 200 ~ 4,
            CD4_RESULT < 50 ~ 5,
         ),
         ahd                  = case_when(
            WHO_CLASS %in% c(3, 4) ~ 1,
            AGE >= 5 & BASELINE_CD4 %in% c(4, 5) ~ 1,
            AGE < 5 ~ 1,
            !is.na(BASELINE_CD4) ~ 0
         ),
         BASELINE_CD4         = labelled(
            BASELINE_CD4,
            c(
               "1_500+ cells/μL"     = 1,
               "2_350-499 cells/μL"  = 2,
               "3_200-349 cells/μL"  = 3,
               "4_50-199 cells/μL"   = 4,
               "5_below 50 cells/μL" = 5
            )
         ),

         # tb patient
         # class
         classd               = if_else(
            condition = !is.na(WHO_CLASS),
            true      = WHO_CLASS,
            false     = NA_integer_
         ) %>% as.numeric(),
         description_symptoms = stri_trans_toupper(SYMPTOMS),
         MED_TB_PX            = case_when(
            stri_detect_fixed(description_symptoms, "TB") ~ 1,
            TRUE ~ as.numeric(MED_TB_PX)
         ),
         classd               = case_when(
            stri_detect_regex(description_symptoms, paste(collapse = "|", (class_corr %>% filter(as.numeric(class) == 3))$symptom)) ~ 3,
            MED_TB_PX == 1 ~ 3,
            TRUE ~ classd
         ),
         classd               = case_when(
            stri_detect_regex(description_symptoms, paste(collapse = "|", (class_corr %>% filter(as.numeric(class) == 4))$symptom)) ~ 4,
            TRUE ~ classd
         ),

         # new class for 2022
         HIV_STAGE            = case_when(
            classd %in% c(3, 4) ~ "AIDS",
            ahd == 1 ~ "AIDS",
            TRUE ~ "HIV"
         ),

         # no data for stAGE of hiv
         nodata_hiv_stage     = if_else(
            is.na(ahd) &
               is.na(BASELINE_CD4) &
               ((coalesce(description_symptoms, "") == "" & StrLeft(CLINICAL_PIC, 1) == "1") | is.na(CLINICAL_PIC)) &
               is.na(MED_TB_PX) &
               is.na(WHO_CLASS) &
               HIV_STAGE == "HIV",
            1,
            0,
            0
         ),
      )

   return(data)
}

##  Facilities & Address -------------------------------------------------------

convert_faci_addr <- function(data) {
   log_info("Converting address & facility data.")
   # rename columns
   data %<>%
      mutate(
         RT_FACI_ID = RT_FACI,
         .before    = RT_FACI
      ) %>%
      ohasis$get_faci(
         list(HTS_FACI = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
         "name"
      ) %>%
      ohasis$get_faci(
         list(SPECIMEN_SOURCE_FACI = c("SPECIMEN_SOURCE", "SPECIMEN_SUB_SOURCE")),
         "name"
      ) %>%
      ohasis$get_faci(
         list(CONFIRM_LAB = c("CONFIRM_FACI", "CONFIRM_SUB_FACI")),
         "name"
      ) %>%
      ohasis$get_faci(
         list(RT_LAB = c("RT_FACI", "RT_SUB_FACI")),
         "name",
         c("RT_REG", "RT_PROV", "RT_MUNC")
      ) %>%
      ohasis$get_addr(
         c(
            PERM_REG  = "PERM_PSGC_REG",
            PERM_PROV = "PERM_PSGC_PROV",
            PERM_MUNC = "PERM_PSGC_MUNC"
         ),
         "name"
      ) %>%
      ohasis$get_addr(
         c(
            CURR_REG  = "CURR_PSGC_REG",
            CURR_PROV = "CURR_PSGC_PROV",
            CURR_MUNC = "CURR_PSGC_MUNC"
         ),
         "name"
      ) %>%
      ohasis$get_addr(
         c(
            BIRTH_REG  = "BIRTH_PSGC_REG",
            BIRTH_PROV = "BIRTH_PSGC_PROV",
            BIRTH_MUNC = "BIRTH_PSGC_MUNC"
         ),
         "name"
      ) %>%
      ohasis$get_addr(
         c(
            CBS_REG  = "HIV_SERVICE_PSGC_REG",
            CBS_PROV = "HIV_SERVICE_PSGC_PROV",
            CBS_MUNC = "HIV_SERVICE_PSGC_MUNC"
         ),
         "name"
      ) %>%
      # country names
      left_join(
         y  = ohasis$ref_country %>%
            select(COUNTRY_CODE, OCW_COUNTRY = COUNTRY_NAME),
         by = join_by(OFW_COUNTRY == COUNTRY_CODE)
      ) %>%
      relocate(OCW_COUNTRY, .before = OFW_COUNTRY) %>%
      mutate_at(
         .vars = vars(SERVICE_BY, CREATED, UPDATED, SIGNATORY_1, SIGNATORY_2, SIGNATORY_3),
         ~as.character(.)
      ) %>%
      ohasis$get_staff(c(CREATED_BY = "CREATED")) %>%
      ohasis$get_staff(c(UPDATED_BY = "UPDATED")) %>%
      ohasis$get_staff(c(HTS_PROVIDER = "SERVICE_BY")) %>%
      ohasis$get_staff(c(ANALYZED_BY = "SIGNATORY_1")) %>%
      ohasis$get_staff(c(REVIEWED_BY = "SIGNATORY_2")) %>%
      ohasis$get_staff(c(NOTED_BY = "SIGNATORY_3"))

   return(data)
}

##  Finalize -------------------------------------------------------------------

final_conversion <- function(data) {
   data %<>%
      select(
         -any_of(
            c(
               "PRIME",
               "RECORD_DATE",
               "DISEASE",
               "HIV_SERVICE_TYPE",
               "GENDER_AFFIRM_THERAPY",
               "HIV_SERVICE_ADDR",
               "src",
               "MODULE",
               "MODALITY",
               "FACI_ID",
               "SUB_FACI_ID",
               "CONFIRMATORY_CODE",
               "DELETED_BY",
               "DELETED_AT",
               "SCREEN_AGREED",
               "EXPOSE_SEX_M_NOCONDOM",
               "EXPOSE_SEX_F_NOCONDOM",
               "EXPOSE_SEX_HIV",
               "AGE_FIRST_SEX",
               "NUM_F_PARTNER",
               "YR_LAST_F",
               "NUM_M_PARTNER",
               "YR_LAST_M",
               "AGE_FIRST_INJECT",
               "MED_CBS_REACTIVE",
               "MED_IS_PREGNANT",
               "FORMA_MSM",
               "FORMA_TGW",
               "FORMA_PWID",
               "FORMA_FSW",
               "FORMA_GENPOP",
               "SCREEN_REFER",
               "PARTNER_REFERRAL_FACI",
               "EXPOSE_SEX_EVER",
               "EXPOSE_CONDOMLESS_ANAL",
               "EXPOSE_CONDOMLESS_VAGINAL",
               "EXPOSE_M_SEX_ORAL_ANAL",
               "EXPOSE_NEEDLE_SHARE",
               "EXPOSE_ILLICIT_DRUGS",
               "EXPOSE_SEX_HIV_DATE",
               "EXPOSE_CONDOMLESS_ANAL_DATE",
               "EXPOSE_CONDOMLESS_VAGINAL_DATE",
               "EXPOSE_NEEDLE_SHARE_DATE",
               "EXPOSE_ILLICIT_DRUGS_DATE",
               "SERVICE_GIVEN_CONDOMS",
               "SERVICE_GIVEN_LUBES",
               "TEST_REFUSE_NO_TIME",
               "TEST_REFUSE_OTHER",
               "TEST_REFUSE_NO_CURE",
               "TEST_REFUSE_FEAR_RESULT",
               "TEST_REFUSE_FEAR_DISCLOSE",
               "TEST_REFUSE_FEAR_MSM",
               "CFBS_MSM",
               "CFBS_TGW",
               "CFBS_PWID",
               "CFBS_FSW",
               "CFBS_GENPOP"
            )
         )
      ) %>%
      distinct_all() %>%
      mutate(
         kap_unknown = if_else(coalesce(risks, "(no data)") == "(no data)", "(no data)", NA_character_),
         kap_msm     = if_else(SEX == "MALE" & SEXUAL_RISK %in% c("M", "M+F"), "MSM", NA_character_),
         kap_heterom = if_else(SEX == "MALE" & SEXUAL_RISK == "F", "Hetero Male", NA_character_),
         kap_heterof = if_else(SEX == "FEMALE" & !is.na(SEXUAL_RISK), "Hetero Female", NA_character_),
         kap_pip     = if_else(str_detect(risk_paymentforsex, "yes"), "PIP", NA_character_),
         kap_pdl     = case_when(
            StrLeft(CLIENT_TYPE, 1) == "7" ~ "PDL",
            str_detect(RT_LAB, "Jail") ~ "PDL",
            TRUE ~ NA_character_
         ),
      ) %>%
      unite(
         col   = "KAP_TYPE",
         sep   = "-",
         starts_with("kap_", ignore.case = FALSE),
         na.rm = TRUE
      ) %>%
      mutate(
         KAP_TYPE = if_else(KAP_TYPE == "", "No apparent risk", KAP_TYPE, KAP_TYPE)
      )

   return(data)
}

##  Flag data for validation ---------------------------------------------------

get_checks <- function(data, run_checks = NULL) {
   check      <- list()
   run_checks <- ifelse(
      !is.null(run_checks),
      run_checks,
      input(
         prompt  = "Run `hts_tst_pos` validations?",
         options = c("1" = "yes", "2" = "no"),
         default = "1"
      )
   )

   if (run_checks == "1") {
      data %<>%
         mutate(
            reg_order = RT_REG,
            reg_order = case_when(
               reg_order == "1" ~ 1,
               reg_order == "2" ~ 2,
               reg_order == "CAR" ~ 3,
               reg_order == "3" ~ 4,
               reg_order == "NCR" ~ 5,
               reg_order == "4A" ~ 6,
               reg_order == "4B" ~ 7,
               reg_order == "5" ~ 8,
               reg_order == "6" ~ 9,
               reg_order == "7" ~ 10,
               reg_order == "8" ~ 11,
               reg_order == "9" ~ 12,
               reg_order == "10" ~ 13,
               reg_order == "11" ~ 14,
               reg_order == "12" ~ 15,
               reg_order == "CARAGA" ~ 16,
               reg_order == "ARMM" ~ 17,
               reg_order == "BARMM" ~ 17,
               TRUE ~ 9999
            ),
         ) %>%
         arrange(reg_order, RT_REG, RT_LAB, CONFIRM_CODE) %>%
         select(-reg_order)

      view_vars <- c(
         "REC_ID",
         "PATIENT_ID",
         "RT_REG",
         "RT_LAB",
         "RT_ACTIVATION_DATE",
         "RT_OFFER_DATE",
         "RT_INCLUDED",
         "RT_AGREED",
         "RT_DATE",
         "RT_RESULT",
         "RT_VALIDATION_REMARKS",
         "RT_VALIDATION_STATUS",
         "RT_VL_REQUESTED",
         "RT_VL_DATE",
         "RT_VL_RESULT",
         "RITA_RESULT",
         "HARP_INCLUSION_DATE",
         "FORM_VERSION",
         "CONFIRM_CODE",
         "UIC",
         "PATIENT_CODE",
         "FIRST",
         "MIDDLE",
         "LAST",
         "SUFFIX",
         "BIRTHDATE",
         "SEX",
         "SELF_IDENT",
         "SELF_IDENT_OTHER",
         "gender_identity",
         "SPECIMEN_REFER_TYPE",
         "HTS_FACI",
         "SOURCE_FACI",
         "hts_date",
         "hts_modality",
         "mot",
         "transmit",
         "sexhow"
      )
      check     <- check_pii(data, check, view_vars)

      # dates
      date_vars <- c(
         "RT_OFFER_DATE",
         "DATE_COLLECT",
         "DATE_RECEIVE",
         "hts_date"
      )
      check     <- check_dates(data, check, view_vars, date_vars)

      # non-negotiable variables
      nonnegotiables <- c(
         "UIC",
         "AGE",
         "CONFIRM_LAB",
         "FORM_VERSION",
         "SPECIMEN_REFER_TYPE",
         "SELF_IDENT",
         "NATIONALITY",
         "RT_AGREED",
         "RT_OFFER_DATE",
         "transmit"
      )
      check          <- check_unknown(data, check, "perm_addr", view_vars, PERM_REG, PERM_PROV, PERM_MUNC)
      check          <- check_unknown(data, check, "curr_addr", view_vars, CURR_REG, CURR_PROV, CURR_MUNC)
      check          <- check_nonnegotiables(data, check, view_vars, nonnegotiables)
      check          <- check_preggy(data, check, view_vars, sex = SEX)
      check          <- check_age(data, check, view_vars, birthdate = BIRTHDATE, AGE = AGE, visit_date = RT_OFFER_DATE)

      # special checks
      log_info("Checking for not RT Result.")
      check[["no_rt_result"]] <- data %>%
         filter(
            RT_AGREED == "1_Yes",
            is.na(RT_RESULT),
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for no RITA Result.")
      check[["no_rita_result"]] <- data %>%
         filter(
            RT_RESULT == "1_Recent",
            is.na(RITA_RESULT)
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for no RITA Result.")
      check[["ahd_recent"]] <- data %>%
         filter(
            RT_RESULT == "1_Recent",
            HIV_STAGE == "AIDS"
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for missing gender identity.")
      check[["no_gender_ident"]] <- data %>%
         filter(
            is.na(gender_identity)
         ) %>%
         select(
            any_of(view_vars),
         )

      # range-median
      tabstat <- c(
         "RT_OFFER_DATE",
         "DATE_COLLECT",
         "DATE_RECEIVE",
         "RT_OFFER_DATE",
         "BIRTHDATE",
         "AGE"
      )
      check   <- check_tabstat(data, check, tabstat)
   }

   return(check)
}

##  Output Stata Datasets ------------------------------------------------------

output_dta <- function(official, params, save = "2") {
   if (save == "1") {
      log_info("Checking output directory.")
      version <- format(Sys.time(), "%Y%m%d")
      dir     <- Sys.getenv("HARP_DX")
      check_dir(dir)

      log_info("Saving in Stata data format.")
      period_ext <- str_c(params$yr, "-", stri_pad_left(params$mo, 2, "0"), ".dta")
      files      <- list(
         recency = file.path(dir, str_c(version, "_recency_", period_ext)),
      )
      for (output in intersect(names(files), names(official))) {
         if (nrow(official[[output]]) > 0) {
            official[[output]] %>%
               format_stata() %>%
               write_dta(files[[output]])

            compress_stata(files[[output]])
         }
      }
   }
}

##  Actual flow ----------------------------------------------------------------

.init <- function(envir = parent.env(environment()), ...) {
   step <- parent.env(environment())
   p    <- envir
   vars <- as.list(list(...))

   data       <- clean_data(p$forms, p$harp)
   data       <- get_cd4(data, p$forms)
   data       <- standardize_data(data)
   data       <- tag_mot(data, p$params)
   data       <- tag_class(data)
   data       <- convert_faci_addr(data)
   final_data <- prioritize_reports(data)
   final_data <- final_conversion(final_data)
   final_data %<>%
      left_join(
         y  = p$params$sites %>%
            mutate(rt_activation_date = as.Date(rt_activation_date)) %>%
            select(
               RT_FACI_ID         = FACI_ID,
               RT_ACTIVATION_DATE = rt_activation_date
            ),
         by = join_by(RT_FACI_ID)
      )

   step$check <- get_checks(final_data, run_checks = vars$run_checks)
   step$data  <- data

   p$official$recency <- final_data
   # output_dta(p$official, p$params, vars$save)

   flow_validation(p, "hts_recent", p$params$ym, upload = vars$upload)
   log_success("Done.")
}