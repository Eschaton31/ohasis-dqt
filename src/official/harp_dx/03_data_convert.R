##  Generate subset variables --------------------------------------------------

standardize_data <- function(data) {
   data %<>%
      mutate(
         # generate idnum
         idnum                     = if_else(
            condition = is.na(IDNUM),
            true      = .GlobalEnv$nhsss$harp_dx$params$latest_idnum + row_number(),
            false     = as.integer(IDNUM)
         ),

         # Perm Region (as encoded)
         PERMONLY_PSGC_REG         = if_else(
            condition = use_curr == 0,
            true      = PERM_PSGC_REG,
            false     = NA_character_
         ),
         PERMONLY_PSGC_PROV        = if_else(
            condition = use_curr == 0,
            true      = PERM_PSGC_PROV,
            false     = NA_character_
         ),
         PERMONLY_PSGC_MUNC        = if_else(
            condition = use_curr == 0,
            true      = PERM_PSGC_MUNC,
            false     = NA_character_
         ),

         # tagging vars
         male                      = if_else(
            condition = StrLeft(SEX, 1) == "1",
            true      = 1,
            false     = 0
         ),
         female                    = if_else(
            condition = StrLeft(SEX, 1) == "2",
            true      = 1,
            false     = 0
         ),

         # report date
         year                      = ohasis$yr %>% as.integer(),
         month                     = ohasis$mo %>% as.integer(),

         # confirmatory info
         test_done                 = case_when(
            T3_KIT == "Geenius HIV 1/2 Confirmatory Assay" ~ "GEENIUS",
            T3_KIT == "HIV 1/2 STAT-PAK Assay" ~ "STAT-PAK",
            T3_KIT == "MP Diagnostics HIV BLOT 2.2" ~ "WESTERN BLOT",
            AGE <= 1 ~ "PCR"
         ),
         rhivda_done               = if_else(
            condition = StrLeft(CONFIRM_TYPE, 1) == "2",
            true      = 1,
            false     = as.numeric(NA)
         ),
         sample_source             = if_else(
            condition = !is.na(SPECIMEN_REFER_TYPE),
            true      = substr(SPECIMEN_REFER_TYPE, 3, 3),
            false     = NA_character_
         ),

         # demographics
         pxcode                    = paste0(
            if_else(
               condition = !is.na(FIRST),
               true      = StrLeft(FIRST, 1),
               false     = ""
            ),
            if_else(
               condition = !is.na(MIDDLE),
               true      = StrLeft(MIDDLE, 1),
               false     = ""
            ),
            if_else(
               condition = !is.na(LAST),
               true      = StrLeft(LAST, 1),
               false     = ""
            )
         ),
         SEX                       = stri_trans_toupper(SEX),
         self_identity             = if_else(
            condition = !is.na(SELF_IDENT),
            true      = substr(stri_trans_toupper(SELF_IDENT), 3, stri_length(SELF_IDENT)),
            false     = NA_character_
         ),
         self_identity_other       = toupper(SELF_IDENT_OTHER),
         self_identity             = case_when(
            # self_identity_other == "N/A" ~ NA_character_,
            # self_identity_other == "no answer" ~ NA_character_,
            self_identity == "OTHER" ~ "OTHERS",
            self_identity == "MAN" ~ "MALE",
            self_identity == "WOMAN" ~ "FEMALE",
            self_identity == "MALE" ~ "MALE",
            self_identity == "FEMALE" ~ "FEMALE",
            TRUE ~ self_identity
         ),
         self_identity_other       = toupper(self_identity_other),
         self_identity_other_sieve = if_else(
            condition = !is.na(self_identity_other),
            true      = str_replace_all(self_identity_other, "[^[:alnum:]]", ""),
            false     = NA_character_
         ),

         CIVIL_STATUS              = stri_trans_toupper(CIVIL_STATUS),
         nationalit                = case_when(
            NATIONALITY == "Philippines" ~ "FILIPINO",
            NATIONALITY != "Philippines" ~ "NON-FILIPINO",
            TRUE ~ "UNKNOWN"
         ),
         current_school_level      = if_else(
            condition = StrLeft(IS_STUDENT, 1) == "1",
            true      = EDUC_LEVEL,
            false     = NA_character_
         ),

         # occupation
         curr_work                 = if_else(
            condition = StrLeft(IS_EMPLOYED, 1) == "1",
            true      = stri_trans_toupper(WORK),
            false     = NA_character_
         ),
         prev_work                 = if_else(
            condition = StrLeft(IS_EMPLOYED, 1) == "0" | is.na(IS_EMPLOYED),
            true      = stri_trans_toupper(WORK),
            false     = NA_character_
         ),

         # clinical pic
         who_staging               = StrLeft(WHO_CLASS, 1) %>% as.integer(),
         other_reason_test         = stri_trans_toupper(TEST_REASON_OTHER_TEXT),
         CLINICAL_PIC              = case_when(
            StrLeft(CLINICAL_PIC, 1) == "1" ~ "0_Asymptomatic",
            StrLeft(CLINICAL_PIC, 1) == "2" ~ "1_Symptomatic",
         ),
         OFW_STATION               = case_when(
            StrLeft(OFW_STATION, 1) == "1" ~ "1_On ship",
            StrLeft(OFW_STATION, 1) == "2" ~ "2_Land",
         ),
         REFER_TYPE                = case_when(
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
         .vars = vars(starts_with("SERVICE_")),
         ~if_else(
            condition = !is.na(.),
            true      = StrLeft(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # gender identity
      left_join(
         y  = nhsss$harp_dx$corr$gender_identity$Sheet1,
         by = c("SEX", "self_identity", "self_identity_other_sieve")
      )
   return(data)
}

##  Modes of transmission ------------------------------------------------------

tag_mot <- function(data) {
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
            male == 1 & YR_LAST_M >= nhsss$harp_dx$params$p10y ~ 1,
            male == 1 & year(EXPOSE_SEX_M_AV_DATE) >= nhsss$harp_dx$params$p10y ~ 1,          # HTS Form
            male == 1 & year(EXPOSE_SEX_M_AV_NOCONDOM_DATE) >= nhsss$harp_dx$params$p10y ~ 1, # HTS Form
            TRUE ~ mot
         ),

         # m->m+f
         mot        = case_when(
            mot == 1 & EXPOSE_SEX_F_NOCONDOM == 1 ~ 2,
            mot == 1 & YR_LAST_F >= nhsss$harp_dx$params$p10y ~ 2,
            mot == 1 & year(EXPOSE_SEX_F_AV_DATE) >= nhsss$harp_dx$params$p10y ~ 2,          # HTS Form
            mot == 1 & year(EXPOSE_SEX_F_AV_NOCONDOM_DATE) >= nhsss$harp_dx$params$p10y ~ 2, # HTS Form
            TRUE ~ mot
         ),

         # m->f only
         mot        = case_when(
            male == 1 & mot == 0 & EXPOSE_SEX_F_NOCONDOM == 1 ~ 3,
            male == 1 &
               mot == 0 &
               YR_LAST_F >= nhsss$harp_dx$params$p10y ~ 3,
            male == 1 &
               mot == 0 &
               year(EXPOSE_SEX_F_AV_DATE) >= nhsss$harp_dx$params$p10y ~ 3,          # HTS Form
            male == 1 &
               mot == 0 &
               year(EXPOSE_SEX_F_AV_NOCONDOM_DATE) >= nhsss$harp_dx$params$p10y ~ 3, # HTS Form
            TRUE ~ mot
         ),

         # f->m
         mot        = case_when(
            female == 1 & EXPOSE_SEX_M_NOCONDOM == 1 ~ 4,
            female == 1 & YR_LAST_M >= nhsss$harp_dx$params$p10y ~ 4,
            female == 1 & year(EXPOSE_SEX_M_AV_DATE) >= nhsss$harp_dx$params$p10y ~ 4,          # HTS Form
            female == 1 & year(EXPOSE_SEX_M_AV_NOCONDOM_DATE) >= nhsss$harp_dx$params$p10y ~ 4, # HTS Form
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
               YR_LAST_M >= nhsss$harp_dx$params$p10y ~ 11,
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
            mot == 11 & YR_LAST_F >= nhsss$harp_dx$params$p10y ~ 21,
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
               YR_LAST_F >= nhsss$harp_dx$params$p10y ~ 31,
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
               YR_LAST_M >= nhsss$harp_dx$params$p10y ~ 41,
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
   data %<>%
      mutate(
         # cd4 tagging
         CD4_RESULT           = stri_replace_all_charclass(CD4_RESULT, "[:alpha:]", ""),
         CD4_RESULT           = stri_replace_all_fixed(CD4_RESULT, " ", ""),
         CD4_RESULT           = stri_replace_all_fixed(CD4_RESULT, "<", ""),
         CD4_RESULT           = as.numeric(CD4_RESULT),
         baseline_cd4         = case_when(
            CD4_RESULT >= 500 ~ 1,
            CD4_RESULT >= 350 & CD4_RESULT < 500 ~ 2,
            CD4_RESULT >= 200 & CD4_RESULT < 350 ~ 3,
            CD4_RESULT >= 50 & CD4_RESULT < 200 ~ 4,
            CD4_RESULT < 50 ~ 5,
         ),
         CD4_DATE             = if_else(
            condition = is.na(CD4_RESULT),
            true      = NA_Date_,
            false     = CD4_DATE
         ),

         # WHO Case Definition of advanced HIV classification
         CD4_CONFIRM          = difftime(as.Date(confirm_date), CD4_DATE, units = "days") %>% as.numeric(),
         baseline_cd4         = if_else(
            condition = CD4_CONFIRM <= 182,
            true      = baseline_cd4,
            false     = as.numeric(NA)
         ),
         CD4_DATE             = if_else(
            condition = CD4_CONFIRM <= 182,
            true      = CD4_DATE,
            false     = as.Date(NA)
         ),
         CD4_RESULT           = if_else(
            condition = CD4_CONFIRM <= 182,
            true      = CD4_RESULT,
            false     = as.numeric(NA)
         ),
         ahd                  = case_when(
            who_staging %in% c(3, 4) ~ 1,
            AGE >= 5 &
               baseline_cd4 %in% c(3, 4, 5) ~ 1,
            AGE >= 1 &
               AGE < 5 &
               baseline_cd4 %in% c(2, 3, 4, 5) ~ 1,
            (AGE < 1 | AGE_MO < 12) &
               baseline_cd4 %in% c(1, 2, 3, 4, 5) ~ 1,
            !is.na(baseline_cd4) ~ 0
         ),
         # refined ahd
         ahd                  = case_when(
            who_staging %in% c(3, 4) ~ 1,
            AGE >= 5 & baseline_cd4 %in% c(4, 5) ~ 1,
            AGE < 5 ~ 1,
            !is.na(baseline_cd4) ~ 0
         ),

         # class
         classd               = if_else(
            condition = !is.na(who_staging),
            true      = who_staging,
            false     = NA_integer_
         ) %>% as.numeric(),
         description_symptoms = stri_trans_toupper(SYMPTOMS),
         classd               = case_when(
            stri_detect_regex(description_symptoms, paste(collapse = "|", (nhsss$harp_dx$corr$classd %>% filter(class == 3))$symptom)) ~ 3,
            MED_TB_PX == 1 ~ 3,
            TRUE ~ classd
         ),
         classd               = case_when(
            stri_detect_regex(description_symptoms, paste(collapse = "|", (nhsss$harp_dx$corr$classd %>% filter(class == 4))$symptom)) ~ 4,
            TRUE ~ classd
         ),

         # final class
         class                = case_when(
            classd %in% c(3, 4) ~ "AIDS",
            TRUE ~ "HIV"
         ),

         # new class for 2022
         class2022            = case_when(
            class == "AIDS" ~ "AIDS",
            ahd == 1 ~ "AIDS",
            TRUE ~ "HIV"
         ),

         # tb patient
         MED_TB_PX            = case_when(
            stri_detect_fixed(description_symptoms, "TB") ~ 1,
            TRUE ~ as.numeric(MED_TB_PX)
         ),

         # modality (HTS)
         MODALITY             = case_when(
            FORM_VERSION == "Form A (v2017)" ~ "FBT",
            StrLeft(MODALITY, 6) == "101101" ~ "FBT",
            StrLeft(MODALITY, 6) == "101103" ~ "CBS",
            StrLeft(MODALITY, 6) == "101104" ~ "FBS",
            StrLeft(MODALITY, 6) == "101105" ~ "ST",
            StrLeft(MODALITY, 6) == "101304" ~ "REACH",
         ),

         # form (HTS)
         FORM_VERSION         = if_else(FORM_VERSION == " (vNA)", NA_character_, FORM_VERSION),

         # provider type (HTS)
         PROVIDER_TYPE        = if_else(!is.na(PROVIDER_TYPE), substr(PROVIDER_TYPE, 1, stri_locate_first_fixed(PROVIDER_TYPE, "_") - 1), NA_character_) %>% as.integer(),

         # agreed to test (HTS)
         SCREEN_AGREED        = case_when(
            FORM_VERSION == "Form A (v2017)" ~ "1",
            !is.na(SCREEN_AGREED) ~ StrLeft(SCREEN_AGREED, 1),
         ) %>% as.integer(),

         # other services (HTS)
         given_ssnt           = case_when(
            SERVICE_SSNT_ACCEPT == 1 ~ "Accepted",
            SERVICE_SSNT_OFFER == 1 ~ "Offered",
         ),

         # combi prev (HTS)
         SERVICE_CONDOMS      = if_else(SERVICE_CONDOMS == 0, NA_integer_, as.integer(SERVICE_CONDOMS), NA_integer_),
         SERVICE_LUBES        = if_else(SERVICE_LUBES == 0, NA_integer_, as.integer(SERVICE_LUBES), NA_integer_),
      )
   return(data)
}

##  Address --------------------------------------------------------------------

convert_address <- function(data) {
   data %<>%
      mutate_at(
         .vars = vars(contains("_PSGC_")),
         ~if_else(is.na(.), "", .)
      ) %>%
      # permanent
      rename_at(
         .vars = vars(starts_with("PERM_PSGC")),
         ~stri_replace_all_fixed(., "PERM_", "")
      ) %>%
      left_join(
         y  = ohasis$ref_addr %>%
            select(
               PSGC_REG,
               PSGC_PROV,
               PSGC_MUNC,
               region   = NHSSS_REG,
               province = NHSSS_PROV,
               muncity  = NHSSS_MUNC,
            ),
         by = c("PSGC_REG", "PSGC_PROV", "PSGC_MUNC")
      ) %>%
      relocate(muncity, province, region, .before = PSGC_REG) %>%
      rename(
         psgc_region   = PSGC_REG,
         psgc_province = PSGC_PROV,
         psgc_muncity  = PSGC_MUNC
      ) %>%
      # current
      rename_at(
         .vars = vars(starts_with("CURR_PSGC")),
         ~stri_replace_all_fixed(., "CURR_", "")
      ) %>%
      left_join(
         y  = ohasis$ref_addr %>%
            select(
               PSGC_REG,
               PSGC_PROV,
               PSGC_MUNC,
               region_c   = NHSSS_REG,
               province_c = NHSSS_PROV,
               muncity_c  = NHSSS_MUNC,
            ),
         by = c("PSGC_REG", "PSGC_PROV", "PSGC_MUNC")
      ) %>%
      relocate(muncity_c, province_c, region_c, .before = PSGC_REG) %>%
      rename(
         psgc_region_c   = PSGC_REG,
         psgc_province_c = PSGC_PROV,
         psgc_muncity_c  = PSGC_MUNC
      ) %>%
      # birth
      rename_at(
         .vars = vars(starts_with("BIRTH_PSGC")),
         ~stri_replace_all_fixed(., "BIRTH_", "")
      ) %>%
      left_join(
         y  = ohasis$ref_addr %>%
            select(
               PSGC_REG,
               PSGC_PROV,
               PSGC_MUNC,
               region01   = NHSSS_REG,
               province01 = NHSSS_PROV,
               placefbir  = NHSSS_MUNC,
            ),
         by = c("PSGC_REG", "PSGC_PROV", "PSGC_MUNC")
      ) %>%
      relocate(placefbir, province01, region01, .before = PSGC_REG) %>%
      rename(
         psgc_region01   = PSGC_REG,
         psgc_province01 = PSGC_PROV,
         psgc_placeofbir = PSGC_MUNC
      ) %>%
      # perm region only
      rename_at(
         .vars = vars(starts_with("PERMONLY_PSGC")),
         ~stri_replace_all_fixed(., "PERMONLY_", "")
      ) %>%
      left_join(
         y  = ohasis$ref_addr %>%
            select(
               PSGC_REG,
               PSGC_PROV,
               PSGC_MUNC,
               region_p   = NHSSS_REG,
               province_p = NHSSS_PROV,
               muncity_p  = NHSSS_MUNC,
            ),
         by = c("PSGC_REG", "PSGC_PROV", "PSGC_MUNC")
      ) %>%
      relocate(muncity_p, province_p, region_p, .before = PSGC_REG) %>%
      rename(
         psgc_region_p   = PSGC_REG,
         psgc_province_p = PSGC_PROV,
         psgc_muncity_p  = PSGC_MUNC
      ) %>%
      # hiv_service location
      rename_at(
         .vars = vars(starts_with("HIV_SERVICE_PSGC")),
         ~stri_replace_all_fixed(., "HIV_SERVICE_", "")
      ) %>%
      left_join(
         y  = ohasis$ref_addr %>%
            select(
               PSGC_REG,
               PSGC_PROV,
               PSGC_MUNC,
               venue_region   = NHSSS_REG,
               venue_province = NHSSS_PROV,
               venue_muncity  = NHSSS_MUNC,
            ),
         by = c("PSGC_REG", "PSGC_PROV", "PSGC_MUNC")
      ) %>%
      relocate(venue_muncity, venue_province, venue_region, .before = PSGC_REG) %>%
      rename(
         psgc_venue_region   = PSGC_REG,
         psgc_venue_province = PSGC_PROV,
         psgc_venue_muncity  = PSGC_MUNC
      ) %>%
      # country names
      left_join(
         y  = ohasis$ref_country %>%
            select(COUNTRY_CODE, ocw_country = COUNTRY_NAME),
         by = c("OFW_COUNTRY" = "COUNTRY_CODE")
      ) %>%
      relocate(ocw_country, .before = OFW_COUNTRY)
   return(data)
}

##  Facilities -----------------------------------------------------------------

convert_faci_names <- function(data) {
   data %<>%
      # confirmlab
      mutate(
         CONFIRM_FACI     = if_else(
            condition = is.na(CONFIRM_FACI),
            true      = "",
            false     = CONFIRM_FACI
         ),
         CONFIRM_SUB_FACI = case_when(
            is.na(CONFIRM_SUB_FACI) ~ "",
            StrLeft(CONFIRM_SUB_FACI, 6) != CONFIRM_FACI ~ "",
            TRUE ~ CONFIRM_SUB_FACI
         )
      ) %>%
      left_join(
         na_matches = "never",
         y          = nhsss$harp_dx$corr$confirmlab %>%
            mutate(
               FACI_ID     = if_else(
                  condition = is.na(FACI_ID),
                  true      = "",
                  false     = FACI_ID
               ),
               SUB_FACI_ID = case_when(
                  is.na(SUB_FACI_ID) ~ "",
                  StrLeft(SUB_FACI_ID, 6) != FACI_ID ~ "",
                  TRUE ~ SUB_FACI_ID
               )
            ) %>%
            rename(
               CONFIRM_FACI     = FACI_ID,
               CONFIRM_SUB_FACI = SUB_FACI_ID,
            ),
         by         = c("CONFIRM_FACI", "CONFIRM_SUB_FACI")
      ) %>%
      relocate(confirmlab, .before = CONFIRM_FACI) %>%
      # dxlab_standard
      mutate(
         TEST_FACI     = if_else(
            condition = is.na(TEST_FACI),
            true      = "",
            false     = TEST_FACI
         ),
         TEST_SUB_FACI = case_when(
            is.na(TEST_SUB_FACI) ~ "",
            StrLeft(TEST_SUB_FACI, 6) != TEST_FACI ~ "",
            TRUE ~ TEST_SUB_FACI
         )
      ) %>%
      left_join(
         na_matches = "never",
         y          = ohasis$ref_faci %>%
            left_join(
               y  = ohasis$ref_addr %>%
                  select(
                     FACI_PSGC_REG  = PSGC_REG,
                     FACI_PSGC_PROV = PSGC_PROV,
                     FACI_PSGC_MUNC = PSGC_MUNC,
                     dx_region      = NHSSS_REG,
                     dx_province    = NHSSS_PROV,
                     dx_muncity     = NHSSS_MUNC
                  ),
               by = c("FACI_PSGC_REG", "FACI_PSGC_PROV", "FACI_PSGC_MUNC")
            ) %>%
            select(
               TEST_FACI      = FACI_ID,
               TEST_SUB_FACI  = SUB_FACI_ID,
               dxlab_standard = FACI_NAME_CLEAN,
               pubpriv        = PUBPRIV,
               dx_region,
               dx_province,
               dx_muncity
            ),
         by         = c("TEST_FACI", "TEST_SUB_FACI")
      ) %>%
      relocate(dxlab_standard, .before = CONFIRM_FACI)
   return(data)
}

##  Finalize -------------------------------------------------------------------

final_conversion <- function(data) {
   data %<>%
      # same vars as registry
      select(
         CENTRAL_ID,
         PATIENT_ID,
         REC_ID,
         idnum,
         form                      = FORM_VERSION,
         modality                  = MODALITY,              # HTS Form
         consent_test              = SCREEN_AGREED,         # HTS Form
         mot,
         starts_with("risk_", ignore.case = FALSE),
         labcode                   = CONFIRM_CODE,
         year,
         month,
         uic                       = UIC,
         firstname                 = FIRST,
         middle                    = MIDDLE,
         last                      = LAST,
         name_suffix               = SUFFIX,
         bdate                     = BIRTHDATE,
         pxcode,
         age                       = AGE,
         age_months                = AGE_MO,
         sex                       = SEX,
         philhealth                = PHILHEALTH_NO,
         muncity,
         province,
         region,
         psgc_muncity,
         psgc_province,
         psgc_region,
         muncity_c,
         province_c,
         region_c,
         psgc_muncity_c,
         psgc_province_c,
         psgc_region_c,
         muncity_p,
         province_p,
         region_p,
         ocw                       = IS_OFW,
         motherisi1,
         pregnant                  = IS_PREGNANT,
         tbpatient1                = MED_TB_PX,
         nationalit,
         civilstat                 = CIVIL_STATUS,
         self_identity,
         self_identity_other,
         gender_identity,
         nationality               = NATIONALITY,
         highest_educ              = EDUC_LEVEL,
         in_school                 = IS_STUDENT,
         current_school_level,
         with_partner              = LIVING_WITH_PARTNER,
         child_count               = CHILDREN,
         sexwithf,
         sexwithm,
         sexwithpro,
         regularlya,
         injectdrug,
         chemsex,
         receivedbt,
         sti,
         needlepri1,
         transmit,
         sexhow,
         class,
         class2022,
         ahd,
         baseline_cd4,
         baseline_cd4_date         = CD4_DATE,
         baseline_cd4_result       = CD4_RESULT,
         confirm_date,
         CONFIRM_FACI,
         confirmlab,
         region01,
         province01,
         placefbir,
         curr_work,
         prev_work,
         ocw_based                 = OFW_STATION,
         ocw_country,
         age_sex                   = AGE_FIRST_SEX,
         age_inj                   = AGE_FIRST_INJECT,
         howmanymse                = NUM_M_PARTNER,
         yrlastmsex                = YR_LAST_M,
         howmanyfse                = NUM_F_PARTNER,
         yrlastfsex                = YR_LAST_F,
         past12mo_injdrug          = EXPOSE_DRUG_INJECT,
         past12mo_rcvbt            = EXPOSE_BLOOD_TRANSFUSE,
         past12mo_sti              = EXPOSE_STI,
         past12mo_sexfnocondom     = EXPOSE_SEX_F_NOCONDOM,
         past12mo_sexmnocondom     = EXPOSE_SEX_M_NOCONDOM,
         past12mo_sexprosti        = EXPOSE_SEX_PAYING,
         past12mo_acceptpayforsex  = EXPOSE_SEX_PAYMENT,
         past12mo_needle           = EXPOSE_OCCUPATION,
         past12mo_hadtattoo        = EXPOSE_TATTOO,
         history_sex_m             = EXPOSE_SEX_M,
         date_lastsex_m            = EXPOSE_SEX_M_AV_DATE,
         date_lastsex_condomless_m = EXPOSE_SEX_M_AV_NOCONDOM_DATE,
         history_sex_f             = EXPOSE_SEX_F,
         date_lastsex_f            = EXPOSE_SEX_F_AV_DATE,
         date_lastsex_condomless_f = EXPOSE_SEX_F_AV_NOCONDOM_DATE,
         prevtest                  = PREV_TESTED,
         prev_test_result          = PREV_TEST_RESULT,
         prev_test_faci            = PREV_TEST_FACI,
         prevtest_date             = PREV_TEST_DATE,
         clinicalpicture           = CLINICAL_PIC,
         recombyph1                = TEST_REASON_PHYSICIAN,
         recomby_peer_ed           = TEST_REASON_PEER_ED,   # HTS Form
         insurance1                = TEST_REASON_INSURANCE,
         recheckpr1                = TEST_REASON_RETEST,
         no_test_reason            = TEST_REASON_NO_REASON,
         possible_exposure         = TEST_REASON_HIV_EXPOSE,
         emp_local                 = TEST_REASON_EMPLOY_LOCAL,
         emp_abroad                = TEST_REASON_EMPLOY_OFW,
         other_reason_test,
         description_symptoms,
         who_staging,
         hx_hepb                   = MED_HEP_B,
         hx_hepc                   = MED_HEP_C,
         hx_cbs                    = MED_CBS_REACTIVE,
         hx_prep                   = MED_PREP_PX,
         hx_pep                    = MED_PEP_PX,
         hx_sti                    = MED_STI,
         reach_clinical            = REACH_CLINICAL,
         reach_online              = REACH_ONLINE,
         reach_it                  = REACH_INDEX_TESTING,
         reach_ssnt                = REACH_SSNT,
         reach_venue               = REACH_VENUE,
         refer_art                 = REFER_ART,
         refer_confirm             = REFER_CONFIRM,
         retest                    = REFER_RETEST,
         retest_in_mos             = RETEST_MOS,
         retest_in_wks             = RETEST_WKS,
         retest_date               = RETEST_DATE,
         given_hiv101              = SERVICE_HIV_101,
         given_iec_mats            = SERVICE_IEC_MATS,
         given_risk_reduce         = SERVICE_RISK_COUNSEL,
         given_prep_pep            = SERVICE_PREP_REFER,
         given_ssnt,
         provider_type             = PROVIDER_TYPE,
         provider_type_other       = PROVIDER_TYPE_OTHER,
         venue_region,
         venue_province,
         venue_muncity,
         venue_text                = HIV_SERVICE_ADDR,
         px_type                   = CLIENT_TYPE,
         referred_by               = REFER_TYPE,
         t0_date                   = T0_DATE,
         t0_result                 = T0_RESULT,
         test_done,
         name,
         t1_result                 = T1_RESULT,
         t2_result                 = T2_RESULT,
         t3_result                 = T3_RESULT,
         final_interpretation      = CONFIRM_RESULT,
         visit_date,
         blood_extract_date,
         specimen_receipt_date,
         rhivda_done,
         sample_source,
         TEST_FACI,
         dxlab_standard,
         pubpriv,
         dx_region,
         dx_province,
         dx_muncity,
         CD4_CONFIRM
      ) %>%
      # turn into codes
      mutate_at(
         .vars = vars(
            ocw,
            highest_educ,
            current_school_level,
            in_school,
            pregnant,
            with_partner,
            ocw_based,
            prev_test_result,
            clinicalpicture,
            prevtest,
            px_type,
            referred_by
         ),
         ~if_else(
            condition = !is.na(.),
            true      = substr(., 1, stri_locate_first_fixed(., "_") - 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # remove codes
      mutate_at(
         .vars = vars(
            sex,
            civilstat,
            t1_result,
            t2_result,
            t3_result,
            final_interpretation
         ),
         ~if_else(
            condition = !is.na(.),
            true      = substr(., stri_locate_first_fixed(., "_") + 1, stri_length(.)),
            false     = NA_character_
         )
      ) %>%
      # remove codes
      mutate_at(
         .vars = vars(contains("date")),
         ~if_else(
            condition = !is.na(.),
            true      = as.Date(.),
            false     = NA_Date_
         )
      ) %>%
      mutate(
         age_pregnant = if_else(
            condition = pregnant == 1,
            true      = age,
            false     = as.numeric(NA)
         ),
         age_vertical = if_else(
            condition = transmit == 'PERINATAL',
            true      = age,
            false     = as.numeric(NA)
         ),
         age_unknown  = if_else(
            condition = transmit == 'UNKNOWN',
            true      = age,
            false     = as.numeric(NA)
         ),
         pubpriv      = if_else(pubpriv == "0", NA_character_, as.character(pubpriv))
      ) %>%
      distinct_all()
   return(data)
}

##  Flag data for validation ---------------------------------------------------

get_checks <- function(data) {
   check  <- list()
   update <- input(
      prompt  = "Run `converted` validations?",
      options = c("1" = "yes", "2" = "no"),
      default = "1"
   )
   update <- substr(toupper(update), 1, 1)

   if (update == "1") {
      # initialize checking layer
      view_vars <- c(
         "REC_ID",
         "PATIENT_ID",
         "form",
         "labcode",
         "uic",
         "firstname",
         "middle",
         "last",
         "name_suffix",
         "bdate",
         "sex",
         "gender_identity",
         "curr_work",
         "prev_work",
         "CONFIRM_FACI",
         "confirmlab",
         "TEST_FACI",
         "dxlab_standard",
         "mot",
         "transmit",
         "sexhow",
         get_names(data, "risk_")
      )

      # non-negotiable variables
      nonnegotiables <- c(
         "idnum",
         "age",
         "confirmlab",
         "dxlab_standard",
         "pubpriv"
      )
      check          <- check_nonnegotiables(data, check, view_vars, nonnegotiables)

      # unknown data
      vars <- c(
         "transmit",
         "region",
         "province",
         "muncity"
      )
      .log_info("Checking if required variables have UNKNOWN data or unpaired NHSSS versions.")
      for (var in vars) {
         var          <- as.symbol(var)
         check[[var]] <- data %>%
            filter(
               !!var %in% c("UNKNOWN", "OTHERS", NA_character_)
            ) %>%
            select(
               any_of(view_vars),
               !!var
            )
      }

      # special checks
      check[["perinatal"]] <- data %>%
         filter(
            transmit == "PERINATAL"
         ) %>%
         select(
            any_of(view_vars),
            transmit
         )

      check[["no_gender_ident"]] <- data %>%
         filter(
            !is.na(self_identity),
            is.na(gender_identity)
         ) %>%
         select(
            any_of(view_vars),
            gender_identity
         )

      # range-median
      tabstat <- c(
         "age_unknown",
         "age_vertical",
         "age_pregnant",
         "yrlastfsex",
         "howmanyfse",
         "yrlastmsex",
         "howmanymse",
         "age_sex",
         "age_inj"
      )
      check   <- check_tabstat(data, check, tabstat)
   }

   return(check)
}

##  Actual flow ----------------------------------------------------------------

.init <- function() {
   p <- parent.env(environment())
   local(envir = p, {
      data <- read_rds(file.path(wd, "initial.RDS"))
      data <- standardize_data(data) %>%
         tag_mot() %>%
         tag_class() %>%
         convert_address() %>%
         convert_faci_names() %>%
         final_conversion()

      write_rds(data, file.path(wd, "converted.RDS"))

      check <- get_checks(data)
   })

   local(envir = .GlobalEnv, flow_validation(nhsss$harp_dx, "converted", ohasis$ym))
}