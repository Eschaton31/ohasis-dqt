##  Initial Cleaning -----------------------------------------------------------

clean_data <- function(forms, dup_munc) {
   log_info("Processing new positives.")
   data <- process_hts(forms$form_hts, forms$form_a, forms$form_cfbs) %>%
      bind_rows(forms$px_confirmed %>% mutate(SNAPSHOT = as.POSIXct(SNAPSHOT))) %>%
      distinct(REC_ID, .keep_all = TRUE) %>%
      select(-any_of(c("CONFIRM_RESULT", "CONFIRM_REMARKS"))) %>%
      left_join(
         y  = forms$px_confirmed %>%
            select(REC_ID, CONFIRM_RESULT, CONFIRM_REMARKS),
         by = join_by(REC_ID)
      ) %>%
      mutate_at(
         .vars = vars(FIRST, MIDDLE, LAST, SUFFIX, PATIENT_CODE, UIC, PHILHEALTH_NO, PHILSYS_ID, CLIENT_MOBILE, CLIENT_EMAIL),
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
      get_latest_pii(
         "CENTRAL_ID",
         c(
            "FIRST",
            "MIDDLE",
            "LAST",
            "SUFFIX",
            "BIRTHDATE",
            "SEX",
            "UIC",
            "PHILHEALTH_NO",
            "SELF_IDENT",
            "SELF_IDENT_OTHER",
            "PHILSYS_ID",
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
            "BIRTH_PSGC_MUNC",
            "CLIENT_MOBILE",
            "CLIENT_EMAIL"
         )
      ) %>%
      rename(
         blood_extract_date    = DATE_COLLECT,
         specimen_receipt_date = DATE_RECEIVE,
         confirm_date          = DATE_CONFIRM,
      ) %>%
      mutate(
         # month of labcode/date received
         lab_month      = coalesce(
            str_extract(CONFIRM_CODE, "[A-Z]+([0-9][0-9])-([0-9][0-9])", 2),
            stri_pad_left(month(specimen_receipt_date), 2, "0")
         ),

         # year of labcode/date received
         lab_year       = coalesce(
            stri_c("20", str_extract(CONFIRM_CODE, "[A-Z]+([0-9][0-9])-([0-9][0-9])", 1)),
            stri_pad_left(year(specimen_receipt_date), 4, "0")
         ),

         # date variables
         visit_date     = RECORD_DATE,

         # date var for keeping
         report_date    = as.Date(stri_c(sep = "-", lab_year, lab_month, "01")),

         # name
         STANDARD_FIRST = stri_trans_general(FIRST, "latin-ascii"),
         name           = str_squish(stri_c(LAST, ", ", FIRST, " ", MIDDLE, " ", SUFFIX)),

         # Permanent
         PERM_PSGC_PROV = if_else(StrLeft(PERM_PSGC_REG, 2) == "99", "999900000", PERM_PSGC_PROV, PERM_PSGC_PROV),
         PERM_PSGC_MUNC = if_else(StrLeft(PERM_PSGC_REG, 2) == "99", "999999000", PERM_PSGC_MUNC, PERM_PSGC_MUNC),
         use_curr       = if_else(
            condition = !is.na(CURR_PSGC_MUNC) & (is.na(PERM_PSGC_MUNC) | StrLeft(PERM_PSGC_MUNC, 2) == "99"),
            true      = 1,
            false     = 0
         ),
         PERM_PSGC_REG  = if_else(
            condition = use_curr == 1,
            true      = CURR_PSGC_REG,
            false     = PERM_PSGC_REG
         ),
         PERM_PSGC_PROV = if_else(
            condition = use_curr == 1,
            true      = CURR_PSGC_PROV,
            false     = PERM_PSGC_PROV
         ),
         PERM_PSGC_MUNC = if_else(
            condition = use_curr == 1,
            true      = CURR_PSGC_MUNC,
            false     = PERM_PSGC_MUNC
         ),

         # Age
         AGE            = coalesce(AGE, AGE_MO / 12),
         AGE_DTA        = calc_age(BIRTHDATE, visit_date),
      ) %>%
      left_join(
         y  = dup_munc %>%
            select(
               PERM_PSGC_MUNC = PSGC_MUNC,
               DUP_MUNC
            ),
         by = "PERM_PSGC_MUNC"
      )

   return(data)
}

##  Sorting confirmatory results -----------------------------------------------

prioritize_reports <- function(data) {
   log_info("Using first visited facility.")
   data %<>%
      arrange(CONFIRM_RESULT, lab_year, lab_month, desc(CONFIRM_TYPE), visit_date, confirm_date) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      filter(report_date < ohasis$next_date | is.na(report_date)) %>%
      rename(
         TEST_FACI     = SERVICE_FACI,
         TEST_SUB_FACI = SERVICE_SUB_FACI,
      )

   return(data)
}

##  Adding CD4 results ---------------------------------------------------------

get_cd4 <- function(data, lab_cd4) {
   log_info("Attaching baseline cd4.")
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
         CD4_CONFIRM  = interval(CD4_DATE, confirm_date) / days(1),

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

standardize_data <- function(initial, params) {
   log_info("Converting to final HARP variables.")
   data <- initial %>%
      mutate(
         # generate idnum
         idnum                     = if_else(
            condition = is.na(IDNUM),
            true      = params$latest_idnum + row_number(),
            false     = as.integer(IDNUM)
         ),

         # report date
         year                      = params$yr,
         month                     = params$mo,

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

         # confirmatory info
         test_done                 = case_when(
            str_detect(toupper(T3_KIT), "GEENIUS") ~ "GEENIUS",
            str_detect(toupper(T3_KIT), "STAT-PAK") ~ "STAT-PAK",
            str_detect(toupper(T3_KIT), "MP DIAGNOSTICS") ~ "WESTERN BLOT",
            AGE <= 1 ~ "PCR"
         ),
         rhivda_done               = if_else(
            condition = StrLeft(CONFIRM_TYPE, 1) == "2",
            true      = 1,
            false     = as.numeric(NA)
         ),
         sample_source             = substr(SPECIMEN_REFER_TYPE, 3, 3),

         # demographics
         pxcode                    = str_squish(stri_c(StrLeft(FIRST, 1), StrLeft(MIDDLE, 1), StrLeft(LAST, 1))),
         SEX                       = remove_code(stri_trans_toupper(SEX)),
         self_identity             = remove_code(stri_trans_toupper(SELF_IDENT)),
         self_identity             = case_when(
            self_identity == "OTHER" ~ "OTHERS",
            self_identity == "MAN" ~ "MALE",
            self_identity == "WOMAN" ~ "FEMALE",
            self_identity == "MALE" ~ "MALE",
            self_identity == "FEMALE" ~ "FEMALE",
            TRUE ~ self_identity
         ),
         self_identity_other       = stri_trans_toupper(SELF_IDENT_OTHER),
         self_identity_other_sieve = str_replace_all(self_identity_other, "[^[:alnum:]]", ""),

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
         who_staging               = as.integer(keep_code(WHO_CLASS)),
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
      generate_gender_identity(SEX, SELF_IDENT, SELF_IDENT_OTHER, gender_identity)

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

tag_class <- function(data, corr) {
   log_info("Tagging AHD.")
   data %<>%
      mutate(
         # cd4 tagging
         days_cd4_confirm     = interval(CD4_DATE, confirm_date) / days(1),
         cd4_is_baseline      = if_else(condition = days_cd4_confirm <= 182, 1, 0, 0),
         CD4_DATE             = case_when(
            cd4_is_baseline == 0 ~ NA_Date_,
            is.na(CD4_RESULT) ~ NA_Date_,
            TRUE ~ CD4_DATE
         ),
         CD4_RESULT           = case_when(
            cd4_is_baseline == 0 ~ NA_character_,
            TRUE ~ CD4_RESULT
         ),
         CD4_RESULT           = stri_replace_all_charclass(CD4_RESULT, "[:alpha:]", "") %>%
            stri_replace_all_fixed(" ", "") %>%
            stri_replace_all_fixed("<", "") %>%
            as.numeric(),
         baseline_cd4         = case_when(
            CD4_RESULT >= 500 ~ 1,
            CD4_RESULT >= 350 & CD4_RESULT < 500 ~ 2,
            CD4_RESULT >= 200 & CD4_RESULT < 350 ~ 3,
            CD4_RESULT >= 50 & CD4_RESULT < 200 ~ 4,
            CD4_RESULT < 50 ~ 5,
         ),

         # WHO Case Definition of advanced HIV classification
         # refined ahd
         ahd                  = case_when(
            who_staging %in% c(3, 4) ~ 1,
            AGE >= 5 & baseline_cd4 %in% c(4, 5) ~ 1,
            AGE < 5 ~ 1,
            !is.na(baseline_cd4) ~ 0
         ),
         baseline_cd4         = labelled(
            baseline_cd4,
            c(
               "1_500+ cells/μL"    = 1,
               "2_350-499 cells/μL" = 2,
               "3_200-349 cells/μL" = 3,
               "4_50-199 cells/μL"  = 4,
               "5_below 50"         = 5
            )
         ),

         # tb patient
         # class
         classd               = if_else(
            condition = !is.na(who_staging),
            true      = who_staging,
            false     = NA_integer_
         ) %>% as.numeric(),
         description_symptoms = stri_trans_toupper(SYMPTOMS),
         MED_TB_PX            = case_when(
            stri_detect_fixed(description_symptoms, "TB") ~ 1,
            TRUE ~ as.numeric(MED_TB_PX)
         ),
         classd               = case_when(
            stri_detect_regex(description_symptoms, paste(collapse = "|", (corr$classd %>% filter(as.numeric(class) == 3))$symptom)) ~ 3,
            MED_TB_PX == 1 ~ 3,
            TRUE ~ classd
         ),
         classd               = case_when(
            stri_detect_regex(description_symptoms, paste(collapse = "|", (corr$classd %>% filter(as.numeric(class) == 4))$symptom)) ~ 4,
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

         # no data for stage of hiv
         nodata_hiv_stage     = if_else(
            if_all(c(who_staging, description_symptoms, MED_TB_PX, CLINICAL_PIC), ~is.na(.)),
            1,
            0,
            0
         ),

         # form (HTS)
         FORM_VERSION         = if_else(FORM_VERSION == " (vNA)", NA_character_, FORM_VERSION),

         # provider type (HTS)
         PROVIDER_TYPE        = as.integer(keep_code(PROVIDER_TYPE)),

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

##  Facilities & Address -------------------------------------------------------

convert_faci_addr <- function(data) {
   log_info("Converting address & facility data.")
   # rename columns
   data %<>%
      ohasis$get_addr(
         c(
            region   = "PERM_PSGC_REG",
            province = "PERM_PSGC_PROV",
            muncity  = "PERM_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      ohasis$get_addr(
         c(
            region_c   = "CURR_PSGC_REG",
            province_c = "CURR_PSGC_PROV",
            muncity_c  = "CURR_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      ohasis$get_addr(
         c(
            region01   = "BIRTH_PSGC_REG",
            province01 = "BIRTH_PSGC_PROV",
            placefbir  = "BIRTH_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      ohasis$get_addr(
         c(
            region_p   = "PERMONLY_PSGC_REG",
            province_p = "PERMONLY_PSGC_PROV",
            muncity_p  = "PERMONLY_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      ohasis$get_addr(
         c(
            venue_region   = "HIV_SERVICE_PSGC_REG",
            venue_province = "HIV_SERVICE_PSGC_PROV",
            venue_muncity  = "HIV_SERVICE_PSGC_MUNC"
         ),
         "nhsss"
      ) %>%
      # country names
      left_join(
         y  = ohasis$ref_country %>%
            select(COUNTRY_CODE, ocw_country = COUNTRY_NAME),
         by = join_by(OFW_COUNTRY == COUNTRY_CODE)
      ) %>%
      relocate(ocw_country, .before = OFW_COUNTRY) %>%
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
            select(
               TEST_FACI     = FACI_ID,
               TEST_SUB_FACI = SUB_FACI_ID,
               pubpriv       = PUBPRIV
            ),
         by         = join_by(TEST_FACI, TEST_SUB_FACI)
      ) %>%
      mutate(
         FORM_FACI_2        = TEST_FACI,
         FORM_FACI          = TEST_FACI,
         SUB_FORM_FACI      = TEST_SUB_FACI,
         diff_source_v_form = if_else(coalesce(FORM_FACI, "") != coalesce(SPECIMEN_SOURCE, "") & (sample_source == "R" | is.na(sample_source)), 1, 0, 0)
      ) %>%
      ohasis$get_faci(
         list(HTS_FACI = c("FORM_FACI", "SUB_FORM_FACI")),
         "name"
      ) %>%
      ohasis$get_faci(
         list(SOURCE_FACI = c("SPECIMEN_SOURCE", "SPECIMEN_SUB_SOURCE")),
         "name"
      ) %>%
      # confirmlab
      ohasis$get_faci(
         list(confirmlab = c("CONFIRM_FACI", "CONFIRM_SUB_FACI")),
         "code",
         c("confirm_region", "confirm_province", "confirm_muncity")
      ) %>%
      ohasis$get_faci(
         list(dxlab_standard = c("TEST_FACI", "TEST_SUB_FACI")),
         "nhsss",
         c("dx_region", "dx_province", "dx_muncity")
      ) %>%
      rename(
         FORM_FACI = FORM_FACI_2
      )

   return(data)
}

##  Finalize -------------------------------------------------------------------

final_conversion <- function(data) {
   data %<>%
      mutate(
         labcode2 = CONFIRM_CODE
      ) %>%
      # same vars as registry
      select(
         REC_ID,
         CENTRAL_ID,
         PATIENT_ID,
         idnum,
         form                      = FORM_VERSION,
         modality                  = hts_modality,          # HTS Form
         consent_test              = test_agreed,           # HTS Form
         labcode                   = CONFIRM_CODE,
         labcode2,
         year,
         month,
         uic                       = UIC,
         firstname                 = FIRST,
         middle                    = MIDDLE,
         last                      = LAST,
         name_suffix               = SUFFIX,
         bdate                     = BIRTHDATE,
         patient_code              = PATIENT_CODE,
         pxcode,
         age                       = AGE,
         age_months                = AGE_MO,
         sex                       = SEX,
         philhealth                = PHILHEALTH_NO,
         philsys_id                = PHILSYS_ID,
         mobile                    = CLIENT_MOBILE,
         email                     = CLIENT_EMAIL,
         muncity,
         province,
         region,
         muncity_c,
         province_c,
         region_c,
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
         mot,
         starts_with("risk_", ignore.case = FALSE),
         class,
         class2022,
         ahd,
         baseline_cd4,
         baseline_cd4_date         = CD4_DATE,
         baseline_cd4_result       = CD4_RESULT,
         confirm_date,
         confirmlab,
         confirm_region,
         confirm_province,
         confirm_muncity,
         confirm_result            = CONFIRM_RESULT,
         confirm_remarks           = CONFIRM_REMARKS,
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
         hts_date,
         t0_date                   = T0_DATE,
         t0_result                 = T0_RESULT,
         test_done,
         name,
         t1_date                   = T1_DATE,
         t1_kit                    = T1_KIT,
         t1_result                 = T1_RESULT,
         t2_date                   = T2_DATE,
         t2_kit                    = T2_KIT,
         t2_result                 = T2_RESULT,
         t3_date                   = T3_DATE,
         t3_kit                    = T3_KIT,
         t3_result                 = T3_RESULT,
         final_interpretation      = CONFIRM_RESULT,
         visit_date,
         blood_extract_date,
         specimen_receipt_date,
         rhivda_done,
         sample_source,
         dxlab_standard,
         pubpriv,
         dx_region,
         dx_province,
         dx_muncity,
         diff_source_v_form,
         SOURCE_FACI,
         HTS_FACI,
         DUP_MUNC,
         FORM_FACI
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
            t1_result,
            t2_result,
            t3_result,
         ),
         ~as.integer(keep_code(.))
      ) %>%
      # remove codes
      mutate_at(
         .vars = vars(
            sex,
            civilstat,
            final_interpretation
         ),
         ~remove_code(.)
      ) %>%
      # fix test data
      mutate_at(
         .vars = vars(
            t1_result,
            t2_result,
            t3_result
         ),
         ~case_when(
            . == 1 ~ "Positive / Reactive",
            . == 2 ~ "Negative / Non-reactive",
            . == 3 ~ "Indeterminate",
            TRUE ~ NA_character_
         )
      ) %>%
      mutate(
         age_pregnant = if_else(
            condition = pregnant == 1,
            true      = age,
            false     = as.numeric(NA)
         ),
         age_vertical = if_else(
            condition = transmit == "PERINATAL",
            true      = age,
            false     = as.numeric(NA)
         ),
         age_unknown  = if_else(
            condition = transmit == "UNKNOWN",
            true      = age,
            false     = as.numeric(NA)
         ),
         pubpriv      = if_else(pubpriv == "0", NA_character_, as.character(pubpriv))
      ) %>%
      distinct_all()

   return(data)
}

##  Append w/ old Registry -----------------------------------------------------

append_data <- function(old, new) {
   log_info("Appending cases to final registry.")
   data <- new %>%
      mutate(
         confirm_date = coalesce(confirm_date, as.Date(t3_date))
      ) %>%
      bind_rows(
         old %>%
            mutate(
               consent_test = as.integer(consent_test),
            )
      ) %>%
      arrange(idnum) %>%
      mutate(
         drop_notyet     = 0,
         drop_duplicates = 0,
      )

   return(data)
}

##  Tag data to be reported later on and duplicates for dropping ---------------

tag_fordrop <- function(data, corr) {
   log_info("Tagging enrollees for dropping.")
   for (drop_var in c("drop_notyet", "drop_duplicates"))
      if (drop_var %in% names(corr))
         data %<>%
            left_join(
               y  = corr[[drop_var]] %>%
                  distinct(REC_ID) %>%
                  mutate(drop_this = 1),
               by = join_by(REC_ID)
            ) %>%
            mutate_at(
               .vars = vars(matches(drop_var)),
               ~coalesce(drop_this, .)
            ) %>%
            select(-drop_this)

   return(data)
}

##  Subsets for documentation --------------------------------------------------

subset_drops <- function(data) {
   log_info("Archive those for dropping.")
   drops <- list(
      dropped_notyet     = data %>% filter(drop_notyet == 1),
      dropped_duplicates = data %>% filter(drop_duplicates == 1)
   )

   return(drops)
}

##  Drop using taggings --------------------------------------------------------

remove_drops <- function(data, params) {
   log_info("Cleaning final dataset.")
   data %<>%
      mutate(
         labcode2    = if_else(
            condition = is.na(labcode2),
            true      = labcode,
            false     = labcode2,
            missing   = labcode2
         ),
         drop        = drop_duplicates + drop_notyet,
         who_staging = as.integer(who_staging)
      ) %>%
      filter(drop == 0) %>%
      select(
         -drop,
         -drop_duplicates,
         -drop_notyet,
         -mot,
         -FORM_FACI,
         -any_of(
            c(
               "diff_source_v_form",
               "SOURCE_FACI",
               "HTS_FACI",
               "DUP_MUNC",
               "age_pregnant",
               "age_vertical",
               "age_unknown",
               "CONFIRM_FACI",
               "TEST_FACI",
               "CD4_CONFIRM",
               "update_ocw"
            )
         )
      )

   final_new <- data %>%
      filter(year == as.numeric(params$yr), month == as.numeric(params$mo))

   nrow_new  <- nrow(final_new)
   nrow_ahd  <- final_new %>%
      filter(class2022 == "AIDS") %>%
      nrow()
   perc_ahd  <- stri_c(format((nrow_ahd / nrow_new) * 100, digits = 2), "%")
   nrow_none <- final_new %>%
      filter(transmit == "UNKNOWN") %>%
      nrow()
   perc_none <- stri_c(format((nrow_none / nrow_new) * 100, digits = 2), "%")
   nrow_mtct <- final_new %>%
      filter(transmit == "PERINATAL") %>%
      nrow()
   perc_mtct <- stri_c(format((nrow_mtct / nrow_new) * 100, digits = 2), "%")

   log_info("New cases    = {green(stri_pad_left(nrow_new, 4, ' '))}.")
   log_info("New AHD      = {green(stri_pad_left(nrow_ahd, 4, ' '))}, {red(perc_ahd)}.")
   log_info("New Unknown  = {green(stri_pad_left(nrow_none, 4, ' '))}, {red(perc_none)}.")
   log_info("New Vertical = {green(stri_pad_left(nrow_mtct, 4, ' '))}, {red(perc_mtct)}.")

   return(data)
}

##  Flag data for validation ---------------------------------------------------

get_checks <- function(data, pdf_rhivda, corr, run_checks = NULL, exclude_drops = NULL) {
   check         <- list()
   run_checks    <- ifelse(
      !is.null(run_checks),
      run_checks,
      input(
         prompt  = "Run `hts_tst_pos` validations?",
         options = c("1" = "yes", "2" = "no"),
         default = "1"
      )
   )
   exclude_drops <- switch(
      run_checks,
      `1`     = ifelse(!is.null(exclude_drops), exclude_drops, input(
         prompt  = "Exclude clients initially tagged for dropping from validations?",
         options = c("1" = "yes", "2" = "no"),
         default = "1"
      )),
      default = "2"
   )

   if (run_checks == "1") {
      data %<>%
         mutate(
            reg_order = confirm_region,
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
         arrange(reg_order, confirmlab, labcode) %>%
         select(-reg_order)

      view_vars <- c(
         "REC_ID",
         "PATIENT_ID",
         "confirm_region",
         "confirmlab",
         "form",
         "labcode",
         "uic",
         "patient_code",
         "firstname",
         "middle",
         "last",
         "name_suffix",
         "bdate",
         "sex",
         "self_identity",
         "self_identity_other",
         "gender_identity",
         "sample_source",
         "HTS_FACI",
         "SOURCE_FACI",
         "hts_date",
         "hts_modality",
         "visit_date",
         "blood_extract_date",
         "specimen_receipt_date",
         "confirm_date",
         "mot",
         "transmit",
         "sexhow",
         "confirm_result",
         "confirm_remarks",
         "FORM_FACI"
      )
      check     <- check_pii(data, check, view_vars, first = firstname, middle = middle, last = last, birthdate = bdate, sex = sex)

      # dates
      date_vars <- c(
         "visit_date",
         "blood_extract_date",
         "specimen_receipt_date",
         "hts_date"
      )
      check     <- check_dates(data, check, view_vars, date_vars)
      check[["blood_extract_date"]] %<>%
         filter(confirmlab != "SACCL")

      # non-negotiable variables
      nonnegotiables <- c(
         "uic",
         "age",
         "confirmlab",
         "form",
         "sample_source",
         "self_identity",
         "nationality",
         "confirm_date"
      )
      check          <- check_nonnegotiables(data, check, view_vars, nonnegotiables)
      check          <- check_unknown(data, check, "perm_addr", view_vars, region, province, muncity)
      check          <- check_unknown(data, check, "curr_addr", view_vars, region_c, province_c, muncity_c)
      check          <- check_unknown(data, check, "dxlab_data", view_vars, dxlab_standard, pubpriv)
      check          <- check_preggy(data, check, view_vars, sex = sex)
      check          <- check_age(data, check, view_vars, birthdate = bdate, age = age, visit_date = visit_date)

      # test kits
      log_info("Checking invalid test kits.")
      check[["t1_data"]] <- data %>%
         filter(confirmlab != "SACCL") %>%
         mutate(
            keep = case_when(
               !str_detect(t1_kit, "Bioline") ~ 1,
               if_any(c(t1_kit, t1_result, t1_date), ~is.na(.)) ~ 1,
               t1_date > t2_date ~ 1,
               t1_date > t3_date ~ 1,
               t1_date > confirm_date ~ 1,
               t1_date < specimen_receipt_date ~ 1,
               t1_date < blood_extract_date ~ 1,
               t1_date < hts_date ~ 1,
               TRUE ~ 0
            )
         ) %>%
         filter(
            keep == 1
         ) %>%
         select(
            any_of(view_vars),
            starts_with("t1")
         )

      check[["t2_data"]] <- data %>%
         filter(confirmlab != "SACCL") %>%
         mutate(
            keep = case_when(
               !str_detect(t2_kit, "Bioline") ~ 1,
               if_any(c(t2_kit, t2_result, t2_date), ~is.na(.)) ~ 1,
               t2_date > t3_date ~ 1,
               t2_date > confirm_date ~ 1,
               t2_date < specimen_receipt_date ~ 1,
               t2_date < blood_extract_date ~ 1,
               t2_date < hts_date ~ 1,
               t2_date < t1_date ~ 1,
               t2_result != 1 ~ 1,
               TRUE ~ 0
            )
         ) %>%
         filter(
            keep == 1
         ) %>%
         select(
            any_of(view_vars),
            starts_with("t2")
         )

      check[["t3_data"]] <- data %>%
         filter(confirmlab != "SACCL") %>%
         mutate(
            keep = case_when(
               !str_detect(t3_kit, "Bioline") ~ 1,
               if_any(c(t3_kit, t3_result, t3_date), ~is.na(.)) ~ 1,
               t3_date > confirm_date ~ 1,
               t3_date < specimen_receipt_date ~ 1,
               t3_date < blood_extract_date ~ 1,
               t3_date < hts_date ~ 1,
               t3_date < t1_date ~ 1,
               t3_date < t2_date ~ 1,
               t3_result != 1 ~ 1,
               TRUE ~ 0
            )
         ) %>%
         filter(
            keep == 1
         ) %>%
         select(
            any_of(view_vars),
            starts_with("t3")
         )

      # special checks
      log_info("Checking for non-standard transmission.")
      check[["transmit"]] <- data %>%
         filter(transmit %in% c("OTHERS", "UNKNOWN")) %>%
         select(
            any_of(view_vars),
            starts_with("risk_")
         )

      log_info("Checking for mismatch facilities (source != test).")
      check[["faci_diff_source_v_form"]] <- data %>%
         filter(diff_source_v_form == 1) %>%
         select(
            any_of(view_vars)
         )

      log_info("Checking for similarly named municipalities.")
      check[["dup_munc"]] <- data %>%
         filter(
            DUP_MUNC == 1
         ) %>%
         select(
            any_of(view_vars)
         )

      log_info("Checking for duplicate results w/o a reported positive result.")
      check[["dup_not_positive"]] <- data %>%
         filter(
            confirm_result == "5_Duplicate"
         ) %>%
         select(
            any_of(view_vars)
         )

      # pdf results
      log_info("Checking missing rHIVda PDF.")
      check[["no_pdf_result"]] <- data %>%
         filter(confirmlab != "SACCL") %>%
         anti_join(
            y  = pdf_rhivda$data %>% select(CONFIRM_CODE),
            by = join_by(labcode == CONFIRM_CODE)
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for MTCT.")
      check[["perinatal"]] <- data %>%
         filter(
            transmit == "PERINATAL"
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
         "visit_date",
         "blood_extract_date",
         "specimen_receipt_date",
         "confirm_date",
         "t1_date",
         "t2_date",
         "t3_date",
         "age_unknown",
         "age_vertical",
         "age_pregnant",
         "date_lastsex_m",
         "date_lastsex_condomless_m",
         "date_lastsex_f",
         "date_lastsex_condomless_f",
         "bdate",
         "age"
      )
      check   <- check_tabstat(data, check, tabstat)

      # Remove already tagged data from validation
      if (exclude_drops == "1") {
         for (drop in c("drop_notart", "drop_notyet")) {
            if (drop %in% names(corr))
               for (check_var in names(check)) {
                  if (check_var != "tabstat")
                     check[[check_var]] %<>%
                        anti_join(
                           y  = corr[[drop]],
                           by = "REC_ID"
                        )
               }
         }
      }
   }

   return(check)
}

##  Stata Labels ---------------------------------------------------------------

label_stata <- function(newdx, stata_labels) {
   labels <- split(stata_labels$lab_def, ~label_name)
   labels <- lapply(labels, function(data) {
      final_labels        <- as.integer(data[["value"]])
      names(final_labels) <- as.character(data[["label"]])
      return(final_labels)
   })

   for (i in seq_len(nrow(stata_labels$lab_val))) {
      var   <- stata_labels$lab_val[i,]$variable
      label <- stata_labels$lab_val[i,]$label_name

      if (var %in% names(newdx))
         newdx[[var]] <- labelled(
            newdx[[var]],
            labels[[label]]
         )
   }

   return(newdx)
}

##  Output Stata Datasets ------------------------------------------------------

output_dta <- function(official, params, save = "2") {
   if (save == "1") {
      log_info("Checking output directory.")
      version <- format(Sys.time(), "%Y%m%d")
      dir     <- Sys.getenv("HARP_DX")
      check_dir(dir)

      log_info("Saving in Stata data format.")
      period_ext <- stri_c(params$yr, "-", stri_pad_left(params$mo, 2, "0"), ".dta")
      files      <- list(
         new                = file.path(dir, stri_c(version, "_reg_", period_ext)),
         dropped_notyet     = file.path(dir, stri_c(version, "_dropped_notyet_", period_ext)),
         dropped_duplicates = file.path(dir, stri_c(version, "_dropped_duplicates_", period_ext))
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

   data <- clean_data(p$forms, p$corr$dup_munc)
   data <- prioritize_reports(data)
   data <- get_cd4(data, p$forms$cd4)
   data <- standardize_data(data, p$params)
   data <- tag_mot(data, p$params)
   data <- tag_class(data, p$corr)
   data <- convert_faci_addr(data)
   data <- final_conversion(data)

   new_reg <- append_data(p$official$old, data)
   new_reg <- tag_fordrop(new_reg, p$corr)
   drops   <- subset_drops(new_reg)
   new_reg <- remove_drops(new_reg, p$params)
   new_reg <- label_stata(new_reg, p$corr$stata_labels)

   step$check <- get_checks(data, p$pdf_rhivda, p$corr, run_checks = vars$run_checks, exclude_drops = vars$exclude_drops)
   step$data  <- data

   p$official$new <- new_reg
   append(p$official, drops)
   output_dta(p$official, p$params, vars$save)

   flow_validation(p, "hts_tst_pos", p$params$ym, upload = vars$upload)
   log_success("Done.")
}