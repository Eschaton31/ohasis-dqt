##  Initial Cleaning -----------------------------------------------------------

clean_data <- function(forms) {
   log_info("Processing new positives.")
   hts          <- process_hts(forms$form_hts, forms$form_a, forms$form_cfbs, forms$px_confirmed)
   confirm_cols <- names(forms$px_confirm)
   confirm_cols <- confirm_cols[!(confirm_cols %in% c("rec_id", "central_id"))]

   same           <- forms$px_confirmed %>%
      inner_join(
         y  = hts %>%
            filter(!is.na(form_version)) %>%
            mutate(
               hts_rec = rec_id,
            ) %>%
            select(rec_id, hts_rec),
         by = join_by(rec_id)
      )
   before_confirm <- forms$px_confirmed %>%
      anti_join(same, join_by(rec_id)) %>%
      left_join(
         y  = hts %>%
            filter(!is.na(form_version)) %>%
            mutate(
               hts_rec = rec_id,
            ) %>%
            select(
               central_id,
               hts_rec,
               hts_visit = record_date
            ),
         by = join_by(central_id, closest(record_date >= hts_visit))
      )
   after_confirm  <- forms$px_confirmed %>%
      anti_join(same, join_by(rec_id)) %>%
      anti_join(before_confirm, join_by(rec_id)) %>%
      left_join(
         y  = hts %>%
            filter(!is.na(form_version)) %>%
            mutate(
               hts_rec = rec_id,
            ) %>%
            select(
               central_id,
               hts_rec,
               hts_visit = record_date
            ),
         by = join_by(central_id, closest(record_date <= hts_visit))
      )


   data <- bind_rows(same, before_confirm, after_confirm) %>%
      left_join(
         y  = hts %>%
            rename(
               hts_rec = rec_id,
            ) %>%
            select(-any_of(confirm_cols), -central_id),
         by = join_by(hts_rec),
      ) %>%
      mutate_at(
         .vars = vars(first, middle, last, suffix, patient_code, uic, philhealth_no, philsys_id, client_mobile, client_email),
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
         "central_id",
         c(
            "first",
            "middle",
            "last",
            "suffix",
            "birthdate",
            "sex",
            "uic",
            "philhealth_no",
            "self_ident",
            "self_ident_other",
            "philsys_id",
            "civil_status",
            "nationality",
            "educ_level",
            "curr_reg",
            "curr_prov",
            "curr_munc",
            "perm_reg",
            "perm_prov",
            "perm_munc",
            "birth_reg",
            "birth_prov",
            "birth_munc",
            "client_mobile",
            "client_email"
         )
      ) %>%
      rename(
         blood_extract_date    = date_collect,
         specimen_receipt_date = date_receive,
         confirm_date          = date_confirm,
      ) %>%
      mutate(
         # month of labcode/date received
         lab_month      = coalesce(
            str_extract(confirm_code, "[A-Z]+([0-9][0-9])-([0-9][0-9])", 2),
            stri_pad_left(month(specimen_receipt_date), 2, "0")
         ),

         # year of labcode/date received
         lab_year       = coalesce(
            stri_c("20", str_extract(confirm_code, "[A-Z]+([0-9][0-9])-([0-9][0-9])", 1)),
            stri_pad_left(year(specimen_receipt_date), 4, "0")
         ),

         # date variables
         visit_date     = record_date,

         # date var for keeping
         report_date    = as.Date(coalesce(specimen_receipt_date, visit_date)),
         # report_date    = as.Date(stri_c(sep = "-", lab_year, lab_month, "01")),

         # name
         standard_first = stri_trans_general(first, "latin-ascii"),
         name           = str_squish(stri_c(last, ", ", first, " ", middle, " ", suffix)),

         # Permanent
         perm_prov      = if_else(str_left(perm_reg, 2) == "99", "9999000000", perm_prov, perm_prov),
         perm_munc      = if_else(str_left(perm_reg, 2) == "99", "9999990000", perm_munc, perm_munc),
         use_curr       = if_else(
            condition = !is.na(curr_munc) & (is.na(perm_munc) | str_left(perm_munc, 2) == "99"),
            true      = 1,
            false     = 0
         ),
         perm_reg       = if_else(
            condition = use_curr == 1,
            true      = curr_reg,
            false     = perm_reg
         ),
         perm_prov      = if_else(
            condition = use_curr == 1,
            true      = curr_prov,
            false     = perm_prov
         ),
         perm_munc      = if_else(
            condition = use_curr == 1,
            true      = curr_munc,
            false     = perm_munc
         ),

         # Age
         age            = coalesce(age, age_mo / 12),
         age_dta        = calc_age(birthdate, visit_date),

         form_sort      = if_else(rec_id == hts_rec, 1, 9999, 9999)
      ) %>%
      rename(country_code = nationality) %>%
      left_join(
         y  = ohasis$ref_country %>%
            select(country_code, nationality = country_name),
         by = join_by(country_code)
      ) %>%
      relocate(nationality, .before = country_code) %>%
      select(-country_code)

   return(data)
}

##  Sorting confirmatory results -----------------------------------------------

prioritize_reports <- function(data) {
   log_info("Using first visited facility.")
   data %<>%
      arrange(confirm_result, form_sort, lab_year, lab_month, desc(confirm_type), hts_date, confirm_date) %>%
      distinct(central_id, .keep_all = TRUE) %>%
      filter(report_date < ohasis$next_date | is.na(report_date)) %>%
      rename(
         test_faci     = service_faci,
         test_sub_faci = service_sub_faci,
      )

   return(data)
}

##  Adding CD4 results ---------------------------------------------------------

get_cd4 <- function(data, lab_cd4) {
   log_info("Attaching baseline cd4.")
   data %<>%
      # get cd4 data
      # todo: attach max dates for filtering of cd4 data
      left_join(
         y  = lab_cd4 %>%
            select(
               cd4_date,
               cd4_result,
               central_id
            ),
         by = join_by(central_id)
      ) %>%
      mutate(
         # calculate distance from confirmatory date
         cd4_date     = as.Date(cd4_date),
         cd4_confirm  = interval(cd4_date, confirm_date) / days(1),

         # baseline is within 182 days
         baseline_cd4 = if_else(
            cd4_confirm >= -182 & cd4_confirm <= 182,
            1,
            0
         ),

         # make values absolute to take date nearest to confirmatory
         cd4_confirm  = abs(cd4_confirm),
      ) %>%
      arrange(rec_id, cd4_confirm) %>%
      distinct(rec_id, .keep_all = TRUE) %>%
      arrange(desc(confirm_type), confirm_code)

   return(data)
}

##  Generate subset variables --------------------------------------------------

standardize_data <- function(initial, params) {
   log_info("Converting to final harp variables.")
   data <- initial %>%
      mutate(
         # generate idnum
         idnum                     = coalesce(as.integer(idnum), params$latest_idnum + row_number()),

         # report date
         year                      = params$yr,
         month                     = params$mo,

         # Perm Region (as encoded)
         permonly_reg              = if_else(
            condition = use_curr == 0,
            true      = perm_reg,
            false     = NA_character_
         ),
         permonly_prov             = if_else(
            condition = use_curr == 0,
            true      = perm_prov,
            false     = NA_character_
         ),
         permonly_munc             = if_else(
            condition = use_curr == 0,
            true      = perm_munc,
            false     = NA_character_
         ),

         # tagging vars
         male                      = if_else(
            condition = str_left(sex, 1) == "1",
            true      = 1,
            false     = 0
         ),
         female                    = if_else(
            condition = str_left(sex, 1) == "2",
            true      = 1,
            false     = 0
         ),

         # confirmatory info
         test_done                 = case_when(
            str_detect(toupper(t3_kit), "GEENIUS") ~ "GEENIUS",
            str_detect(toupper(t3_kit), "STAT-PAK") ~ "STAT-PAK",
            str_detect(toupper(t3_kit), "MP DIAGNOSTICS") ~ "WESTERN BLOT",
            age <= 1 ~ "pcr"
         ),
         rhivda_done               = if_else(
            condition = str_left(confirm_type, 1) == "2",
            true      = 1,
            false     = as.numeric(NA)
         ),
         sample_source             = substr(specimen_refer_type, 3, 3),

         # demographics
         pxcode                    = str_squish(stri_c(str_left(first, 1), str_left(middle, 1), str_left(last, 1))),
         sex                       = remove_code(stri_trans_toupper(sex)),
         self_identity             = remove_code(stri_trans_toupper(self_ident)),
         self_identity             = case_when(
            self_identity == "OTHER" ~ "OTHERS",
            self_identity == "MAN" ~ "MALE",
            self_identity == "WOMAN" ~ "FEMALE",
            self_identity == "MALE" ~ "MALE",
            self_identity == "FEMALE" ~ "FEMALE",
            TRUE ~ self_identity
         ),
         self_identity_other       = stri_trans_toupper(self_ident_other),
         self_identity_other_sieve = str_replace_all(self_identity_other, "[^[:alnum:]]", ""),

         civil_status              = stri_trans_toupper(civil_status),
         nationalit                = case_when(
            toupper(nationality) == "PHILIPPINES" ~ "FILIPINO",
            toupper(nationality) != "PHILIPPINES" ~ "NON-FILIPINO",
            TRUE ~ "UNKNOWN"
         ),
         current_school_level      = if_else(
            condition = str_left(is_student, 1) == "1",
            true      = educ_level,
            false     = NA_character_
         ),

         # occupation
         curr_work                 = if_else(
            condition = str_left(is_employed, 1) == "1",
            true      = stri_trans_toupper(work_text),
            false     = NA_character_
         ),
         prev_work                 = if_else(
            condition = str_left(is_employed, 1) == "0" | is.na(is_employed),
            true      = stri_trans_toupper(work_text),
            false     = NA_character_
         ),

         # clinical pic
         who_staging               = as.integer(keep_code(who_class)),
         other_reason_test         = stri_trans_toupper(test_reason_other_text),

         clinical_pic              = case_when(
            str_left(clinical_pic, 1) == "1" ~ "0_Asymptomatic",
            str_left(clinical_pic, 1) == "2" ~ "1_Symptomatic",
         ),

         ofw_station               = case_when(
            str_left(ofw_station, 1) == "1" ~ "1_On ship",
            str_left(ofw_station, 1) == "2" ~ "2_Land",
         ),

         refer_type                = case_when(
            str_left(refer_type, 1) == "1" ~ "1",
            str_left(refer_type, 1) == "2" ~ "1",
         )
      ) %>%
      # exposure history
      mutate_at(
         .vars = vars(starts_with("expose_") & !contains("date")),
         ~if_else(
            condition = !is.na(.),
            true      = str_left(., 1),
            false     = NA_character_,
         ) %>% as.integer()
      ) %>%
      # medical history
      mutate_at(
         .vars = vars(starts_with("med_")),
         ~if_else(
            condition = !is.na(.),
            true      = str_left(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # test reason
      mutate_at(
         .vars = vars(starts_with("test_reason") & !matches("_OTHER")),
         ~if_else(
            condition = !is.na(.),
            true      = str_left(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # mode of reach (HTS)
      mutate_at(
         .vars = vars(starts_with("reach_")),
         ~if_else(
            condition = !is.na(.),
            true      = str_left(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # mode of reach (HTS)
      mutate_at(
         .vars = vars(starts_with("refer")),
         ~if_else(
            condition = !is.na(.),
            true      = str_left(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      # services provided (HTS)
      mutate_at(
         .vars = vars(starts_with("service_")),
         ~if_else(
            condition = !is.na(.),
            true      = str_left(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      generate_gender_identity(sex, self_ident, self_ident_other, gender_identity)

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
            expose_hiv_mother > 0 ~ 1,
            TRUE ~ 0
         ),
         sexwithf   = case_when(
            expose_sex_f > 0 ~ 1,                      # HTS Form
            !is.na(expose_sex_f_av_date) ~ 1,          # HTS Form
            !is.na(expose_sex_f_av_nocondom_date) ~ 1, # HTS Form
            expose_sex_f_nocondom > 0 ~ 1,
            TRUE ~ 0
         ),
         sexwithm   = case_when(
            expose_sex_m > 0 ~ 1,                      # HTS Form
            !is.na(expose_sex_m_av_date) ~ 1,          # HTS Form
            !is.na(expose_sex_m_av_nocondom_date) ~ 1, # HTS Form
            expose_sex_m_nocondom > 0 ~ 1,
            TRUE ~ 0
         ),
         sexwithpro = case_when(
            expose_sex_paying > 0 ~ 1,
            TRUE ~ 0
         ),
         regularlya = case_when(
            expose_sex_payment > 0 ~ 1,
            TRUE ~ 0
         ),
         injectdrug = case_when(
            expose_drug_inject > 0 ~ 1,
            TRUE ~ 0
         ),
         chemsex    = case_when(
            expose_sex_drugs > 0 ~ 1, # HTS Form
            TRUE ~ 0
         ),
         receivedbt = case_when(
            expose_blood_transfuse > 0 ~ 1,
            TRUE ~ 0
         ),
         sti        = case_when(
            expose_sti > 0 ~ 1,
            TRUE ~ 0
         ),
         needlepri1 = case_when(
            expose_occupation > 0 ~ 1,
            TRUE ~ 0
         ),

         mot        = 0,
         # m->m only
         mot        = case_when(
            male == 1 & expose_sex_m_nocondom == 1 ~ 1,
            male == 1 & yr_last_m >= params$p10y ~ 1,
            male == 1 & year(expose_sex_m_av_date) >= params$p10y ~ 1,          # HTS Form
            male == 1 & year(expose_sex_m_av_nocondom_date) >= params$p10y ~ 1, # HTS Form
            TRUE ~ mot
         ),

         # m->m+f
         mot        = case_when(
            mot == 1 & expose_sex_f_nocondom == 1 ~ 2,
            mot == 1 & yr_last_f >= params$p10y ~ 2,
            mot == 1 & year(expose_sex_f_av_date) >= params$p10y ~ 2,          # HTS Form
            mot == 1 & year(expose_sex_f_av_nocondom_date) >= params$p10y ~ 2, # HTS Form
            TRUE ~ mot
         ),

         # m->f only
         mot        = case_when(
            male == 1 & mot == 0 & expose_sex_f_nocondom == 1 ~ 3,
            male == 1 &
               mot == 0 &
               yr_last_f >= params$p10y ~ 3,
            male == 1 &
               mot == 0 &
               year(expose_sex_f_av_date) >= params$p10y ~ 3,          # HTS Form
            male == 1 &
               mot == 0 &
               year(expose_sex_f_av_nocondom_date) >= params$p10y ~ 3, # HTS Form
            TRUE ~ mot
         ),

         # f->m
         mot        = case_when(
            female == 1 & expose_sex_m_nocondom == 1 ~ 4,
            female == 1 & yr_last_m >= params$p10y ~ 4,
            female == 1 & year(expose_sex_m_av_date) >= params$p10y ~ 4,          # HTS Form
            female == 1 & year(expose_sex_m_av_nocondom_date) >= params$p10y ~ 4, # HTS Form
            TRUE ~ mot
         ),

         # ivdu
         mot        = case_when(
            expose_drug_inject > 0 & str_left(perm_prov, 4) == "0722" ~ 5,
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
               num_m_partner > 0 &
               is.na(yr_last_m) ~ 11,
            male == 1 &
               mot == 0 &
               yr_last_m >= params$p10y ~ 11,
            male == 1 &
               mot == 0 &
               !is.na(expose_sex_m_av_date) ~ 11,                     # HTS Form
            male == 1 &
               mot == 0 &
               !is.na(expose_sex_m_av_nocondom_date) ~ 11,            # HTS Form
            male == 1 & mot == 0 & expose_sex_m > 0 ~ 11,             # HTS Form
            TRUE ~ mot
         ),

         # m->m+f hx
         mot        = case_when(
            mot == 1 & num_f_partner > 0 & is.na(yr_last_f) ~ 21,
            mot == 3 & num_m_partner > 0 & is.na(yr_last_m) ~ 21,
            mot == 11 & num_f_partner > 0 & is.na(yr_last_f) ~ 21,
            mot == 11 & yr_last_f >= params$p10y ~ 21,
            mot == 11 & !is.na(expose_sex_f_av_date) ~ 21,          # HTS Form,
            mot == 11 & !is.na(expose_sex_f_av_nocondom_date) ~ 21, # HTS Form,
            mot == 11 & expose_sex_f > 0 ~ 21,                      # HTS Form,
            TRUE ~ mot
         ),

         # m->f hx
         mot        = case_when(
            male == 1 &
               mot == 0 &
               num_f_partner > 0 &
               is.na(yr_last_f) ~ 31,
            male == 1 &
               mot == 0 &
               yr_last_f >= params$p10y ~ 31,
            male == 1 &
               mot == 0 &
               !is.na(expose_sex_f_av_date) ~ 31,                     # HTS Form,
            male == 1 &
               mot == 0 &
               !is.na(expose_sex_f_av_nocondom_date) ~ 31,            # HTS Form,
            male == 1 & mot == 0 & expose_sex_f > 0 ~ 31,             # HTS Form,
            TRUE ~ mot
         ),

         # f->m hx
         mot        = case_when(
            female == 1 &
               mot == 0 &
               num_m_partner > 0 &
               is.na(yr_last_m) ~ 41,
            female == 1 &
               mot == 0 &
               yr_last_m >= params$p10y ~ 41,
            female == 1 &
               mot == 0 &
               !is.na(expose_sex_m_av_date) ~ 41,              # HTS Form,
            female == 1 &
               mot == 0 &
               !is.na(expose_sex_m_av_nocondom_date) ~ 41,     # HTS Form,
            female == 1 & mot == 0 & expose_sex_m > 0 ~ 41,    # HTS Form,
            TRUE ~ mot
         ),

         # ivdu hx
         mot        = case_when(
            injectdrug > 0 & str_left(perm_prov, 4) == "0722" ~ 51,
            TRUE ~ mot
         ),

         # mtct
         mot        = case_when(
            mot == 0 & age < 5 ~ 61,
            TRUE ~ mot
         ),

         # all else fails
         mot        = case_when(
            male == 1 & mot == 0 & num_m_partner > 0 ~ 1,
            TRUE ~ mot
         ),
         mot        = case_when(
            mot == 1 & num_f_partner > 0 ~ 2,
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
            female == 1 & mot == 0 & num_f_partner > 0 ~ 10,
            female == 1 & mot == 0 & !is.na(yr_last_f) > 0 ~ 10,
            female == 1 &
               mot == 0 &
               !is.na(expose_sex_f_av_date) ~ 10,
            female == 1 &
               mot == 0 &
               !is.na(expose_sex_f_av_nocondom_date) ~ 10,
            female == 1 & mot == 0 & expose_sex_f > 0 ~ 10,
            TRUE ~ mot
         ),

         # # clean mot_09
         # mot                  = case_when(
         #    mot %in% c(7, 8) ~ 9,
         #    TRUE ~ mot
         # ),

         # final filtering of mot using risk_*
         mot        = case_when(
            mot %in% c(7, 8, 9, 10) &
               male == 1 &
               str_detect(risk_sexwithm, "^yes") &
               str_detect(risk_sexwithf, "^yes") ~ 22,
            mot %in% c(7, 8, 9, 10) &
               male == 1 &
               str_detect(risk_sexwithm, "^yes") &
               !str_detect(risk_sexwithf, "^yes") ~ 12,
            mot %in% c(7, 8, 9, 10) &
               male == 1 &
               !str_detect(risk_sexwithm, "^yes") &
               str_detect(risk_sexwithf, "^yes") ~ 32,
            mot %in% c(7, 8, 9, 10) &
               female == 1 &
               str_detect(risk_sexwithm, "^yes") &
               !str_detect(risk_sexwithf, "^yes") ~ 42,
            mot %in% c(7, 8, 9, 10) & str_detect(risk_injectdrug, "^yes") ~ 52,
            TRUE ~ mot
         ),

         # transmit
         transmit   = case_when(
            mot %in% c(1, 2, 3, 4, 11, 12, 21, 22, 31, 32, 41, 42) ~ "SEX",
            mot %in% c(5, 51, 52) ~ "IVDU",
            mot %in% c(6, 61) ~ "PERINATAL",
            mot %in% c(8, 9, 10) ~ "UNKNOWN",
            mot == 7 ~ "OTHERS",
         ),

         # sexhow
         sexhow     = case_when(
            mot %in% c(1, 11, 12) ~ "HOMOSEXUAL",
            mot %in% c(2, 21, 22) ~ "BISEXUAL",
            mot %in% c(3, 4, 31, 32, 41, 42) ~ "HETEROSEXUAL",
         ),
      )

   return(data)
}

##  Advanced HIV disease -------------------------------------------------------

tag_class <- function(data, corr) {
   log_info("Tagging ahd.")
   data %<>%
      mutate(
         # cd4 tagging
         days_cd4_confirm     = interval(cd4_date, confirm_date) / days(1),
         cd4_is_baseline      = if_else(abs(days_cd4_confirm) <= 182, 1, 0, 0),
         cd4_date             = case_when(
            cd4_is_baseline == 0 ~ NA_Date_,
            is.na(cd4_result) ~ NA_Date_,
            TRUE ~ cd4_date
         ),
         cd4_result           = case_when(
            cd4_is_baseline == 0 ~ NA_character_,
            TRUE ~ cd4_result
         ),
         cd4_result           = parse_number(cd4_result),
         baseline_cd4         = case_when(
            cd4_result >= 500 ~ 1,
            cd4_result >= 350 & cd4_result < 500 ~ 2,
            cd4_result >= 200 & cd4_result < 350 ~ 3,
            cd4_result >= 50 & cd4_result < 200 ~ 4,
            cd4_result < 50 ~ 5,
         ),

         # who Case Definition of advanced HIV classification
         # refined ahd
         ahd                  = case_when(
            who_staging %in% c(3, 4) ~ 1,
            age >= 5 & baseline_cd4 %in% c(4, 5) ~ 1,
            age < 5 ~ 1,
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
         description_symptoms = stri_trans_toupper(symptoms),
         med_tb_px            = case_when(
            stri_detect_fixed(description_symptoms, "TB") ~ 1,
            TRUE ~ as.numeric(med_tb_px)
         ),
         classd               = case_when(
            stri_detect_regex(description_symptoms, paste(collapse = "|", (corr$corr_classd %>% filter(as.numeric(class) == 3))$symptom)) ~ 3,
            med_tb_px == 1 ~ 3,
            TRUE ~ classd
         ),
         classd               = case_when(
            stri_detect_regex(description_symptoms, paste(collapse = "|", (corr$corr_classd %>% filter(as.numeric(class) == 4))$symptom)) ~ 4,
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
            if_all(c(who_staging, description_symptoms, med_tb_px, clinical_pic), ~is.na(.)),
            1,
            0,
            0
         ),

         # form (HTS)
         form_version         = if_else(form_version == " (vNA)", NA_character_, form_version),

         # provider type (HTS)
         provider_type        = as.integer(keep_code(provider_type)),

         # other services (HTS)
         given_ssnt           = case_when(
            service_ssnt_accept == 1 ~ "Accepted",
            service_ssnt_offer == 1 ~ "Offered",
         ),

         # combi prev (HTS)
         service_condoms      = if_else(service_condoms == 0, NA_integer_, as.integer(service_condoms), NA_integer_),
         service_lubes        = if_else(service_lubes == 0, NA_integer_, as.integer(service_lubes), NA_integer_),
      ) %>%
      arrange(central_id, desc(cd4_is_baseline), days_cd4_confirm, cd4_date) %>%
      distinct(central_id, .keep_all = TRUE)

   return(data)
}

##  Facilities & Address -------------------------------------------------------

convert_faci_addr <- function(data) {
   log_info("Converting address & facility data.")
   # rename columns
   data %<>%
      get_addr(
         c(
            region   = "perm_reg",
            province = "perm_prov",
            muncity  = "perm_munc"
         ),
         "nhsss"
      ) %>%
      get_addr(
         c(
            region_c   = "curr_reg",
            province_c = "curr_prov",
            muncity_c  = "curr_munc"
         ),
         "nhsss"
      ) %>%
      get_addr(
         c(
            region01   = "birth_reg",
            province01 = "birth_prov",
            placefbir  = "birth_munc"
         ),
         "nhsss"
      ) %>%
      get_addr(
         c(
            region_p   = "permonly_reg",
            province_p = "permonly_prov",
            muncity_p  = "permonly_munc"
         ),
         "nhsss"
      ) %>%
      get_addr(
         c(
            venue_region   = "hiv_service_reg",
            venue_province = "hiv_service_prov",
            venue_muncity  = "hiv_service_munc"
         ),
         "nhsss"
      ) %>%
      # country names
      left_join(
         y  = ohasis$ref_country %>%
            select(country_code, ocw_country = country_name),
         by = join_by(ofw_country == country_code)
      ) %>%
      relocate(ocw_country, .before = ofw_country) %>%
      # dxlab_standard
      mutate(
         use_specimen_source = case_when(
            is.na(test_faci) & !is.na(specimen_source) ~ TRUE,
            # test_faci == "990005" & str_left(specimen_refer_type, 1) == "2" ~ TRUE,
            TRUE ~ FALSE
         ),
         use_confirm_faci    = case_when(
            test_faci == "990005" & str_left(specimen_refer_type, 1) == "2" ~ TRUE,
            TRUE ~ FALSE
         ),
         test_faci           = coalesce(if_else(use_specimen_source, specimen_source, test_faci, test_faci), ""),
         test_sub_faci       = coalesce(if_else(use_specimen_source, specimen_sub_source, test_sub_faci, test_sub_faci), ""),
         test_faci           = coalesce(if_else(use_confirm_faci, confirm_faci, test_faci, test_faci), ""),
         test_sub_faci       = coalesce(if_else(use_confirm_faci, confirm_sub_faci, test_sub_faci, test_sub_faci), ""),
      ) %>%
      # left_join(
      #    na_matches = "never",
      #    y          = read_sheet("1aOqYjx5wbc403xy-64YHJU6NzhEBRUu6Ldg59yDEUMw", "Sheet1", range = "A:D", col_types = "c") %>%
      #       select(
      #          test_faci = harp_faci,
      #          pubpriv   = final_pubpriv
      #       ) %>%
      #       distinct(test_faci, .keep_all = TRUE) %>%
      #       mutate_all(~toupper(coalesce(., ""))),
      #    by         = join_by(test_faci)
      # ) %>%
      left_join(
         na_matches = "never",
         y          = ohasis$ref_faci %>%
            select(test_faci = faci_id, test_sub_faci = sub_faci_id, pubpriv = ownership) %>%
            mutate(
               test_sub_faci = coalesce(test_sub_faci, ""),
               pubpriv       = case_when(
                  pubpriv == 1 ~ "PUBLIC",
                  pubpriv == 2 ~ "PRIVATE",
               )
            ),
         by         = join_by(test_faci, test_sub_faci)
      ) %>%
      mutate(
         form_faci_2        = test_faci,
         form_faci          = test_faci,
         sub_form_faci      = test_sub_faci,
         diff_source_v_form = if_else(coalesce(form_faci, "") != coalesce(specimen_source, "") & (sample_source == "R" | is.na(sample_source)), 1, 0, 0)
      ) %>%
      ohasis$get_faci(
         list(hts_faci = c("form_faci", "sub_form_faci")),
         "name"
      ) %>%
      ohasis$get_faci(
         list(source_faci = c("specimen_source", "specimen_sub_source")),
         "name"
      ) %>%
      # confirmlab
      ohasis$get_faci(
         list(confirmlab = c("confirm_faci", "confirm_sub_faci")),
         "code",
         c("confirm_region", "confirm_province", "confirm_muncity")
      ) %>%
      ohasis$get_faci(
         list(dxlab_standard = c("test_faci", "test_sub_faci")),
         "nhsss",
         c("dx_region", "dx_province", "dx_muncity")
      ) %>%
      rename(
         form_faci = form_faci_2
      )

   return(data)
}

##  Finalize -------------------------------------------------------------------

final_conversion <- function(data) {
   data %<>%
      mutate(
         labcode2    = confirm_code,
         confirm_rec = rec_id,
      ) %>%
      # same vars as registry
      select(
         rec_id,
         central_id,
         patient_id,
         idnum,
         confirm_rec,
         hts_rec                   = hts_rec,
         form                      = form_version,
         modality                  = hts_modality,          # HTS Form
         consent_test              = test_agreed,           # HTS Form
         labcode                   = confirm_code,
         labcode2,
         year,
         month,
         uic                       = uic,
         firstname                 = first,
         middle                    = middle,
         last                      = last,
         name_suffix               = suffix,
         bdate                     = birthdate,
         patient_code              = patient_code,
         pxcode,
         age                       = age,
         age_months                = age_mo,
         sex                       = sex,
         philhealth                = philhealth_no,
         philsys_id                = philsys_id,
         mobile                    = client_mobile,
         email                     = client_email,
         muncity,
         province,
         region,
         muncity_c,
         province_c,
         region_c,
         muncity_p,
         province_p,
         region_p,
         ocw                       = is_ofw,
         motherisi1,
         pregnant                  = is_pregnant,
         tbpatient1                = med_tb_px,
         nationalit,
         civilstat                 = civil_status,
         self_identity,
         self_identity_other,
         gender_identity,
         nationality               = nationality,
         highest_educ              = educ_level,
         in_school                 = is_student,
         current_school_level,
         with_partner              = living_with_partner,
         child_count               = children,
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
         baseline_cd4_date         = cd4_date,
         baseline_cd4_result       = cd4_result,
         confirm_date,
         confirmlab,
         confirm_region,
         confirm_province,
         confirm_muncity,
         confirm_result            = confirm_result,
         confirm_remarks           = confirm_remarks,
         region01,
         province01,
         placefbir,
         curr_work,
         prev_work,
         ocw_based                 = ofw_station,
         ocw_country,
         age_sex                   = age_first_sex,
         age_inj                   = age_first_inject,
         howmanymse                = num_m_partner,
         yrlastmsex                = yr_last_m,
         howmanyfse                = num_f_partner,
         yrlastfsex                = yr_last_f,
         past12mo_injdrug          = expose_drug_inject,
         past12mo_rcvbt            = expose_blood_transfuse,
         past12mo_sti              = expose_sti,
         past12mo_sexfnocondom     = expose_sex_f_nocondom,
         past12mo_sexmnocondom     = expose_sex_m_nocondom,
         past12mo_sexprosti        = expose_sex_paying,
         past12mo_acceptpayforsex  = expose_sex_payment,
         past12mo_needle           = expose_occupation,
         past12mo_hadtattoo        = expose_tattoo,
         history_sex_m             = expose_sex_m,
         date_lastsex_m            = expose_sex_m_av_date,
         date_lastsex_condomless_m = expose_sex_m_av_nocondom_date,
         history_sex_f             = expose_sex_f,
         date_lastsex_f            = expose_sex_f_av_date,
         date_lastsex_condomless_f = expose_sex_f_av_nocondom_date,
         prevtest                  = prev_tested,
         prev_test_result          = prev_test_result,
         prev_test_faci            = prev_test_faci,
         prevtest_date             = prev_test_date,
         clinicalpicture           = clinical_pic,
         recombyph1                = test_reason_physician,
         recomby_peer_ed           = test_reason_peer_ed,   # HTS Form
         insurance1                = test_reason_insurance,
         recheckpr1                = test_reason_retest,
         no_test_reason            = test_reason_no_reason,
         possible_exposure         = test_reason_hiv_expose,
         emp_local                 = test_reason_employ_local,
         emp_abroad                = test_reason_employ_ofw,
         other_reason_test,
         description_symptoms,
         who_staging,
         hx_hepb                   = med_hep_b,
         hx_hepc                   = med_hep_c,
         hx_cbs                    = med_cbs_reactive,
         hx_prep                   = med_prep_px,
         hx_pep                    = med_pep_px,
         hx_sti                    = med_sti,
         reach_clinical            = reach_clinical,
         reach_online              = reach_online,
         reach_it                  = reach_index_testing,
         reach_ssnt                = reach_ssnt,
         reach_venue               = reach_venue,
         refer_art                 = refer_art,
         refer_confirm             = refer_confirm,
         retest                    = refer_retest,
         retest_in_mos             = retest_mos,
         retest_in_wks             = retest_wks,
         retest_date               = retest_date,
         given_hiv101              = service_hiv_101,
         given_iec_mats            = service_iec_mats,
         given_risk_reduce         = service_risk_counsel,
         given_prep_pep            = service_prep_refer,
         given_ssnt,
         provider_type             = provider_type,
         provider_type_other       = provider_type_other,
         venue_region,
         venue_province,
         venue_muncity,
         venue_text                = hiv_service_addr,
         px_type                   = client_type,
         referred_by               = refer_type,
         hts_date,
         t0_date                   = t0_date,
         t0_result                 = t0_result,
         test_done,
         name,
         t1_date                   = t1_date,
         t1_kit                    = t1_kit,
         t1_result                 = t1_result,
         t2_date                   = t2_date,
         t2_kit                    = t2_kit,
         t2_result                 = t2_result,
         t3_date                   = t3_date,
         t3_kit                    = t3_kit,
         t3_result                 = t3_result,
         final_interpretation      = confirm_result,
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
         source_faci,
         hts_faci,
         # dup_munc,
         form_faci
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
         confirm_date        = coalesce(confirm_date, as.Date(t3_date)),
         who_staging         = as.integer(who_staging),
         baseline_cd4_result = as.integer(baseline_cd4_result),
      ) %>%
      mutate_if(
         .predicate = is.labelled,
         ~to_character(.)
      ) %>%
      bind_rows(
         old %>%
            mutate_if(
               .predicate = is.labelled,
               ~to_character(.)
            )
      ) %>%
      arrange(idnum) %>%
      mutate(
         corr_defer = 0,
         corr_drop  = 0,
      ) %>%
      mutate(
         nodata_hiv_stage = if_else(
            is.na(ahd) &
               is.na(baseline_cd4) &
               ((coalesce(description_symptoms, "") == "" & clinicalpicture == 1) | is.na(clinicalpicture)) &
               is.na(tbpatient1) &
               is.na(who_staging) &
               class2022 == "HIV",
            1,
            0,
            0
         )
      )

   return(data)
}

##  Tag data to be reported later on and duplicates for dropping ---------------

tag_fordrop <- function(data, corr) {
   log_info("Tagging enrollees for dropping.")
   for (drop_var in c("corr_defer"))
      if (drop_var %in% names(corr)) {
         if (nrow(corr[[drop_var]]) > 0) {
            data %<>%
               left_join(
                  y  = corr[[drop_var]] %>%
                     distinct(rec_id) %>%
                     mutate(drop_this = 1),
                  by = join_by(rec_id)
               ) %>%
               mutate_at(
                  .vars = vars(matches(drop_var)),
                  ~coalesce(drop_this, .)
               ) %>%
               select(-drop_this)
         }
      }

   return(data)
}

##  Subsets for documentation --------------------------------------------------

subset_drops <- function(data) {
   log_info("Archive those for dropping.")
   drops <- list(
      dropped_notyet     = data %>% filter(corr_defer == 1),
      dropped_duplicates = data %>% filter(corr_drop == 1)
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
         drop        = corr_drop + corr_defer,
         who_staging = as.integer(who_staging),
         transmit    = if_else(
            year == params$yr &
               month == params$mo &
               transmit == "others",
            "unknown",
            transmit,
            transmit
         )
      ) %>%
      filter(drop == 0) %>%
      select(
         -drop,
         -corr_drop,
         -corr_defer,
         -mot,
         -form_faci,
         -any_of(
            c(
               "diff_source_v_form",
               "source_faci",
               "hts_faci",
               # "dup_munc",
               "age_pregnant",
               "age_vertical",
               "age_unknown",
               "confirm_faci",
               "test_faci",
               "cd4_confirm",
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
   log_info("New ahd      = {green(stri_pad_left(nrow_ahd, 4, ' '))}, {red(perc_ahd)}.")
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
         "rec_id",
         "hts_rec",
         "patient_id",
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
         "hts_faci",
         "source_faci",
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
         "form_faci"
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
         filter(confirmlab != "saccl")

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
         filter(confirmlab != "saccl") %>%
         mutate(
            keep = case_when(
               !str_detect(t1_kit, "Bioline") ~ 1,
               !str_detect(t1_result, "Reactive") ~ 1,
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
         filter(confirmlab != "saccl") %>%
         mutate(
            keep = case_when(
               !str_detect(t2_kit, "Determine") ~ 1,
               !str_detect(t2_result, "Reactive") ~ 1,
               if_any(c(t2_kit, t2_result, t2_date), ~is.na(.)) ~ 1,
               t2_date > t3_date ~ 1,
               t2_date > confirm_date ~ 1,
               t2_date < specimen_receipt_date ~ 1,
               t2_date < blood_extract_date ~ 1,
               t2_date < hts_date ~ 1,
               t2_date < t1_date ~ 1,
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
         filter(confirmlab != "saccl") %>%
         mutate(
            keep = case_when(
               !str_detect(t3_kit, "stat-pak") & !str_detect(t3_kit, "Geenius") ~ 1,
               !str_detect(t3_result, "Reactive") ~ 1,
               if_any(c(t3_kit, t3_result, t3_date), ~is.na(.)) ~ 1,
               t3_date > confirm_date ~ 1,
               t3_date < specimen_receipt_date ~ 1,
               t3_date < blood_extract_date ~ 1,
               t3_date < hts_date ~ 1,
               t3_date < t1_date ~ 1,
               t3_date < t2_date ~ 1,
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
         filter(transmit %in% c("others", "unknown")) %>%
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

      log_info("Checking for duplicate results w/o a reported positive result.")
      check[["dup_not_positive"]] <- data %>%
         filter(
            confirm_result == "5_Duplicate"
         ) %>%
         select(
            any_of(view_vars)
         )

      # pdf results
      log_info("Checking missing rHIVda pdf.")
      check[["no_pdf_result"]] <- data %>%
         filter(confirmlab != "saccl") %>%
         anti_join(
            y  = pdf_rhivda$data %>% select(confirm_code),
            by = join_by(labcode == confirm_code)
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for mtct.")
      check[["perinatal"]] <- data %>%
         filter(
            transmit == "perinatal"
         ) %>%
         select(
            any_of(view_vars),
         ) %>%
         mutate(
            age_in_months = interval(bdate, confirm_date) / months(1)
         )

      log_info("Checking for missing gender identity.")
      check[["no_gender_ident"]] <- data %>%
         filter(
            is.na(gender_identity)
         ) %>%
         select(
            any_of(view_vars),
         )

      all_issues <- combine_validations(data, check, "rec_id") %>%
         mutate(
            reg_order = confirm_region,
            reg_order = case_when(
               reg_order == "1" ~ 1,
               reg_order == "2" ~ 2,
               reg_order == "car" ~ 3,
               reg_order == "3" ~ 4,
               reg_order == "ncr" ~ 5,
               reg_order == "4a" ~ 6,
               reg_order == "4b" ~ 7,
               reg_order == "5" ~ 8,
               reg_order == "6" ~ 9,
               reg_order == "7" ~ 10,
               reg_order == "8" ~ 11,
               reg_order == "9" ~ 12,
               reg_order == "10" ~ 13,
               reg_order == "11" ~ 14,
               reg_order == "12" ~ 15,
               reg_order == "caraga" ~ 16,
               reg_order == "armm" ~ 17,
               reg_order == "barmm" ~ 17,
               TRUE ~ 9999
            ),
         ) %>%
         arrange(reg_order, confirmlab, labcode) %>%
         select(-reg_order)

      check <- list(all_issues = all_issues)

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
         for (drop in c("drop_notart", "corr_defer")) {
            if (drop %in% names(corr))
               for (check_var in names(check)) {
                  if (check_var != "tabstat")
                     check[[check_var]] %<>%
                        anti_join(
                           y  = corr[[drop]],
                           by = "rec_id"
                        )
               }
         }
      }
   }

   return(check)
}

##  Stata Labels ---------------------------------------------------------------

label_stata <- function(newdx, label_values, label_variables) {
   labels <- split(label_values, ~name)
   labels <- lapply(labels, function(data) {
      final_labels        <- as.integer(data[["value"]])
      names(final_labels) <- as.character(data[["label"]])
      return(final_labels)
   })

   for (i in seq_len(nrow(label_variables))) {
      var   <- label_variables[i,]$variable
      label <- label_variables[i,]$label_name

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
      dir     <- Sys.getenv("harp_dx")
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

            # compress_stata(files[[output]])
         }
      }

      flow_dta(official$new, "harp_dx", "reg", params$yr, params$mo)
   }
}

##  Actual flow ----------------------------------------------------------------

.init <- function(envir = parent.env(environment()), ...) {
   step <- parent.env(environment())
   p    <- envir
   vars <- as.list(list(...))

   data <- clean_data(p$forms)
   data <- prioritize_reports(data)
   data <- get_cd4(data, p$forms$cd4)
   data <- standardize_data(data, p$params)
   data <- tag_mot(data, p$params)
   data <- tag_class(data, p$corr)
   data <- convert_faci_addr(data)
   data <- final_conversion(data)
   data <- label_stata(data, p$corr$label_values, p$corr$label_variables)

   new_reg <- append_data(p$official$old, data)
   new_reg <- tag_fordrop(new_reg, p$corr)
   drops   <- subset_drops(new_reg)
   new_reg <- remove_drops(new_reg, p$params)
   # new_reg <- label_stata(new_reg, p$corr$label_values, p$corr$label_variables)

   step$check <- get_checks(data, p$pdf_rhivda, p$corr, run_checks = vars$run_checks, exclude_drops = vars$exclude_drops)
   step$data  <- data

   p$official$new <- new_reg
   append(p$official, drops)
   output_dta(p$official, p$params, vars$save)

   flow_validation(p, "hts_tst_pos", p$params$ym, upload = vars$upload)
   log_success("Done.")
}