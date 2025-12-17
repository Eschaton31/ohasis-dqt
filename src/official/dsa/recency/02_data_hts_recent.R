##  Initial Cleaning -----------------------------------------------------------

clean_data <- function(forms, harp) {
   local_gs4_quiet()
   log_info("Processing recent tests.")
   trace_validation <- read_sheet("1wmCemVcAtp5nUEpti1qacU7nFQqi8GE0_qT7PE0WtC0", .name_repair = "unique_quiet") %>%
      rename_all(tolower)

   data <- forms$hiv_recency %>%
      left_join(
         y  = process_hts(forms$form_hts, forms$form_a, forms$form_cfbs) %>%
            select(
               -any_of(c(
                  "record_date",
                  "confirm_faci",
                  "confirm_sub_faci",
                  "specimen_source",
                  "specimen_sub_source",
                  "confirm_code",
                  "date_collect",
                  "date_receive",
                  "date_confirm"
               ))
            ),
         by = join_by(rec_id)
      ) %>%
      distinct(rec_id, .keep_all = TRUE) %>%
      get_cid(forms$id_reg, patient_id)

   hts_risk <- data %>%
      select(
         rec_id,
         contains("risk", ignore.case = FALSE)
      ) %>%
      pivot_longer(
         cols = contains("risk", ignore.case = FALSE)
      ) %>%
      group_by(rec_id) %>%
      summarise(
         risks = stri_c(collapse = ", ", unique(sort(value)))
      )

   data %<>%
      mutate_at(
         .vars = vars(first, middle, last, suffix),
         ~coalesce(clean_pii(.), "")
      ) %>%
      mutate_at(
         .vars = vars(patient_code, uic, philhealth_no, philsys_id, client_mobile, client_email),
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
         rt_offer_date   = coalesce(date_collect, record_date),

         # name
         standard_first  = stri_trans_general(first, "latin-ascii"),
         name            = str_squish(stri_c(last, ", ", first, " ", middle, " ", suffix)),

         # Permanent
         perm_prov       = if_else(str_left(perm_reg, 2) == "99", "999900000", perm_prov, perm_prov),
         perm_munc       = if_else(str_left(perm_reg, 2) == "99", "999999000", perm_munc, perm_munc),
         use_curr        = if_else(
            condition = !is.na(curr_munc) & (is.na(perm_munc) | str_left(perm_munc, 2) == "99"),
            true      = 1,
            false     = 0
         ),
         perm_reg        = if_else(
            condition = use_curr == 1,
            true      = curr_reg,
            false     = perm_reg
         ),
         perm_prov       = if_else(
            condition = use_curr == 1,
            true      = curr_prov,
            false     = perm_prov
         ),
         perm_munc       = if_else(
            condition = use_curr == 1,
            true      = curr_munc,
            false     = perm_munc
         ),

         # Age
         age_dta         = calc_age(birthdate, rt_offer_date),
         age             = coalesce(age, age_mo / 12, age_dta),

         # form_version (HTS)
         form_version    = if_else(form_version == " (vNA)", NA_character_, form_version),

         # provider type (HTS)
         provider_type   = as.integer(keep_code(provider_type)),

         # other services (HTS)
         given_ssnt      = case_when(
            service_ssnt_accept == 1 ~ "Accepted",
            service_ssnt_offer == 1 ~ "Offered",
         ),

         # combi prev (HTS)
         service_condoms = if_else(service_condoms == 0, NA_integer_, as.integer(service_condoms), NA_integer_),
         service_lubes   = if_else(service_lubes == 0, NA_integer_, as.integer(service_lubes), NA_integer_),
      ) %>%
      mutate(
         form_encoded = if_else(!is.na(form_version), '1_Yes', "0_No", "0_No"),
         .after       = rt_result
      ) %>%
      mutate(
         cbs_venue      = toupper(str_squish(hiv_service_addr)),
         online_app     = case_when(
            grepl("GRINDR", cbs_venue) ~ "GRINDR",
            grepl("GRNDR", cbs_venue) ~ "GRINDR",
            grepl("GRINDER", cbs_venue) ~ "GRINDR",
            grepl("TWITTER", cbs_venue) ~ "TWITTER",
            grepl("FACEBOOK", cbs_venue) ~ "FACEBOOK",
            grepl("MESSENGER", cbs_venue) ~ "FACEBOOK",
            grepl("\\bFB\\b", cbs_venue) ~ "FACEBOOK",
            grepl("\\bGR\\b", cbs_venue) ~ "GRINDR",
         ),
         reach_online   = if_else(!is.na(online_app), "1_Yes", reach_online, reach_online),
         reach_clinical = if_else(
            condition = if_all(starts_with("reach_"), ~is.na(.)) & hts_modality == "fbt",
            true      = "1_Yes",
            false     = reach_clinical,
            missing   = reach_clinical
         ),
      ) %>%
      select(-matches("risks")) %>%
      left_join(hts_risk, join_by(rec_id)) %>%
      mutate(
         sexual_risk = case_when(
            str_detect(risk_sexwithm, "yes") & str_detect(risk_sexwithf, "yes") ~ "M+F",
            str_detect(risk_sexwithm, "yes") & !str_detect(risk_sexwithf, "yes") ~ "M",
            !str_detect(risk_sexwithm, "yes") & str_detect(risk_sexwithf, "yes") ~ "F",
         ),
         kap_unknown = if_else(coalesce(risks, "(no data)") == "(no data)", "(no data)", NA_character_),
         kap_msm     = if_else(sex == "MALE" & sexual_risk %in% c("M", "M+F"), "MSM", NA_character_),
         kap_heterom = if_else(sex == "MALE" & sexual_risk == "F", "Hetero Male", NA_character_),
         kap_heterof = if_else(sex == "FEMALE" & !is.na(sexual_risk), "Hetero Female", NA_character_),
         kap_pwid    = if_else(str_detect(risk_injectdrug, "yes"), "PWID", NA_character_),
         kap_pip     = if_else(str_detect(risk_paymentforsex, "yes"), "PIP", NA_character_),
         kap_pdl     = case_when(
            str_left(client_type, 1) == "7" ~ "PDL",
            str_left(client_type, 1) == "7" ~ "PDL",
         ),
      ) %>%
      rename(
         created                 = created_by,
         updated                 = updated_by,
         hts_provider_type       = provider_type,
         hts_provider_type_other = provider_type_other,
      ) %>%
      left_join(trace_validation, join_by(confirm_code)) %>%
      left_join(
         y  = harp$dx %>%
            select(
               idnum,
               central_id,
               harp_inclusion_date,
               harp_confirm_date = confirm_date,
               harp_confirm_code = labcode2,
            ),
         by = join_by(central_id)
      ) %>%
      left_join(harp$tx %>% select(central_id, art_start_date = artstart_date), join_by(central_id)) %>%
      left_join(harp$prep %>% select(central_id, prep_start_date = prepstart_date), join_by(central_id)) %>%
      relocate(harp_inclusion_date, .after = date_confirm) %>%
      relocate(rt_agreed_actual, rt_validation_remarks, .before = rt_agreed) %>%
      mutate(
         rt_agreed       = case_when(
            rt_agreed_actual == "Y" ~ "1_Yes",
            rt_agreed_actual == "N" ~ "0_No",
            TRUE ~ rt_agreed
         ),
         rt_included     = case_when(
            confirm_code != harp_confirm_code & year(harp_confirm_date) < year(date_confirm) ~ 0,
            interval(art_start_date, hts_date) / days(1) > 28 ~ 0,
            age < 15 ~ 0,
            !is.na(rt_agreed) ~ 1,
            !is.na(rt_result) ~ 1,
            age >= 15 ~ 1,
            TRUE ~ 1
         ),
         tat_test_enroll = interval(rt_offer_date, art_start_date) / days(1),
         tat_test_enroll = case_when(
            tat_test_enroll < 0 ~ "0) Tx before dx",
            tat_test_enroll == 0 ~ "1) Same day",
            tat_test_enroll >= 1 & tat_test_enroll <= 14 ~ "2) rai (w/in 14 days)",
            tat_test_enroll >= 15 & tat_test_enroll <= 30 ~ "3) W/in 30days",
            tat_test_enroll >= 31 ~ "4) More than 30 days",
            is.na(harp_confirm_date) & !is.na(idnum) ~ "5) More than 30 days",
            is.na(art_start_date) ~ "(not yet enrolled)",
            TRUE ~ "(not yet confirmed)",
         )
      )

   return(data)
}

##  Sorting confirmatory results -----------------------------------------------

prioritize_reports <- function(data) {
   log_info("Add inclusion criteria.")
   data %<>%
      filter(rt_included == 1) %>%
      arrange(rt_result, rt_agreed, confirm_result, record_date, date_confirm) %>%
      distinct(central_id, .keep_all = TRUE)

   return(data)
}

##  Adding CD4 results ---------------------------------------------------------

get_cd4 <- function(data, forms) {
   log_info("Attaching baseline cd4.")
   lab_cd4 <- forms$lab_cd4 %>%
      mutate(
         cd4_result = str_replace_all(cd4_result, "[:alpha:]", ""),
         cd4_result = str_replace_all(cd4_result, " ", ""),
         cd4_result = str_replace_all(cd4_result, "<", ""),
         cd4_result = suppress_warnings(as.numeric(cd4_result), "NAs introduced")
      ) %>%
      get_cid(forms$id_reg, patient_id)

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
         cd4_confirm  = interval(cd4_date, rt_offer_date) / days(1),

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

standardize_data <- function(initial) {
   log_info("Converting to final harp variables.")
   data <- initial %>%
      mutate(
         # tagging vars
         male                   = if_else(
            condition = str_left(sex, 1) == "1",
            true      = 1,
            false     = 0
         ),
         female                 = if_else(
            condition = str_left(sex, 1) == "2",
            true      = 1,
            false     = 0
         ),

         # demographics
         sex                    = remove_code(stri_trans_toupper(sex)),
         self_ident             = remove_code(stri_trans_toupper(self_ident)),
         self_ident             = case_when(
            self_ident == "OTHER" ~ "OTHERS",
            self_ident == "MAN" ~ "MALE",
            self_ident == "WOMAN" ~ "FEMALE",
            self_ident == "MALE" ~ "MALE",
            self_ident == "FEMALE" ~ "FEMALE",
            TRUE ~ self_ident
         ),
         self_ident_other       = stri_trans_toupper(self_ident_other),
         self_ident_other_sieve = str_replace_all(self_ident_other, "[^[:alnum:]]", ""),

         civil_status           = stri_trans_toupper(civil_status),

         # clinical pic
         who_class              = as.integer(keep_code(who_class)),

         clinical_pic           = case_when(
            str_left(clinical_pic, 1) == "1" ~ "0_Asymptomatic",
            str_left(clinical_pic, 1) == "2" ~ "1_Symptomatic",
         ),

         ofw_station            = case_when(
            str_left(ofw_station, 1) == "1" ~ "1_On ship",
            str_left(ofw_station, 1) == "2" ~ "2_Land",
         ),

         refer_type             = case_when(
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
         .vars = vars(starts_with("service_") & !ends_with("faci")),
         ~if_else(
            condition = !is.na(.),
            true      = str_left(., 1),
            false     = NA_character_
         ) %>% as.integer()
      ) %>%
      generate_gender_identity(sex, self_ident, self_ident_other, gender_identity) %>%
      process_vl("rt_vl_result", "rt_vl_result_clean") %>%
      relocate(rt_vl_result_clean, .after = rt_vl_result) %>%
      mutate_at(
         .vars = vars(specimen_source, service_faci, confirm_faci, specimen_sub_source, service_sub_faci, confirm_sub_faci),
         ~na_if(as.character(.), "0")
      ) %>%
      mutate(
         rita_result = case_when(
            rt_vl_result_clean >= 1000 ~ "1_Recent",
            rt_vl_result_clean < 1000 ~ "2_Long-term",
         ),
         rt_faci     = coalesce(specimen_source, service_faci, confirm_faci),
         rt_sub_faci = coalesce(specimen_sub_source, service_sub_faci, confirm_sub_faci),
         .after      = rt_vl_result_clean,
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
   log_info("Tagging ahd.")
   class_corr <- read_sheet("1gNLaYTULQij4lzPyiadm_tofgtXZxcwJ5BDVZSyH7Rc")

   data %<>%
      mutate(
         # who Case Definition of advanced HIV classification
         # refined ahd
         baseline_cd4         = case_when(
            cd4_result >= 500 ~ 1,
            cd4_result >= 350 & cd4_result < 500 ~ 2,
            cd4_result >= 200 & cd4_result < 350 ~ 3,
            cd4_result >= 50 & cd4_result < 200 ~ 4,
            cd4_result < 50 ~ 5,
         ),
         ahd                  = case_when(
            who_class %in% c(3, 4) ~ 1,
            age >= 5 & baseline_cd4 %in% c(4, 5) ~ 1,
            age < 5 ~ 1,
            !is.na(baseline_cd4) ~ 0
         ),
         baseline_cd4         = labelled(
            baseline_cd4,
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
            condition = !is.na(who_class),
            true      = who_class,
            false     = NA_integer_
         ) %>% as.numeric(),
         description_symptoms = stri_trans_toupper(symptoms),
         med_tb_px            = case_when(
            stri_detect_fixed(description_symptoms, "TB") ~ 1,
            TRUE ~ as.numeric(med_tb_px)
         ),
         classd               = case_when(
            stri_detect_regex(description_symptoms, paste(collapse = "|", (class_corr %>% filter(as.numeric(class) == 3))$symptom)) ~ 3,
            med_tb_px == 1 ~ 3,
            TRUE ~ classd
         ),
         classd               = case_when(
            stri_detect_regex(description_symptoms, paste(collapse = "|", (class_corr %>% filter(as.numeric(class) == 4))$symptom)) ~ 4,
            TRUE ~ classd
         ),

         # new class for 2022
         hiv_stage            = case_when(
            classd %in% c(3, 4) ~ "AIDS",
            ahd == 1 ~ "AIDS",
            TRUE ~ "HIV"
         ),

         # no data for stAGE of hiv
         nodata_hiv_stage     = if_else(
            is.na(ahd) &
               is.na(baseline_cd4) &
               ((coalesce(description_symptoms, "") == "" & str_left(clinical_pic, 1) == "1") | is.na(clinical_pic)) &
               is.na(med_tb_px) &
               is.na(who_class) &
               hiv_stage == "HIV",
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
         rt_faci_id = rt_faci,
         .before    = rt_faci
      ) %>%
      ohasis$get_faci(
         list(hts_faci = c("service_faci", "service_sub_faci")),
         "name"
      ) %>%
      ohasis$get_faci(
         list(specimen_source_faci = c("specimen_source", "specimen_sub_source")),
         "name"
      ) %>%
      ohasis$get_faci(
         list(confirm_lab = c("confirm_faci", "confirm_sub_faci")),
         "name"
      ) %>%
      ohasis$get_faci(
         list(rt_lab = c("rt_faci", "rt_sub_faci")),
         "name",
         c("rt_reg", "rt_prov", "rt_munc")
      ) %>%
      get_addr(
         c(
            perm_reg  = "perm_reg",
            perm_prov = "perm_prov",
            perm_munc = "perm_munc"
         ),
         "name"
      ) %>%
      get_addr(
         c(
            curr_reg  = "curr_reg",
            curr_prov = "curr_prov",
            curr_munc = "curr_munc"
         ),
         "name"
      ) %>%
      get_addr(
         c(
            birth_reg  = "birth_reg",
            birth_prov = "birth_prov",
            birth_munc = "birth_munc"
         ),
         "name"
      ) %>%
      get_addr(
         c(
            cbs_reg  = "hiv_service_reg",
            cbs_prov = "hiv_service_prov",
            cbs_munc = "hiv_service_munc"
         ),
         "name"
      ) %>%
      # country names
      left_join(
         y  = ohasis$ref_country %>%
            select(country_code, ocw_country = country_name),
         by = join_by(ofw_country == country_code)
      ) %>%
      relocate(ocw_country, .before = ofw_country) %>%
      mutate_at(
         .vars = vars(provider_id, created, updated, signatory_1, signatory_2, signatory_3),
         ~as.character(.)
      ) %>%
      ohasis$get_staff(c(created_by = "created")) %>%
      ohasis$get_staff(c(updated_by = "updated")) %>%
      ohasis$get_staff(c(hts_provider = "provider_id")) %>%
      ohasis$get_staff(c(analyzed_by = "signatory_1")) %>%
      ohasis$get_staff(c(reviewed_by = "signatory_2")) %>%
      ohasis$get_staff(c(noted_by = "signatory_3"))

   return(data)
}

##  Finalize -------------------------------------------------------------------

final_conversion <- function(data) {
   data %<>%
      select(
         -any_of(
            c(
               "prime",
               "record_date",
               "disease",
               "hiv_service_type",
               "gender_affirm_therapy",
               "hiv_service_addr",
               "src",
               "module",
               "modality",
               "faci_id",
               "sub_faci_id",
               "confirmatory_code",
               "deleted_by",
               "deleted_at",
               "screen_agreed",
               "expose_sex_m_nocondom",
               "expose_sex_f_nocondom",
               "expose_sex_hiv",
               "age_first_sex",
               "num_f_partner",
               "yr_last_f",
               "num_m_partner",
               "yr_last_m",
               "age_first_inject",
               "med_cbs_reactive",
               "med_is_pregnant",
               "forma_msm",
               "forma_tgw",
               "forma_pwid",
               "forma_fsw",
               "forma_genpop",
               "screen_refer",
               "partner_referral_faci",
               "expose_sex_ever",
               "expose_condomless_anal",
               "expose_condomless_vaginal",
               "expose_m_sex_oral_anal",
               "expose_needle_share",
               "expose_illicit_drugs",
               "expose_sex_hiv_date",
               "expose_condomless_anal_date",
               "expose_condomless_vaginal_date",
               "expose_needle_share_date",
               "expose_illicit_drugs_date",
               "service_given_condoms",
               "service_given_lubes",
               "test_refuse_no_time",
               "test_refuse_other",
               "test_refuse_no_cure",
               "test_refuse_fear_result",
               "test_refuse_fear_disclose",
               "test_refuse_fear_msm",
               "cfbs_msm",
               "cfbs_tgw",
               "cfbs_pwid",
               "cfbs_fsw",
               "cfbs_genpop"
            )
         )
      ) %>%
      distinct_all() %>%
      mutate(
         kap_unknown = if_else(coalesce(risks, "(no data)") == "(no data)", "(no data)", NA_character_),
         kap_msm     = if_else(sex == "MALE" & sexual_risk %in% c("M", "M+F"), "MSM", NA_character_),
         kap_heterom = if_else(sex == "MALE" & sexual_risk == "F", "Hetero Male", NA_character_),
         kap_heterof = if_else(sex == "FEMALE" & !is.na(sexual_risk), "Hetero Female", NA_character_),
         kap_pip     = if_else(str_detect(risk_paymentforsex, "yes"), "PIP", NA_character_),
         kap_pdl     = case_when(
            str_left(client_type, 1) == "7" ~ "PDL",
            str_detect(rt_lab, "Jail") ~ "PDL",
            TRUE ~ NA_character_
         ),
      ) %>%
      unite(
         col   = "kap_type",
         sep   = "-",
         starts_with("kap_", ignore.case = FALSE),
         na.rm = TRUE
      ) %>%
      mutate(
         kap_type = if_else(kap_type == "", "No apparent risk", kap_type, kap_type)
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
            reg_order = rt_reg,
            reg_order = case_when(
               reg_order == "1" ~ 1,
               reg_order == "2" ~ 2,
               reg_order == "car" ~ 3,
               reg_order == "3" ~ 4,
               reg_order == "NCR" ~ 5,
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
         arrange(reg_order, rt_reg, rt_lab, confirm_code) %>%
         select(-reg_order)

      view_vars <- c(
         "rec_id",
         "patient_id",
         "rt_reg",
         "rt_lab",
         "rt_activation_date",
         "rt_offer_date",
         "rt_included",
         "rt_agreed",
         "rt_date",
         "rt_result",
         "rt_validation_remarks",
         "rt_validation_status",
         "rt_vl_requested",
         "rt_vl_date",
         "rt_vl_result",
         "rita_result",
         "harp_inclusion_date",
         "form_version",
         "confirm_code",
         "uic",
         "patient_code",
         "first",
         "middle",
         "last",
         "suffix",
         "birthdate",
         "sex",
         "self_ident",
         "self_ident_other",
         "gender_identity",
         "specimen_refer_type",
         "hts_faci",
         "source_faci",
         "hts_date",
         "hts_modality",
         "mot",
         "transmit",
         "sexhow"
      )
      check     <- check_pii(data, check, view_vars)

      # dates
      date_vars <- c(
         "rt_offer_date",
         "date_collect",
         "date_receive",
         "hts_date"
      )
      check     <- check_dates(data, check, view_vars, date_vars)

      # non-negotiable variables
      nonnegotiables <- c(
         "uic",
         "age",
         "confirm_lab",
         "form_version",
         "specimen_refer_type",
         "self_ident",
         "nationality",
         "rt_agreed",
         "rt_offer_date",
         "transmit"
      )
      check          <- check_unknown(data, check, "perm_addr", view_vars, perm_reg, perm_prov, perm_munc)
      check          <- check_unknown(data, check, "curr_addr", view_vars, curr_reg, curr_prov, curr_munc)
      check          <- check_nonnegotiables(data, check, view_vars, nonnegotiables)
      check          <- check_preggy(data, check, view_vars, sex = sex)
      check          <- check_age(data, check, view_vars, birthdate = birthdate, age = age, visit_date = rt_offer_date)

      # special checks
      log_info("Checking for not rt Result.")
      check[["no_rt_result"]] <- data %>%
         filter(
            rt_agreed == "1_Yes",
            is.na(rt_result),
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for no rita Result.")
      check[["no_rita_result"]] <- data %>%
         filter(
            rt_result == "1_Recent",
            is.na(rita_result)
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for no rita Result.")
      check[["ahd_recent"]] <- data %>%
         filter(
            rt_result == "1_Recent",
            hiv_stage == "AIDS"
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
         "rt_offer_date",
         "date_collect",
         "date_receive",
         "rt_offer_date",
         "birthdate",
         "age"
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
      dir     <- Sys.getenv("harp_dx")
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
               rt_faci_id         = faci_id,
               rt_activation_date = rt_activation_date
            ),
         by = join_by(rt_faci_id)
      )

   step$check <- get_checks(final_data, run_checks = vars$run_checks)
   step$data  <- data

   p$official$recency <- final_data
   # output_dta(p$official, p$params, vars$save)

   flow_validation(p, "hts_recent", p$params$ym, upload = vars$upload)
   log_success("Done.")
}