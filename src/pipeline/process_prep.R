# process prep data
process_prep <- function(form_prep = data.frame(), hts_data = data.frame(), rec_link = data.frame()) {
   if (!("hts_src" %in% names(form_prep)))
      form_prep %<>%
         mutate(
            hts_src = 0
         )

   data <- form_prep %>%
      mutate(
         # fix date formats
         latest_next_date = as.Date(latest_next_date),

         # make simplified tagging for source form
         src              = case_when(
            form_id == "prepScreen2020" ~ "screen2020",
            form_id == "prepFollowup2020" ~ "ffup2020",
            str_left(prep_visit, 1) == "1" ~ "screen2020",
            str_left(prep_visit, 1) == "2" ~ "ffup2020",
            TRUE ~ NA_character_
         )
      ) %>%
      # risk information
      mutate_at(
         .vars = vars(starts_with("risk_", ignore.case = FALSE) & !contains("date")),
         ~as.integer(keep_code(.))
      ) %>%
      mutate(
         # sex with female
         risk_sexwithf          = case_when(
            risk_condomless_vaginal == 4 ~ "yes-p01m",
            risk_condomless_vaginal == 3 ~ "yes-p06m",
            risk_condomless_vaginal == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),
         risk_sexwithf_nocdm    = risk_sexwithf,

         # sex with male
         risk_sexwithm          = case_when(
            risk_condomless_anal == 4 ~ "yes-p01m",
            risk_condomless_anal == 3 ~ "yes-p06m",
            risk_condomless_anal == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),
         risk_sexwithm_nocdm    = risk_sexwithf,

         # shared injects / injecting drugs
         risk_injectdrug        = case_when(
            risk_drug_inject == 4 ~ "yes-p01m",
            risk_drug_inject == 3 ~ "yes-p06m",
            risk_drug_inject == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),

         # had sex under influence of drugs
         risk_chemsex           = case_when(
            risk_drug_sex == 4 ~ "yes-p01m",
            risk_drug_sex == 3 ~ "yes-p06m",
            risk_drug_sex == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),

         # transactional sex
         risk_sextransaction    = case_when(
            risk_transact_sex == 4 ~ "yes-p01m",
            risk_transact_sex == 3 ~ "yes-p06m",
            risk_transact_sex == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),

         # sex w/ someone who has unknown HIV vl status
         risk_sexwithplhiv_novl = case_when(
            risk_hiv_vl_unknown == 4 ~ "yes-p01m",
            risk_hiv_vl_unknown == 3 ~ "yes-p06m",
            risk_hiv_vl_unknown == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),

         # sex w/ someone who has unknown HIV status
         risk_sexunknownhiv     = case_when(
            risk_hiv_unknown == 4 ~ "yes-p01m",
            risk_hiv_unknown == 3 ~ "yes-p06m",
            risk_hiv_unknown == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),

         # sex w/ someone who has HIV
         risk_sexunknownhiv     = case_when(
            risk_hiv_unknown == 4 ~ "yes-p01m",
            risk_hiv_unknown == 3 ~ "yes-p06m",
            risk_hiv_unknown == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),

         # has sti
         risk_sexunknownhiv     = case_when(
            risk_sti == 4 ~ "yes-p01m",
            risk_sti == 3 ~ "yes-p06m",
            risk_sti == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),

         # used pep
         risk_sexunknownhiv     = case_when(
            risk_pep == 4 ~ "yes-p01m",
            risk_pep == 3 ~ "yes-p06m",
            risk_pep == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),

         # sex events
         risk_avgsexweek        = case_when(
            str_left(week_avg_sex, 1) == "1" ~ "<= 1",
            str_left(week_avg_sex, 1) == "2" ~ ">= 2",
            TRUE ~ "(no data)"
         )
      ) %>%
      select(
         -starts_with("risk_")
      ) %>%
      left_join(
         y  = form_prep %>%
            select(
               rec_id,
               starts_with("risk_")
            ) %>%
            rename_at(
               .vars = vars(starts_with("risk", ignore.case = FALSE)),
               ~paste0("prep_", .)
            ),
         by = join_by(rec_id)
      ) %>%
      # get hts data
      left_join(
         y  = rec_link %>%
            select(
               rec_id  = destination_rec,
               hts_rec = source_rec
            ),
         by = "rec_id"
      ) %>%
      mutate(
         hts_rec = if_else(hts_src == 1, rec_id, hts_rec, hts_rec)
      ) %>%
      left_join(
         y  = hts_data %>%
            rename_at(
               .vars = vars(starts_with("risk", ignore.case = FALSE)),
               ~paste0("hts_", .)
            ) %>%
            select(
               hts_rec         = rec_id,
               hts_form        = form_version,
               hts_prep_client = med_prep_px,
               hts_prep_offer  = service_prep_refer,
               starts_with("hts", ignore.case = FALSE),
               starts_with("curr", ignore.case = FALSE),
               starts_with("perm", ignore.case = FALSE),
            ) %>%
            mutate(with_hts = 1),
         by = "hts_rec"
      ) %>%
      mutate(
         curr_reg  = coalesce(curr_reg.x, curr_reg.y),
         curr_prov = coalesce(curr_prov.x, curr_prov.y),
         curr_munc = coalesce(curr_munc.x, curr_munc.y),
      ) %>%
      select(
         -any_of(c(
            "curr_reg.x",
            "curr_reg.y",
            "curr_prov.x",
            "curr_prov.y",
            "curr_munc.x",
            "curr_munc.y"
         )),
      )

   # generate subsets for rowSums
   # concatenated any type of risk screening
   risk <- data %>%
      select(
         rec_id,
         contains("risk", ignore.case = FALSE)
      ) %>%
      pivot_longer(
         cols = contains("risk", ignore.case = FALSE)
      ) %>%
      group_by(rec_id) %>%
      summarise(
         risks = paste0(collapse = ", ", unique(sort(value)))
      ) %>%
      ungroup()

   # summarise as sum(); if > 0 has any type of sti symptom
   sti_sx <- data %>%
      select(
         rec_id,
         starts_with("sti_sx", ignore.case = FALSE) &
            !contains("none") &
            !contains("text")
      ) %>%
      mutate_at(
         .vars = vars(!matches("rec_id")),
         ~as.integer(keep_code(.))
      ) %>%
      pivot_longer(
         cols = !matches("rec_id")
      ) %>%
      group_by(rec_id) %>%
      summarise(
         sti_sx = sum(value, na.rm = TRUE)
      ) %>%
      ungroup()

   # summarise as sum(); if > 0 has any type of ars symptom
   ars_sx <- data %>%
      mutate_at(
         .vars = vars(starts_with("lab", ignore.case = FALSE) & contains("date")),
         ~as.Date(.)
      ) %>%
      select(
         rec_id,
         starts_with("ars_sx", ignore.case = FALSE) &
            !contains("none") &
            !contains("text")
      ) %>%
      mutate_at(
         .vars = vars(!matches("rec_id")),
         ~as.integer(keep_code(.))
      ) %>%
      pivot_longer(
         cols = !matches("rec_id")
      ) %>%
      group_by(rec_id) %>%
      summarise(
         ars_sx = sum(value, na.rm = TRUE)
      ) %>%
      ungroup()

   # prep info
   data %<>%
      left_join(y = risk, by = "rec_id") %>%
      left_join(y = sti_sx, by = "rec_id") %>%
      left_join(y = ars_sx, by = "rec_id") %>%
      mutate_at(
         .vars = vars(
            prep_status,
            prep_continued,
            prep_plan,
            prep_shift,
            prep_type,
            prep_requested,
            prep_type_last_visit,
            starts_with("pre_init", ignore.case = FALSE),
            starts_with("eligible", ignore.case = FALSE),
            starts_with("KP", ignore.case = FALSE) &
               !contains("date") &
               !contains("text"),
         ),
         ~as.integer(keep_code(.))
      ) %>%
      mutate(
         risk_screen = case_when(
            risks == "(no data)" ~ 0,
            is.na(risks) ~ 0,
            !is.na(risks) ~ 1,
            TRUE ~ 9999
         ),

         # sti reactivity
         lab_hep     = case_when(
            str_left(lab_hbsag_result, 2) == "2_" ~ "nonreactive",
            str_left(lab_hbsag_result, 2) != "2_" ~ "hepb",
            !is.na(lab_hbsag_date) & is.na(lab_hbsag_result) ~ "pending",
            TRUE ~ "(no data)"
         ),
         lab_syph    = case_when(
            toupper(lab_syph_titer) %in% c("nonreactive", "non reactive") ~ "nonreactive",
            str_left(lab_syph_result, 1) == "2" ~ "nonreactive",
            str_left(lab_syph_result, 1) != "2" ~ "syph",
            !is.na(lab_syph_titer) ~ "syph",
            !is.na(lab_syph_date) & is.na(lab_syph_result) ~ "pending",
            TRUE ~ "(no data)"
         ),

         # sti screening
         sti_screen  = case_when(
            sti_sx > 0 ~ 1,
            !(tolower(sti_diagnosis) %in% c("wala", "negative", "no sign and symptoms of sti", "no sign and symptoms of sti and ars", "uti")) ~ 1,
            (tolower(sti_diagnosis) %in% c("wala", "negative", "no sign and symptoms of sti", "no sign and symptoms of sti and ars", "uti")) ~ 0,
            sti_sx_none == 1 ~ 0,
            TRUE ~ 9999
         ),
         sti_visit   = case_when(
            lab_hep == "hepb" ~ 1,
            lab_syph == "syph" ~ 1,
            sti_screen == 1 ~ 1,
            lab_hep == "pending" ~ 7777,
            lab_syph == "pending" ~ 7777,
            lab_hep == "(no data)" ~ 9999,
            lab_syph == "(no data)" ~ 9999,
            sti_screen == 9999 ~ 9999,
            lab_hep == "nonreactive" ~ 0,
            lab_syph == "nonreactive" ~ 0,
            sti_screen == 0 ~ 0,
         ),

         # ars screening
         ars_screen  = case_when(
            ars_sx > 0 ~ 1,
            ars_sx_none == 1 ~ 0,
            pre_init_no_ars == 1 ~ 0,
            TRUE ~ 9999
         ),

         # final clinical screening
         clin_screen = case_when(
            sti_screen != 9999 ~ 1,
            ars_screen != 9999 ~ 1,
            TRUE ~ 0
         ),

         dispensed   = case_when(
            !is.na(medicine_summary) ~ as.integer(1),
            !is.na(prep_status) ~ prep_status,
            !is.na(prep_continued) ~ prep_continued,
            TRUE ~ as.integer(9999)
         ),
         prep_nr     = case_when(
            pre_init_hiv_nr == 1 ~ 1,
            !is.na(prep_hiv_date) ~ 1,
            TRUE ~ 0
         ),
         prep_weight = case_when(
            pre_init_weight == 1 ~ 1,
            floor(as.numeric(weight)) >= 35 ~ 1,
            TRUE ~ 0
         ),
         prep_behave = case_when(
            eligible_behavior == 1 ~ 1,
            prep_requested == 1 ~ 1,
            TRUE ~ 0
         ),

         # eligibility
         eligible    = case_when(
            dispensed == 1 ~ 1,
            eligible_prep == 1 ~ 1,
            prep_nr == 1 &
               prep_weight == 1 &
               ars_screen == 0 &
               prep_behave == 1 ~ 1,
            TRUE ~ 0
         ),
      ) %>%
      mutate(
         # prep_on
         prep_on   = case_when(
            prep_status == 0 ~ "refused",
            prep_continued == 0 ~ "discontinued",
            dispensed == 1 ~ "on prep",
            dispensed == 0 & eligible == 1 ~ "not on prep",
            eligible == 1 & dispensed == 9999 ~ "eligible",
            prep_record == "PrEP" ~ "screened",
            risk_screen != 9999 &
               (sti_screen != 9999 | ars_screen != 9999) ~ "screened",
            TRUE ~ "not screened"
         ),

         # plan and type, status
         prep_plan = case_when(
            prep_plan == 1 ~ "free",
            prep_plan == 2 ~ "paid",
            prep_plan == 3 ~ "shared",
            prep_on %in% c("discontinued", "refused", "screened") ~ "(not on prep)",
            TRUE ~ "(no data)"
         ),

         # type of prep
         prep_type = case_when(
            src == "screen2020" ~ prep_type,
            prep_type == prep_type_last_visit ~ prep_type,
            prep_type != prep_type_last_visit ~ prep_type,
            is.na(prep_type) &
               !is.na(prep_type_last_visit) &
               !is.na(medicine_summary) ~ prep_type_last_visit,
            !is.na(prep_type) & is.na(prep_type_last_visit) ~ prep_type,
         ),
         prep_type = case_when(
            prep_type == 1 ~ "daily",
            prep_type == 2 ~ "event",
            prep_on %in% c("discontinued", "refused", "screened") ~ "(not on prep)",
            TRUE ~ "(no data)"
         ),

         # shifting data
         shifted   = case_when(
            prep_on %in% c("discontinued", "refused") ~ "(not on prep)",
            prep_shift == 1 &
               prep_type == 1 &
               prep_type_last_visit == 1 ~ "yes (daily->daily)",
            prep_shift == 1 &
               prep_type == 2 &
               prep_type_last_visit == 2 ~ "yes (event->event)",
            prep_shift == 1 &
               prep_type == 1 &
               prep_type_last_visit == 2 ~ "yes (event->daily)",
            prep_shift == 1 &
               prep_type == 2 &
               prep_type_last_visit == 1 ~ "yes (daily->event)",
            prep_shift == 0 &
               prep_type == 1 &
               prep_type_last_visit == 2 ~ "no (event->daily)",
            prep_shift == 0 &
               prep_type == 2 &
               prep_type_last_visit == 1 ~ "no (daily->event)",
            is.na(prep_shift) &
               prep_type == 1 &
               prep_type_last_visit == 2 ~ "yes (event->daily)",
            is.na(prep_shift) &
               prep_type == 2 &
               prep_type_last_visit == 1 ~ "yes (daily->event)",
            prep_shift == 1 &
               is.na(prep_type) &
               prep_type_last_visit == 1 ~ "yes (daily->event)",
            prep_shift == 1 &
               is.na(prep_type) &
               prep_type_last_visit == 2 ~ "yes (event->daily)",
            prep_shift == 1 &
               is.na(prep_type_last_visit) &
               prep_type == 1 ~ "yes (daily->event)",
            prep_shift == 1 &
               is.na(prep_type_last_visit) &
               prep_type == 2 ~ "yes (event->daily)",
            prep_shift == 0 & prep_type == prep_type_last_visit ~ "no",
            prep_shift == 0 &
               !is.na(prep_type) &
               is.na(prep_type_last_visit) ~ "no",
            prep_shift == 0 &
               is.na(prep_type) &
               !is.na(prep_type_last_visit) ~ "no",
            is.na(prep_shift) & prep_type == prep_type_last_visit ~ "no",
            is.na(prep_shift) &
               !is.na(prep_type) &
               is.na(prep_type_last_visit) ~ "(no data)",
            is.na(prep_shift) &
               is.na(prep_type) &
               !is.na(prep_type_last_visit) ~ "(no data)",
            prep_shift == 0 &
               is.na(prep_type) &
               is.na(prep_type_last_visit) ~ "(no data)",
            is.na(prep_shift) &
               is.na(prep_type) &
               is.na(prep_type_last_visit) ~ "(no data)",
            prep_on == "screened" ~ "(not on prep)",
         ),
      )

   return(data)
}

convert_prep <- function(prep_data, convert_type = c("nhsss", "name", "code")) {
   data <- prep_data %>%
      mutate(
         use_record_faci = if_else(is.na(service_faci), 1, 0, 0),
         service_faci    = if_else(use_record_faci == 1, faci_id, service_faci),
      ) %>%
      rename(
         created = created_by,
         updated = updated_by,
      ) %>%
      select(
         -any_of(
            c(
               "prime",
               "disease",
               "hiv_service_type",
               "src",
               "module",
               "modality",
               "confirmatory_code",
               "use_curr"
            )
         )
      ) %>%
      ohasis$get_faci(
         list(report_faci = c("faci_id", "sub_faci_id")),
         convert_type
      ) %>%
      ohasis$get_faci(
         list(prep_faci = c("service_faci", "service_sub_faci")),
         convert_type,
         c("prep_reg", "prep_prov", "prep_munc")
      ) %>%
      ohasis$get_addr(
         c(
            curr_reg  = "curr_reg",
            curr_prov = "curr_prov",
            curr_munc = "curr_munc"
         ),
         convert_type
      ) %>%
      ohasis$get_staff(c(created_by = "created")) %>%
      ohasis$get_staff(c(updated_by = "updated")) %>%
      ohasis$get_staff(c(hts_provider = "service_by"))

   return(data)
}