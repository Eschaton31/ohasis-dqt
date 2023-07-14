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
         LATEST_NEXT_DATE = as.Date(LATEST_NEXT_DATE),

         # make simplified tagging for source form
         src              = case_when(
            FORM_VERSION == "PrEP Screening (v2020)" ~ "screen2020",
            FORM_VERSION == "PrEP Follow-up (v2020)" ~ "ffup2020",
            StrLeft(PREP_VISIT, 1) == "1" ~ "screen2020",
            StrLeft(PREP_VISIT, 1) == "2" ~ "ffup2020",
            TRUE ~ NA_character_
         )
      ) %>%
      # risk information
      mutate_at(
         .vars = vars(starts_with("RISK_", ignore.case = FALSE) & !contains("DATE")),
         ~as.integer(keep_code(.))
      ) %>%
      mutate(
         # sex with female
         risk_sexwithf          = case_when(
            RISK_CONDOMLESS_VAGINAL == 4 ~ "yes-p01m",
            RISK_CONDOMLESS_VAGINAL == 3 ~ "yes-p06m",
            RISK_CONDOMLESS_VAGINAL == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),
         risk_sexwithf_nocdm    = risk_sexwithf,

         # sex with male
         risk_sexwithm          = case_when(
            RISK_CONDOMLESS_ANAL == 4 ~ "yes-p01m",
            RISK_CONDOMLESS_ANAL == 3 ~ "yes-p06m",
            RISK_CONDOMLESS_ANAL == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),
         risk_sexwithm_nocdm    = risk_sexwithf,

         # shared injects / injecting drugs
         risk_injectdrug        = case_when(
            RISK_DRUG_INJECT == 4 ~ "yes-p01m",
            RISK_DRUG_INJECT == 3 ~ "yes-p06m",
            RISK_DRUG_INJECT == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),

         # had sex under influence of drugs
         risk_chemsex           = case_when(
            RISK_DRUG_SEX == 4 ~ "yes-p01m",
            RISK_DRUG_SEX == 3 ~ "yes-p06m",
            RISK_DRUG_SEX == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),

         # transactional sex
         risk_sextransaction    = case_when(
            RISK_TRANSACT_SEX == 4 ~ "yes-p01m",
            RISK_TRANSACT_SEX == 3 ~ "yes-p06m",
            RISK_TRANSACT_SEX == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),

         # sex w/ someone who has unknown HIV VL status
         risk_sexwithplhiv_novl = case_when(
            RISK_HIV_VL_UNKNOWN == 4 ~ "yes-p01m",
            RISK_HIV_VL_UNKNOWN == 3 ~ "yes-p06m",
            RISK_HIV_VL_UNKNOWN == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),

         # sex w/ someone who has unknown HIV status
         risk_sexunknownhiv     = case_when(
            RISK_HIV_UNKNOWN == 4 ~ "yes-p01m",
            RISK_HIV_UNKNOWN == 3 ~ "yes-p06m",
            RISK_HIV_UNKNOWN == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),

         # sex w/ someone who has HIV
         risk_sexunknownhiv     = case_when(
            RISK_HIV_UNKNOWN == 4 ~ "yes-p01m",
            RISK_HIV_UNKNOWN == 3 ~ "yes-p06m",
            RISK_HIV_UNKNOWN == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),

         # has sti
         risk_sexunknownhiv     = case_when(
            RISK_STI == 4 ~ "yes-p01m",
            RISK_STI == 3 ~ "yes-p06m",
            RISK_STI == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),

         # used pep
         risk_sexunknownhiv     = case_when(
            RISK_PEP == 4 ~ "yes-p01m",
            RISK_PEP == 3 ~ "yes-p06m",
            RISK_PEP == 2 ~ "yes-beyond_p12m",
            TRUE ~ "(no data)"
         ),

         # sex events
         risk_avgsexweek        = case_when(
            StrLeft(WEEK_AVG_SEX, 1) == "1" ~ "<= 1",
            StrLeft(WEEK_AVG_SEX, 1) == "2" ~ ">= 2",
            TRUE ~ "(no data)"
         )
      ) %>%
      rename_at(
         .vars = vars(starts_with("risk", ignore.case = FALSE)),
         ~paste0("prep_", .)
      ) %>%
      # get hts data
      left_join(
         y  = rec_link %>%
            select(
               REC_ID  = DESTINATION_REC,
               HTS_REC = SOURCE_REC
            ),
         by = "REC_ID"
      ) %>%
      mutate(
         HTS_REC = if_else(hts_src == 1, REC_ID, HTS_REC, HTS_REC)
      ) %>%
      left_join(
         y  = hts_data %>%
            rename_at(
               .vars = vars(starts_with("risk", ignore.case = FALSE)),
               ~paste0("hts_", .)
            ) %>%
            select(
               HTS_REC         = REC_ID,
               hts_form        = FORM_VERSION,
               hts_prep_client = MED_PREP_PX,
               hts_prep_offer  = SERVICE_PREP_REFER,
               starts_with("hts", ignore.case = FALSE),
               starts_with("CURR_PSGC", ignore.case = FALSE),
               starts_with("PERM_PSGC", ignore.case = FALSE),
            ) %>%
            mutate(with_hts = 1),
         by = "HTS_REC"
      )

   # generate subsets for rowSums
   # concatenated any type of risk screening
   risk <- data %>%
      select(
         REC_ID,
         contains("risk", ignore.case = FALSE)
      ) %>%
      pivot_longer(
         cols = contains("risk", ignore.case = FALSE)
      ) %>%
      group_by(REC_ID) %>%
      summarise(
         risks = paste0(collapse = ", ", unique(sort(value)))
      ) %>%
      ungroup()

   # summarise as sum(); if > 0 has any type of sti symptom
   sti_sx <- data %>%
      select(
         REC_ID,
         starts_with("STI_SX", ignore.case = FALSE) &
            !contains("NONE") &
            !contains("TEXT")
      ) %>%
      mutate_at(
         .vars = vars(!matches("REC_ID")),
         ~as.integer(keep_code(.))
      ) %>%
      pivot_longer(
         cols = !matches("REC_ID")
      ) %>%
      group_by(REC_ID) %>%
      summarise(
         sti_sx = sum(value, na.rm = TRUE)
      ) %>%
      ungroup()

   # summarise as sum(); if > 0 has any type of ars symptom
   ars_sx <- data %>%
      mutate_at(
         .vars = vars(starts_with("LAB", ignore.case = FALSE) & contains("DATE")),
         ~as.Date(.)
      ) %>%
      select(
         REC_ID,
         starts_with("ARS_SX", ignore.case = FALSE) &
            !contains("NONE") &
            !contains("TEXT")
      ) %>%
      mutate_at(
         .vars = vars(!matches("REC_ID")),
         ~as.integer(keep_code(.))
      ) %>%
      pivot_longer(
         cols = !matches("REC_ID")
      ) %>%
      group_by(REC_ID) %>%
      summarise(
         ars_sx = sum(value, na.rm = TRUE)
      ) %>%
      ungroup()

   # prep info
   data %<>%
      left_join(y = risk, by = "REC_ID") %>%
      left_join(y = sti_sx, by = "REC_ID") %>%
      left_join(y = ars_sx, by = "REC_ID") %>%
      mutate_at(
         .vars = vars(
            PREP_STATUS,
            PREP_CONTINUED,
            PREP_PLAN,
            PREP_SHIFT,
            PREP_TYPE,
            PREP_REQUESTED,
            PREP_TYPE_LAST_VISIT,
            starts_with("PRE_INIT", ignore.case = FALSE),
            starts_with("ELIGIBLE", ignore.case = FALSE),
            starts_with("KP", ignore.case = FALSE) &
               !contains("DATE") &
               !contains("TEXT"),
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
            StrLeft(LAB_HBSAG_RESULT, 2) == "2_" ~ "nonreactive",
            StrLeft(LAB_HBSAG_RESULT, 2) != "2_" ~ "hepb",
            !is.na(LAB_HBSAG_DATE) & is.na(LAB_HBSAG_RESULT) ~ "pending",
            TRUE ~ "(no data)"
         ),
         lab_syph    = case_when(
            toupper(LAB_SYPH_TITER) %in% c("NONREACTIVE", "NON REACTIVE") ~ "nonreactive",
            StrLeft(LAB_SYPH_RESULT, 1) == "2" ~ "nonreactive",
            StrLeft(LAB_SYPH_RESULT, 1) != "2" ~ "syph",
            !is.na(LAB_SYPH_TITER) ~ "syph",
            !is.na(LAB_SYPH_DATE) & is.na(LAB_SYPH_RESULT) ~ "pending",
            TRUE ~ "(no data)"
         ),

         # sti screening
         sti_screen  = case_when(
            sti_sx > 0 ~ 1,
            !(tolower(STI_DIAGNOSIS) %in% c("wala", "negative", "no sign and symptoms of sti", "no sign and symptoms of sti and ars", "uti")) ~ 1,
            (tolower(STI_DIAGNOSIS) %in% c("wala", "negative", "no sign and symptoms of sti", "no sign and symptoms of sti and ars", "uti")) ~ 0,
            STI_SX_NONE == 1 ~ 0,
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
            ARS_SX_NONE == 1 ~ 0,
            PRE_INIT_NO_ARS == 1 ~ 0,
            TRUE ~ 9999
         ),

         # final clinical screening
         clin_screen = case_when(
            sti_screen != 9999 ~ 1,
            ars_screen != 9999 ~ 1,
            TRUE ~ 0
         ),

         dispensed   = case_when(
            !is.na(MEDICINE_SUMMARY) ~ as.integer(1),
            !is.na(PREP_STATUS) ~ PREP_STATUS,
            !is.na(PREP_CONTINUED) ~ PREP_CONTINUED,
            TRUE ~ as.integer(9999)
         ),
         prep_nr     = case_when(
            PRE_INIT_HIV_NR == 1 ~ 1,
            !is.na(PREP_HIV_DATE) ~ 1,
            TRUE ~ 0
         ),
         prep_weight = case_when(
            PRE_INIT_WEIGHT == 1 ~ 1,
            floor(as.numeric(WEIGHT)) >= 35 ~ 1,
            TRUE ~ 0
         ),
         prep_behave = case_when(
            ELIGIBLE_BEHAVIOR == 1 ~ 1,
            PREP_REQUESTED == 1 ~ 1,
            TRUE ~ 0
         ),

         # eligibility
         eligible    = case_when(
            dispensed == 1 ~ 1,
            ELIGIBLE_PREP == 1 ~ 1,
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
            PREP_STATUS == 0 ~ "refused",
            PREP_CONTINUED == 0 ~ "discontinued",
            dispensed == 1 ~ "on prep",
            dispensed == 0 & eligible == 1 ~ "not on prep",
            eligible == 1 & dispensed == 9999 ~ "eligible",
            PREP_RECORD == "PrEP" ~ "screened",
            risk_screen != 9999 &
               (sti_screen != 9999 | ars_screen != 9999) ~ "screened",
            TRUE ~ "not screened"
         ),

         # plan and type, status
         prep_plan = case_when(
            PREP_PLAN == 1 ~ "free",
            PREP_PLAN == 2 ~ "paid",
            PREP_PLAN == 3 ~ "shared",
            prep_on %in% c("discontinued", "refused", "screened") ~ "(not on prep)",
            TRUE ~ "(no data)"
         ),

         # type of prep
         prep_type = case_when(
            src == "screen2020" ~ PREP_TYPE,
            PREP_TYPE == PREP_TYPE_LAST_VISIT ~ PREP_TYPE,
            PREP_TYPE != PREP_TYPE_LAST_VISIT ~ PREP_TYPE,
            is.na(PREP_TYPE) &
               !is.na(PREP_TYPE_LAST_VISIT) &
               !is.na(MEDICINE_SUMMARY) ~ PREP_TYPE_LAST_VISIT,
            !is.na(PREP_TYPE) & is.na(PREP_TYPE_LAST_VISIT) ~ PREP_TYPE,
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
            PREP_SHIFT == 1 &
               PREP_TYPE == 1 &
               PREP_TYPE_LAST_VISIT == 1 ~ "yes (daily->daily)",
            PREP_SHIFT == 1 &
               PREP_TYPE == 2 &
               PREP_TYPE_LAST_VISIT == 2 ~ "yes (event->event)",
            PREP_SHIFT == 1 &
               PREP_TYPE == 1 &
               PREP_TYPE_LAST_VISIT == 2 ~ "yes (event->daily)",
            PREP_SHIFT == 1 &
               PREP_TYPE == 2 &
               PREP_TYPE_LAST_VISIT == 1 ~ "yes (daily->event)",
            PREP_SHIFT == 0 &
               PREP_TYPE == 1 &
               PREP_TYPE_LAST_VISIT == 2 ~ "no (event->daily)",
            PREP_SHIFT == 0 &
               PREP_TYPE == 2 &
               PREP_TYPE_LAST_VISIT == 1 ~ "no (daily->event)",
            is.na(PREP_SHIFT) &
               PREP_TYPE == 1 &
               PREP_TYPE_LAST_VISIT == 2 ~ "yes (event->daily)",
            is.na(PREP_SHIFT) &
               PREP_TYPE == 2 &
               PREP_TYPE_LAST_VISIT == 1 ~ "yes (daily->event)",
            PREP_SHIFT == 1 &
               is.na(PREP_TYPE) &
               PREP_TYPE_LAST_VISIT == 1 ~ "yes (daily->event)",
            PREP_SHIFT == 1 &
               is.na(PREP_TYPE) &
               PREP_TYPE_LAST_VISIT == 2 ~ "yes (event->daily)",
            PREP_SHIFT == 1 &
               is.na(PREP_TYPE_LAST_VISIT) &
               PREP_TYPE == 1 ~ "yes (daily->event)",
            PREP_SHIFT == 1 &
               is.na(PREP_TYPE_LAST_VISIT) &
               PREP_TYPE == 2 ~ "yes (event->daily)",
            PREP_SHIFT == 0 & PREP_TYPE == PREP_TYPE_LAST_VISIT ~ "no",
            PREP_SHIFT == 0 &
               !is.na(PREP_TYPE) &
               is.na(PREP_TYPE_LAST_VISIT) ~ "no",
            PREP_SHIFT == 0 &
               is.na(PREP_TYPE) &
               !is.na(PREP_TYPE_LAST_VISIT) ~ "no",
            is.na(PREP_SHIFT) & PREP_TYPE == PREP_TYPE_LAST_VISIT ~ "no",
            is.na(PREP_SHIFT) &
               !is.na(PREP_TYPE) &
               is.na(PREP_TYPE_LAST_VISIT) ~ "(no data)",
            is.na(PREP_SHIFT) &
               is.na(PREP_TYPE) &
               !is.na(PREP_TYPE_LAST_VISIT) ~ "(no data)",
            PREP_SHIFT == 0 &
               is.na(PREP_TYPE) &
               is.na(PREP_TYPE_LAST_VISIT) ~ "(no data)",
            is.na(PREP_SHIFT) &
               is.na(PREP_TYPE) &
               is.na(PREP_TYPE_LAST_VISIT) ~ "(no data)",
            prep_on == "screened" ~ "(not on prep)",
         ),
      )

   return(data)
}