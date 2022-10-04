##  Generate subset variables --------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
.log_info("Generating `prep`.`converted`.")

prepcutoff_mo   <- if_else(as.numeric(ohasis$next_mo) <= 3, as.numeric(ohasis$next_mo) + 9, as.numeric(ohasis$next_mo) - 3)
prepcutoff_mo   <- stri_pad_left(as.character(prepcutoff_mo), 2, '0')
prepcutoff_yr   <- if_else(as.numeric(ohasis$next_mo) <= 3, as.numeric(ohasis$next_yr) - 1, as.numeric(ohasis$next_yr))
prepcutoff_yr   <- as.character(prepcutoff_yr)
prepcutoff_date <- as.Date(paste(sep = '-', prepcutoff_yr, prepcutoff_mo, '01'))

##  Get reference datasets -----------------------------------------------------


# get latest visits
lw_conn <- ohasis$conn("lw")

.log_info("Getting earliest visits from OHASIS.")
oh_id_schema <- dbplyr::in_schema("ohasis_warehouse", "id_registry")

link_forms <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_a")) %>%
   select(
      HTS_REC = REC_ID,
      starts_with("EXPOSE"),
      starts_with("RISK"),
      starts_with("NUM_M_PARTNER"),
      starts_with("NUM_F_PARTNER"),
   ) %>%
   union(
      tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_hts")) %>%
         select(
            HTS_REC = REC_ID,
            starts_with("EXPOSE"),
            starts_with("RISK"),
            starts_with("NUM_M_PARTNER"),
            starts_with("NUM_F_PARTNER"),
         )
   ) %>%
   union(
      tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_cfbs")) %>%
         select(
            HTS_REC = REC_ID,
            starts_with("EXPOSE"),
            starts_with("RISK"),
            starts_with("NUM_M_PARTNER"),
            starts_with("NUM_F_PARTNER"),
         ) %>%
         rename_at(
            .vars = vars(starts_with("RISK")),
            ~paste0("CFBS_", .)
         )
   ) %>%
   inner_join(
      y    = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "rec_link")) %>%
         select(REC_ID = DESTINATION_REC, HTS_REC = SOURCE_REC),
      by   = "HTS_REC",
      copy = TRUE
   ) %>%
   collect()

first_visits <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "prep_first")) %>%
   select(CENTRAL_ID, REC_ID, EARLIEST_VISIT = VISIT_DATE) %>%
   collect()

# get mortality data
.log_info("Getting latest mortality dataset.")
if (!("harp_dead" %in% names(nhsss)))
   nhsss$harp_dead$official$new <- ohasis$get_data("harp_dead", ohasis$yr, ohasis$mo) %>%
      read_dta() %>%
      # convert Stata string missing data to NAs
      mutate_if(
         .predicate = is.character,
         ~if_else(. == '', NA_character_, .)
      ) %>%
      select(-starts_with("CENTRAL_ID")) %>%
      left_join(
         y  = tbl(lw_conn, oh_id_schema) %>%
            select(CENTRAL_ID, PATIENT_ID) %>%
            collect(),
         by = "PATIENT_ID"
      ) %>%
      mutate(
         CENTRAL_ID       = if_else(
            condition = is.na(CENTRAL_ID),
            true      = PATIENT_ID,
            false     = CENTRAL_ID
         ),
         proxy_death_date = as.Date(ceiling_date(as.Date(paste(sep = '-', year, month, '01')), unit = 'month')) - 1,
         ref_death_date   = if_else(
            condition = is.na(date_of_death),
            true      = proxy_death_date,
            false     = date_of_death
         )
      )
dbDisconnect(lw_conn)

# updated outcomes
.log_info("Performing initial conversion.")
data <- nhsss$prep$outcome.initial$data %>%
   distinct(prep_id, .keep_all = TRUE) %>%
   # get forma
   left_join(
      y  = link_forms,
      by = "REC_ID"
   ) %>%
   # risk
   mutate_at(
      .vars = vars(starts_with("RISK_") & !contains("DATE")),
      ~if_else(
         condition = !is.na(.),
         true      = StrLeft(., 1),
         false     = NA_character_,
      ) %>% as.integer()
   ) %>%
   # ars
   mutate_at(
      .vars = vars(
         starts_with("ARS_SX_") &
            !contains("DATE") &
            !contains("TEXT")
      ),
      ~if_else(
         condition = !is.na(.),
         true      = StrLeft(., 1),
         false     = NA_character_,
      ) %>% as.integer()
   ) %>%
   # sti
   mutate_at(
      .vars = vars(
         starts_with("STI_SX_") &
            !contains("DATE") &
            !contains("TEXT")
      ),
      ~if_else(
         condition = !is.na(.),
         true      = StrLeft(., 1),
         false     = NA_character_,
      ) %>% as.integer()
   ) %>%
   # pre-init
   mutate_at(
      .vars = vars(
         starts_with("PRE_INIT") &
            !contains("DATE") &
            !contains("TEXT")
      ),
      ~if_else(
         condition = !is.na(.),
         true      = StrLeft(., 1),
         false     = NA_character_,
      ) %>% as.integer()
   ) %>%
   # expose
   mutate_at(
      .vars = vars(
         starts_with("EXPOSE") &
            !contains("DATE") &
            !contains("TEXT")
      ),
      ~if_else(
         condition = !is.na(.),
         true      = StrLeft(., 1),
         false     = NA_character_,
      ) %>% as.integer()
   ) %>%
   # keypop
   mutate_at(
      .vars = vars(
         starts_with("KP_") &
            !contains("DATE") &
            !contains("TEXT")
      ),
      ~if_else(
         condition = !is.na(.),
         true      = StrLeft(., 1),
         false     = NA_character_,
      ) %>% as.integer()
   ) %>%
   # others vars
   mutate_at(
      .vars = vars(
         PREP_STATUS,
         PREP_CONTINUED,
         PREP_PLAN,
         PREP_TYPE,
         PREP_TYPE_LAST_VISIT
      ),
      ~if_else(
         condition = !is.na(.),
         true      = StrLeft(., 1),
         false     = NA_character_,
      ) %>% as.integer()
   )

risk   <- data %>%
   select(
      starts_with("RISK") &
         !contains("DATE") &
         !contains("TEXT")
   )
expose <- data %>%
   select(
      starts_with("EXPOSE_") &
         !contains("DATE") &
         !contains("TEXT")
   )

sti_sx <- data %>%
   select(
      starts_with("STI_SX") &
         !contains("NONE") &
         !contains("TEXT")
   )

ars_sx <- data %>%
   select(
      starts_with("ARS_SX") &
         !contains("NONE") &
         !contains("TEXT")
   )

.log_info("Generating taggings.")
nhsss$prep$outcome.converted$data <- data %>%
   # prepare screening taggings
   mutate(
      # form a
      with_hts            = if_else(
         condition = !is.na(HTS_REC),
         true      = 1,
         false     = 0
      ),

      # risk
      risk_screen         = case_when(
         (rowSums(risk, na.rm = TRUE) * ifelse(rowSums(is.na(risk)) == ncol(risk), NA, 1)) > 0 ~ 1,
         (rowSums(expose, na.rm = TRUE) * ifelse(rowSums(is.na(expose)) == ncol(expose), NA, 1)) > 0 ~ 1,
         (rowSums(risk, na.rm = TRUE) * ifelse(rowSums(is.na(risk)) == ncol(risk), NA, 1)) == 0 ~ 0,
         (rowSums(expose, na.rm = TRUE) * ifelse(rowSums(is.na(expose)) == ncol(expose), NA, 1)) == 0 ~ 0,
         !is.na(WEEK_AVG_SEX) ~ 0,
         TRUE ~ 9999
      ),

      # sti reactivity
      lab_hep             = case_when(
         StrLeft(LAB_HBSAG_RESULT, 2) == "2_" ~ "nonreactive",
         StrLeft(LAB_HBSAG_RESULT, 2) != "2_" ~ "hepb",
         !is.na(LAB_HBSAG_DATE) & is.na(LAB_HBSAG_RESULT) ~ "pending",
         TRUE ~ "(no data)"
      ),
      lab_syph            = case_when(
         toupper(LAB_SYPH_TITER) %in% c("NONREACTIVE", "NON REACTIVE") ~ "nonreactive",
         StrLeft(LAB_SYPH_RESULT, 1) == "2" ~ "nonreactive",
         StrLeft(LAB_SYPH_RESULT, 1) != "2" ~ "syph",
         !is.na(LAB_SYPH_TITER) ~ "syph",
         !is.na(LAB_HBSAG_DATE) & is.na(LAB_HBSAG_RESULT) ~ "pending",
         TRUE ~ "(no data)"
      ),

      # sti screening
      sti_screen          = case_when(
         rowSums(sti_sx, na.rm = TRUE) > 0 ~ 1,
         STI_SX_NONE == 1 ~ 0,
         !(tolower(STI_DIAGNOSIS) %in% c("wala", "negative", "no sign and symptoms of sti", "no sign and symptoms of sti and ars", "uti")) ~ 1,
         (tolower(STI_DIAGNOSIS) %in% c("wala", "negative", "no sign and symptoms of sti", "no sign and symptoms of sti and ars", "uti")) ~ 0,
         TRUE ~ 9999
      ),
      sti_visit           = case_when(
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
      ars_screen          = case_when(
         rowSums(ars_sx, na.rm = TRUE) > 0 ~ 1,
         ARS_SX_NONE == 1 ~ 0,
         PRE_INIT_NO_ARS == 1 ~ 0,
         TRUE ~ 9999
      ),

      # final clinical screening
      clin_screen         = case_when(
         sti_screen != 9999 ~ 1,
         ars_screen != 9999 ~ 1,
         TRUE ~ 0
      ),

      dispensed           = case_when(
         !is.na(MEDICINE_SUMMARY) ~ 1,
         !is.na(PREP_STATUS) ~ as.numeric(PREP_STATUS),
         !is.na(PREP_CONTINUED) ~ as.numeric(PREP_CONTINUED),
         TRUE ~ 9999
      ),
      prep_nr             = case_when(
         PRE_INIT_HIV_NR == 1 ~ 1,
         !is.na(PREP_HIV_DATE) ~ 1,
         TRUE ~ 0
      ),
      prep_weight         = case_when(
         PRE_INIT_WEIGHT == 1 ~ 1,
         floor(as.numeric(WEIGHT)) >= 35 ~ 1,
         TRUE ~ 0
      ),
      prep_behave         = case_when(
         ELIGIBLE_BEHAVIOR == 1 ~ 1,
         PREP_REQUESTED == 1 ~ 1,
         TRUE ~ 0
      ),

      # eligibility
      eligible            = case_when(
         dispensed == 1 ~ 1,
         ELIGIBLE_PREP == 1 ~ 1,
         prep_nr == 1 &
            prep_weight == 1 &
            ars_screen == 0 &
            prep_behave == 1 ~ 1,
         TRUE ~ 0
      ),

      # prep_on
      prep_on             = case_when(
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

      # reinitiation
      prep_reinit_date    = if_else(
         condition = INITIATION_DATE > PREP_START_DATE,
         true      = INITIATION_DATE,
         false     = NA_Date_
      ),

      # plan and type, status
      prep_plan           = case_when(
         is.na(PREP_START_DATE) ~ NA_character_,
         PREP_PLAN == 1 ~ "free",
         PREP_PLAN == 2 ~ "paid",
         PREP_PLAN == 3 ~ "shared",
         is.na(PREP_PLAN) & !is.na(PREP_START_DATE) ~ "(no data)",
      ),
      prep_type           = case_when(
         is.na(PREP_START_DATE) ~ NA_character_,
         PREP_TYPE == 1 ~ "daily",
         PREP_TYPE == 2 ~ "event",
         is.na(PREP_TYPE) & PREP_TYPE_LAST_VISIT == 1 ~ "daily",
         is.na(PREP_TYPE) & PREP_TYPE_LAST_VISIT == 2 ~ "event",
         is.na(PREP_TYPE) & !is.na(PREP_START_DATE) ~ "(no data)",
      ),

      # shifting data
      shifted             = case_when(
         is.na(PREP_START_DATE) ~ as.numeric(NA),
         PREP_SHIFT == 1 ~ 1,
         PREP_TYPE_LAST_VISIT != PREP_TYPE ~ 1,
         TRUE ~ 0
      ),

      # fix date formats
      LATEST_NEXT_DATE    = as.Date(LATEST_NEXT_DATE),

      # self identity for the period, if available
      self_identity       = if_else(
         condition = !is.na(SELF_IDENT),
         true      = substr(stri_trans_toupper(SELF_IDENT), 3, stri_length(SELF_IDENT)),
         false     = NA_character_
      ),
      self_identity_other = toupper(SELF_IDENT_OTHER),
      self_identity       = case_when(
         self_identity_other == "N/A" ~ NA_character_,
         self_identity_other == "no answer" ~ NA_character_,
         self_identity == "OTHER" ~ "OTHERS",
         self_identity == "MAN" ~ "MALE",
         self_identity == "WOMAN" ~ "FEMALE",
         self_identity == "MALE" ~ "MALE",
         self_identity == "FEMALE" ~ "FEMALE",
         TRUE ~ self_identity
      ),
      self_identity_other = case_when(
         self_identity_other == "NO ANSWER" ~ NA_character_,
         self_identity_other == "N/A" ~ NA_character_,
         TRUE ~ self_identity_other
      ),
   ) %>%
   # kap
   mutate(
      # for mot
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
         RISK_DRUG_INJECT > 0 ~ 1,
         CFBS_RISK_DRUG_INJECT > 0 ~ 1,
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

      # final kp
      kp_msm     = case_when(
         sex == "MALE" & KP_MSM == 1 ~ 1,
         sex == "MALE" & RISK_CONDOMLESS_ANAL == 1 ~ 1,
         sex == "MALE" & CFBS_RISK_CONDOMLESS_ANAL == 1 ~ 1,
         sex == "MALE" & CFBS_RISK_M_SEX_ORAL_ANAL == 1 ~ 1,
         sex == "MALE" & sexwithm == 1 ~ 1,
         sex == "MALE" & NUM_M_PARTNER > 0 ~ 1,
         TRUE ~ 0
      ),
      kp_tgp     = case_when(
         KP_TG == 1 ~ 1,
         sex != self_identity ~ 1,
         TRUE ~ 0
      ),
      kp_tgw     = case_when(
         sex == "MALE" & KP_TG == 1 ~ 1,
         sex == "MALE" & sex != self_identity ~ 1,
         TRUE ~ 0
      ),
      kp_tgw     = case_when(
         sex == "MALE" & KP_TG == 1 ~ 1,
         sex == "MALE" & sex != self_identity ~ 1,
         TRUE ~ 0
      ),
      kp_sw      = case_when(
         KP_SW == 1 ~ 1,
         regularlya == 1 ~ 1,
         TRUE ~ 0
      ),
      kp_pwid    = case_when(
         KP_PWID == 1 ~ 1,
         injectdrug == 1 ~ 1,
         TRUE ~ 0
      )
   ) %>%
   # get mortality data
   left_join(
      y  = nhsss$harp_dead$official$new %>%
         select(
            CENTRAL_ID,
            mort_id,
            ref_death_date
         ),
      by = "CENTRAL_ID"
   ) %>%
   mutate(
      # status as of current report
      prep_status = case_when(
         !is.na(mort_id) ~ "dead",
         PREP_STATUS == 0 ~ "refused",
         PREP_CONTINUED == 0 ~ "discontinued",
         LATEST_NEXT_DATE < prepcutoff_date ~ "ltfu",
         PREP_START_DATE == VISIT_DATE & is.na(MEDICINE_SUMMARY) ~ "for initiation",
         !is.na(PREP_START_DATE) & is.na(MEDICINE_SUMMARY) ~ "no dispense",
         PREP_START_DATE == VISIT_DATE ~ "enrollment",
         !is.na(prep_reinit_date) & VISIT_DATE == prep_reinit_date ~ "reinitiation",
         LATEST_NEXT_DATE >= prepcutoff_date ~ "on prep",
         prep_on == "not screened" ~ "insufficient screening",
         is.na(PREP_START_DATE) & eligible == 0 ~ "ineligible",
      )
   ) %>%
   # get latest outcome data
   left_join(
      y  = nhsss$prep$official$old_outcome %>%
         select(
            prep_id,
            prev_prepstart = prepstart_date,
            prev_reinit    = prep_reinit_date,
            prev_prep_plan = prep_plan,
            prev_prep_type = prep_type,
            prev_prep_on   = prep_on,
            prev_status    = prep_status,
            prev_outcome   = outcome,
            prev_ffup      = latest_ffupdate,
            prev_pickup    = latest_nextpickup,
            prev_regimen   = latest_regimen,
            prev_faci      = faci,
            prev_branch    = branch,
            prev_age       = curr_age,
         ),
      by = "prep_id"
   ) %>%
   # # get ohasis earliest visits
   # left_join(
   #    y  = first_visits %>% select(CENTRAL_ID, EARLIEST_REC = REC_ID, EARLIEST_VISIT),
   #    by = "CENTRAL_ID"
   # ) %>%
   mutate(
      # tag if new data is to be used
      # use_db         = case_when(
      #    is.na(prev_outcome) &
      #       !is.na(MEDICINE_SUMMARY) &
      #       !is.na(LATEST_NEXT_DATE) ~ 1,
      #    LATEST_VISIT > prev_ffup &
      #       !is.na(MEDICINE_SUMMARY) &
      #       !is.na(LATEST_NEXT_DATE) ~ 1,
      #    LATEST_NEXT_DATE > prev_pickup &
      #       !is.na(MEDICINE_SUMMARY) &
      #       !is.na(LATEST_NEXT_DATE) ~ 1,
      #    TRUE ~ 0
      # ),
      use_db         = 1,

      # current age for class
      # curr_age       = case_when(
      #    !is.na(birthdate) & use_db == 1 ~ abs(as.numeric(difftime(LATEST_VISIT, birthdate, units = "days")) / 365.25),
      #    is.na(birthdate) & use_db == 1 & !is.na(age) ~ age + (year(LATEST_VISIT) - year(prepstart_date)),
      #    is.na(birthdate) & use_db == 1 & is.na(age) ~ prev_age + (year(LATEST_VISIT) - year(prev_ffup)),
      #    !is.na(birthdate) & use_db == 0 ~ abs(as.numeric(difftime(prev_ffup, birthdate, units = "days")) / 365.25),
      #    is.na(birthdate) & use_db == 0 & !is.na(age) ~ age + (year(prev_ffup) - year(prepstart_date)),
      #    is.na(birthdate) & use_db == 0 & is.na(age) ~ prev_age + (year(prev_ffup) - year(prepstart_date)),
      # ) %>% floor(),
      curr_age       = case_when(
         !is.na(birthdate) ~ abs(as.numeric(difftime(LATEST_VISIT, birthdate, units = "days")) / 365.25)
      ),
      curr_age       = floor(curr_age),

      # current outcome
      # curr_outcome   = case_when(
      #    use_db == 1 & LATEST_VISIT <= ref_death_date ~ "dead",
      #    use_db == 1 & LATEST_NEXT_DATE >= prepcutoff_date ~ "alive on arv",
      #    use_db == 1 & LATEST_NEXT_DATE < prepcutoff_date ~ "lost to follow up",
      #    use_db == 0 & prev_ffup <= ref_death_date ~ "dead",
      #    use_db == 0 & prev_pickup >= prepcutoff_date ~ "alive on arv",
      #    use_db == 0 & prev_pickup < prepcutoff_date ~ "lost to follow up",
      #    use_db == 0 ~ prev_outcome
      # ),
      curr_outcome   = case_when(
         prep_status == "on prep" ~ "1_on prep",
         prep_status == "enrollment" ~ "1_on prep",
         prep_status == "reinitiation" ~ "1_on prep",
         prep_status == "ltfu" ~ "2_ltfu",
         prep_status == "discontinued" ~ "3_discontinued",
         prep_status == "refused" ~ "4_refused",
         prep_status == "ineligible" ~ "0_ineligible",
         TRUE ~ "5_not on prep"
      ),

      # check for multi-month clients
      days_to_pickup = case_when(
         use_db == 1 ~ abs(as.numeric(difftime(LATEST_NEXT_DATE, LATEST_VISIT, units = "days"))),
         use_db == 0 ~ abs(as.numeric(difftime(prev_pickup, prev_ffup, units = "days"))),
      ),
      arv_worth      = case_when(
         days_to_pickup == 0 ~ '0_No ARVs',
         days_to_pickup > 0 & days_to_pickup < 90 ~ '1_<3 months worth of ARVs',
         days_to_pickup >= 90 &
            days_to_pickup < 180 ~ '2_3-5 months worth of ARVs',
         days_to_pickup >= 180 & days_to_pickup <= 365.25 ~ '3_6-12 months worth of ARVs',
         days_to_pickup > 365.25 ~ '4_More than 1 yr worth of ARVs',
         TRUE ~ '5_(no data)'
      ),
   )

##  Finalize -------------------------------------------------------------------

.log_info("Finalizing dataframe.")
nhsss$prep$outcome.converted$data %<>%
   arrange(prep_id) %>%
   rename_at(
      .vars = vars(
         lab_hep,
         lab_syph,
         sti_screen,
         sti_visit,
         ars_screen,
         clin_screen,
         eligible,
         prep_on,
         prep_plan,
         prep_type,
         shifted,
         prep_status
      ),
      ~paste0("curr_", .)
   ) %>%
   # same vars as registry
   select(
      REC_ID,
      HTS_REC,
      CENTRAL_ID,
      prep_id,
      mort_id,
      year,
      month,
      px_code        = PATIENT_CODE,
      uic            = UIC,
      first          = FIRST,
      middle         = MIDDLE,
      last           = LAST,
      suffix         = SUFFIX,
      birthdate      = BIRTHDATE,
      sex,
      philsys_id     = PHILSYS_ID,
      philhealth_no  = PHILHEALTH_NO,
      self_identity,
      self_identity_other,
      sexwithm,
      sexwithf,
      sexwithpro,
      regularlya,
      injectdrug,
      chemsex,
      kp_msm,
      kp_tgp,
      kp_tgw,
      kp_sw,
      kp_pwid,
      kp_pdl         = KP_PDL,
      kp_other       = KP_OTHER,
      # use_db,
      prepstart_date = PREP_START_DATE,
      prep_reinit_date,
      # oh_prepstart_rec    = EARLIEST_REC,
      # oh_prepstart         = EARLIEST_VISIT,
      # prev_faci,
      # prev_branch,
      # prev_sathub,
      # prev_transhub,
      # prev_realhub,
      # prev_realhub_branch,
      # prev_class,
      # prev_outcome,
      # prev_line,
      # prev_ffup,
      # prev_pickup,
      # prev_regimen,
      # prev_artreg,
      # prev_num_drugs,
      starts_with("prev_", ignore.case = FALSE),
      curr_faci      = PREP_FACI_CODE,
      curr_branch    = PREP_BRANCH,
      # curr_age,
      with_hts,
      starts_with("curr_", ignore.case = FALSE),
      # curr_outcome,
      # curr_line,
      curr_ffup      = LATEST_VISIT,
      curr_pickup    = LATEST_NEXT_DATE,
      curr_regimen   = MEDICINE_SUMMARY, ,
      # curr_artreg         = prep_reg1,
      # curr_num_drugs,
      prep_reg,
      prep_prov,
      prep_munc,
      days_to_pickup,
      arv_worth,
      ref_death_date,
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
   distinct_all()

##  Flag data for validation ---------------------------------------------------

update <- input(
   prompt  = "Run `converted` validations?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
update <- substr(toupper(update), 1, 1)

nhsss$prep$outcome.converted$check <- list()
if (update == "1") {
   # initialize checking layer

   # non-negotiable variables
   vars <- c(
      # "curr_age",
      "curr_faci",
      # "curr_class",
      "curr_outcome",
      # "curr_regimen",
      "curr_ffup"
      # "curr_pickup"
   )
   .log_info("Checking if non-negotiable variables are missing.")
   for (var in vars) {
      var                                       <- as.symbol(var)
      nhsss$prep$outcome.converted$check[[var]] <- nhsss$prep$outcome.converted$data %>%
         filter(
            is.na(!!var)
         ) %>%
         arrange(curr_faci)
   }

   # duplicate id variables
   vars <- c(
      "prep_id",
      "CENTRAL_ID"
   )
   .log_info("Checking if id variables are duplicated.")
   for (var in vars) {
      var                                                                     <- as.symbol(var)
      nhsss$prep$outcome.converted$check[[paste0("dup_", as.character(var))]] <- nhsss$prep$outcome.converted$data %>%
         filter(
            !is.na(!!var)
         ) %>%
         get_dupes(!!var) %>%
         arrange(curr_faci)
   }

   # unknown data
   vars <- c(
      "prep_reg",
      "prep_prov",
      "prep_munc"
   )
   .log_info("Checking if required variables have UNKNOWN data or unpaired NHSSS versions.")
   for (var in vars) {
      var                                       <- as.symbol(var)
      nhsss$prep$outcome.converted$check[[var]] <- nhsss$prep$outcome.converted$data %>%
         filter(
            !!var %in% c("UNKNOWN", "OTHERS", NA_character_)
         ) %>%
         arrange(curr_faci)
   }

   # special checks
   .log_info("Checking for extreme dispensing.")
   nhsss$prep$outcome.converted$check[["mmd"]] <- nhsss$prep$outcome.converted$data %>%
      mutate(
         months_to_pickup = floor(days_to_pickup / 30)
      ) %>%
      filter(
         months_to_pickup >= 7
      ) %>%
      arrange(curr_faci)

   .log_info("Checking for no dispensing.")
   nhsss$prep$outcome.converted$check[["no_disp"]] <- nhsss$prep$outcome.converted$data %>%
      filter(
         curr_prep_status == "no dispense"
      ) %>%
      arrange(curr_faci)

   .log_info("Checking for no prep plan.")
   nhsss$prep$outcome.converted$check[["no_plan"]] <- nhsss$prep$outcome.converted$data %>%
      filter(
         !(StrLeft(curr_outcome, 1) %in% c("0", "3")),
         curr_prep_plan == "(no data)"
      ) %>%
      arrange(curr_faci)

   .log_info("Checking for no prep type.")
   nhsss$prep$outcome.converted$check[["no_type"]] <- nhsss$prep$outcome.converted$data %>%
      filter(
         !(StrLeft(curr_outcome, 1) %in% c("0", "3")),
         curr_prep_type == "(no data)"
      ) %>%
      arrange(curr_faci)

   .log_info("Checking for supposedly not on prep but with plan/type.")
   nhsss$prep$outcome.converted$check[["not_but_on_prep"]] <- nhsss$prep$outcome.converted$data %>%
      filter(
         StrLeft(curr_outcome, 1) == "5",
         curr_prep_type != "(no data)" | curr_prep_plan != "(no data)"
      ) %>%
      arrange(curr_faci)

   .log_info("Checking for possible ART.")
   nhsss$prep$outcome.converted$check[["updated_outcome"]] <- nhsss$prep$outcome.converted$data %>%
      filter(
         prev_outcome != curr_outcome
      ) %>%
      arrange(curr_faci)

   .log_info("Checking for possible ART.")
   nhsss$prep$outcome.converted$check[["possible_art"]] <- nhsss$prep$outcome.converted$data %>%
      filter(
         !stri_detect_fixed(curr_regimen, "TDF/FTC")
      ) %>%
      arrange(curr_faci)

   .log_info("Checking for extreme dispensing.")
   nhsss$prep$outcome.converted$check[["mismatch_faci"]] <- nhsss$prep$outcome.converted$data %>%
      filter(
         prev_ffup == curr_ffup,
         curr_faci != prev_faci
      ) %>%
      arrange(curr_faci)

   # .log_info("Checking for resurrected clients.")
   # nhsss$prep$outcome.converted$check[["resurrect"]] <- nhsss$prep$outcome.converted$data %>%
   #    filter(
   #       prev_outcome == "dead" & (curr_outcome != "dead" | is.na(curr_outcome)) | use_db == 1 & prev_outcome == "daed"
   #    ) %>%
   #    arrange(curr_faci)

   # .log_info("Checking for mismatch prepstart dates.")
   # nhsss$prep$outcome.converted$check[["oh_earlier_start"]] <- nhsss$prep$outcome.converted$data %>%
   #    filter(
   #       prepstart_date > oh_prepstart
   #    ) %>%
   #    arrange(curr_faci)

   # .log_info("Checking new mortalities.")
   # nhsss$prep$outcome.converted$check[["new_mort"]] <- nhsss$prep$outcome.converted$data %>%
   #    filter(
   #       curr_outcome == "dead" & (prev_outcome != "dead" | is.na(prev_outcome))
   #    ) %>%
   #    arrange(curr_faci)

   # range-median
   vars <- c(
      # "curr_age",
      "birthdate",
      # "prepstart_date",
      # "oh_prepstart",
      # "prev_ffup",
      # "prev_pickup",
      "curr_ffup",
      "curr_pickup",
      # "days_to_pickup",
      "ref_death_date"
      # "prev_num_drugs",
      # "curr_num_drugs"
   )
   .log_info("Checking range-median of data.")
   nhsss$prep$outcome.converted$check$tabstat <- data.frame()
   for (var in vars) {
      var <- as.symbol(var)
      df  <- nhsss$prep$outcome.converted$data

      nhsss$prep$outcome.converted$check$tabstat <- df %>%
         summarise(
            VARIABLE = as.character(var),
            MIN      = suppress_warnings(min(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            MEDIAN   = suppress_warnings(median(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            MAX      = suppress_warnings(max(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            NAs      = sum(if_else(is.na(!!var), 1, 0, 0))
         ) %>%
         mutate_all(~as.character(.)) %>%
         bind_rows(nhsss$prep$outcome.converted$check$tabstat)
   }
}

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
gdrive_validation(nhsss$prep, "outcome.converted", ohasis$ym)

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
