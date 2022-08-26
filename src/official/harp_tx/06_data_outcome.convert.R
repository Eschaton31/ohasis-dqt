##  Generate subset variables --------------------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
.log_info("Generating `harp_tx`.`converted`.")
artcutoff_mo   <- if_else(as.numeric(ohasis$next_mo) <= 3, as.numeric(ohasis$next_mo) + 9, as.numeric(ohasis$next_mo) - 3)
artcutoff_mo   <- stri_pad_left(as.character(artcutoff_mo), 2, '0')
artcutoff_yr   <- if_else(as.numeric(ohasis$next_mo) <= 3, as.numeric(ohasis$next_yr) - 1, as.numeric(ohasis$next_yr))
artcutoff_yr   <- as.character(artcutoff_yr)
artcutoff_date <- as.Date(paste(sep = '-', artcutoff_yr, artcutoff_mo, '01'))

##  Get reference datasets -----------------------------------------------------

# get mortality data
.log_info("Getting latest mortality dataset.")
nhsss$harp_dead$official$new <- ohasis$get_data("harp_dead", ohasis$yr, ohasis$mo) %>%
   read_dta(
      col_select = c(
         PATIENT_ID,
         mort_id,
         date_of_death,
         year,
         month
      )
   ) %>%
   # convert Stata string missing data to NAs
   mutate_if(
      .predicate = is.character,
      ~if_else(. == '', NA_character_, .)
   ) %>%
   left_join(
      y  = nhsss$harp_tx$forms$id_registry,
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

# updated outcomes
.log_info("Performing initial conversion.")
nhsss$harp_tx$outcome.converted$data <- nhsss$harp_tx$outcome.initial$data %>%
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
   # get latest outcome data
   left_join(
      y  = nhsss$harp_tx$official$old_outcome %>%
         mutate(hub = toupper(hub)) %>%
         select(
            art_id,
            prev_class          = class,
            prev_outcome        = outcome,
            prev_ffup           = latest_ffupdate,
            prev_pickup         = latest_nextpickup,
            prev_regimen        = latest_regimen,
            prev_line           = line,
            prev_artreg         = art_reg,
            prev_hub            = hub,
            prev_branch         = branch,
            prev_sathub         = sathub,
            prev_transhub       = transhub,
            prev_realhub        = realhub,
            prev_realhub_branch = realhub_branch,
            prev_age            = curr_age,
         ),
      by = "art_id"
   ) %>%
   # get ohasis earliest visits
   left_join(
      y  = nhsss$harp_tx$forms$art_first %>%
         select(CENTRAL_ID, EARLIEST_REC = REC_ID, EARLIEST_VISIT = VISIT_DATE) %>%
         arrange(EARLIEST_VISIT) %>%
         distinct(CENTRAL_ID, .keep_all = TRUE),
      by = "CENTRAL_ID"
   ) %>%
   mutate(
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

      # clinical pic
      who_staging         = StrLeft(WHO_CLASS, 1) %>% as.integer(),

      # tag if new data is to be used
      use_db              = case_when(
         is.na(prev_outcome) &
            !is.na(MEDICINE_SUMMARY) &
            !is.na(LATEST_NEXT_DATE) ~ 1,
         LATEST_VISIT > prev_ffup &
            !is.na(MEDICINE_SUMMARY) &
            !is.na(LATEST_NEXT_DATE) ~ 1,
         LATEST_NEXT_DATE > prev_pickup &
            !is.na(MEDICINE_SUMMARY) &
            !is.na(LATEST_NEXT_DATE) ~ 1,
         TRUE ~ 0
      ),

      # current age for class
      curr_age            = case_when(
         !is.na(birthdate) & use_db == 1 ~ abs(as.numeric(difftime(LATEST_VISIT, birthdate, units = "days")) / 365.25),
         is.na(birthdate) & use_db == 1 & !is.na(age) ~ age + (year(LATEST_VISIT) - year(artstart_date)),
         is.na(birthdate) & use_db == 1 & is.na(age) ~ prev_age + (year(LATEST_VISIT) - year(prev_ffup)),
         !is.na(birthdate) & use_db == 0 ~ abs(as.numeric(difftime(prev_ffup, birthdate, units = "days")) / 365.25),
         is.na(birthdate) & use_db == 0 & !is.na(age) ~ age + (year(prev_ffup) - year(artstart_date)),
         is.na(birthdate) & use_db == 0 & is.na(age) ~ prev_age + (year(prev_ffup) - year(artstart_date)),
      ) %>% floor(),
      curr_class          = case_when(
         curr_age >= 10 ~ "Adult Patient",
         curr_age < 10 ~ "Pediatric Patient",
      ),

      # current outcome
      curr_outcome        = case_when(
         use_db == 1 & LATEST_VISIT <= ref_death_date ~ "dead",
         use_db == 1 & LATEST_NEXT_DATE >= artcutoff_date ~ "alive on arv",
         use_db == 1 & LATEST_NEXT_DATE < artcutoff_date ~ "lost to follow up",
         use_db == 0 & prev_ffup <= ref_death_date ~ "dead",
         use_db == 0 & stri_detect_fixed(prev_outcome, "stopped") ~ prev_outcome,
         use_db == 0 & prev_pickup >= artcutoff_date ~ "alive on arv",
         use_db == 0 & prev_pickup < artcutoff_date ~ "lost to follow up",
         use_db == 0 ~ prev_outcome
      ),

      # count number of drugs in regimen
      prev_num_drugs      = if_else(
         condition = !is.na(prev_regimen),
         true      = stri_count_fixed(prev_regimen, "+") + 1,
         false     = 0
      ),
      curr_num_drugs      = if_else(
         condition = !is.na(MEDICINE_SUMMARY),
         true      = stri_count_fixed(MEDICINE_SUMMARY, "+") + 1,
         false     = 0
      ),

      # check for multi-month clients
      days_to_pickup      = case_when(
         use_db == 1 ~ abs(as.numeric(difftime(LATEST_NEXT_DATE, LATEST_VISIT, units = "days"))),
         use_db == 0 ~ abs(as.numeric(difftime(prev_pickup, prev_ffup, units = "days"))),
      ),
      arv_worth           = case_when(
         days_to_pickup == 0 ~ '0_No ARVs',
         days_to_pickup > 0 & days_to_pickup < 90 ~ '1_<3 months worth of ARVs',
         days_to_pickup >= 90 &
            days_to_pickup < 180 ~ '2_3-5 months worth of ARVs',
         days_to_pickup >= 180 & days_to_pickup <= 365.25 ~ '3_6-12 months worth of ARVs',
         days_to_pickup > 365.25 ~ '4_More than 1 yr worth of ARVs',
         TRUE ~ '5_(no data)'
      ),

      # regimen disagg
      arv_reg             = stri_trans_tolower(MEDICINE_SUMMARY),
      azt1                = if_else(stri_detect_fixed(arv_reg, "azt"), "azt", NA_character_),
      tdf1                = if_else(stri_detect_fixed(arv_reg, "tdf"), "tdf", NA_character_),
      d4t1                = if_else(stri_detect_fixed(arv_reg, "d4t"), "d4t", NA_character_),
      xtc1                = if_else(stri_detect_fixed(arv_reg, "3tc"), "3tc", NA_character_),
      efv1                = if_else(stri_detect_fixed(arv_reg, "efv"), "efv", NA_character_),
      nvp1                = if_else(stri_detect_fixed(arv_reg, "nvp"), "nvp", NA_character_),
      lpvr1               = if_else(stri_detect_fixed(arv_reg, "lpv/r"), "lpv/r", NA_character_),
      lpvr1               = if_else(stri_detect_fixed(arv_reg, "lpvr"), "lpvr", lpvr1),
      ind1                = if_else(stri_detect_fixed(arv_reg, "ind"), "ind", NA_character_),
      ral1                = if_else(stri_detect_fixed(arv_reg, "ral"), "ral", NA_character_),
      abc1                = if_else(stri_detect_fixed(arv_reg, "abc"), "abc", NA_character_),
      ril1                = if_else(stri_detect_fixed(arv_reg, "ril"), "ril", NA_character_),
      dtg1                = if_else(stri_detect_fixed(arv_reg, "dtg"), "dtg", NA_character_),
   )

##  Finalize -------------------------------------------------------------------

.log_info("Finalizing dataframe.")
nhsss$harp_tx$outcome.converted$data %<>%
   unite(
      col   = 'art_reg1',
      sep   = ' ',
      na.rm = T,
      azt1,
      tdf1,
      d4t1,
      xtc1,
      efv1,
      nvp1,
      lpvr1,
      ind1,
      ral1,
      abc1,
      ril1,
      dtg1
   ) %>%
   mutate(
      curr_line = case_when(
         art_reg1 == "tdf 3tc efv" ~ 1,
         art_reg1 == "tdf 3tc nvp" ~ 1,
         art_reg1 == "azt 3tc efv" ~ 1,
         art_reg1 == "azt 3tc nvp" ~ 1,
         art_reg1 == "d4t 3tc efv" ~ 1,
         art_reg1 == "d4t 3tc nvp" ~ 1,
         art_reg1 == "tdf 3tc dtg" ~ 1,
         art_reg1 == "3tc nvp abc" ~ 1,
         art_reg1 == "3tc efv abc" ~ 1,
         art_reg1 == "3tc abc ril" ~ 1,
         art_reg1 == "tdf 3tc ril" ~ 1,
         art_reg1 == "azt 3tc ril" ~ 1,
         art_reg1 == "azt 3tc lpv/r" ~ 2,
         art_reg1 == "tdf 3tc lpv/r" ~ 2,
         art_reg1 == "d4t 3tc lpv/r" ~ 2,
         art_reg1 == "d4t 3tc idv" ~ 2,
         art_reg1 == "azt 3tc idv" ~ 2,
         art_reg1 == "tdf 3tc idv" ~ 2,
         art_reg1 == "3tc lpv/r abc" ~ 2,
         !is.na(art_reg1) ~ 3
      ),
   ) %>%
   # same vars as registry
   select(
      REC_ID,
      CENTRAL_ID,
      art_id,
      idnum,
      mort_id,
      year,
      month,
      confirmatory_code,
      px_code,
      uic,
      first,
      middle,
      last,
      suffix,
      birthdate,
      sex,
      initials,
      philsys_id,
      philhealth_no,
      who_staging,
      use_db,
      artstart_date,
      oh_artstart_rec     = EARLIEST_REC,
      oh_artstart         = EARLIEST_VISIT,
      prev_hub,
      prev_branch,
      prev_sathub,
      prev_transhub,
      prev_realhub,
      prev_realhub_branch,
      prev_class,
      prev_outcome,
      prev_line,
      prev_ffup,
      prev_pickup,
      prev_regimen,
      prev_artreg,
      prev_num_drugs,
      curr_hub            = ART_FACI_CODE,
      curr_branch         = ART_BRANCH,
      curr_sathub         = SATELLITE_FACI_CODE,
      curr_transhub       = TRANSIENT_FACI_CODE,
      curr_age,
      curr_class,
      curr_outcome,
      curr_line,
      curr_ffup           = LATEST_VISIT,
      curr_pickup         = LATEST_NEXT_DATE,
      curr_regimen        = MEDICINE_SUMMARY, ,
      curr_artreg         = art_reg1,
      curr_num_drugs,
      tx_reg,
      tx_prov,
      tx_munc,
      curr_realhub        = ACTUAL_FACI_CODE,
      curr_realhub_branch = ACTUAL_BRANCH,
      real_reg,
      real_prov,
      real_munc,
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
   )

##  Flag data for validation ---------------------------------------------------

update <- input(
   prompt  = "Run `converted` validations?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
update <- substr(toupper(update), 1, 1)

nhsss$harp_tx$outcome.converted$check <- list()
if (update == "1") {
   # initialize checking layer

   # non-negotiable variables
   vars <- c(
      "curr_age",
      "curr_hub",
      "curr_class",
      "curr_outcome",
      "curr_regimen",
      "curr_ffup",
      "curr_pickup",
      "artstart_date",
      "oh_artstart",
      "curr_line"
   )
   .log_info("Checking if non-negotiable variables are missing.")
   for (var in vars) {
      var                                          <- as.symbol(var)
      nhsss$harp_tx$outcome.converted$check[[var]] <- nhsss$harp_tx$outcome.converted$data %>%
         filter(
            is.na(!!var)
         ) %>%
         arrange(curr_hub)
   }

   # duplicate id variables
   vars <- c(
      "art_id",
      "idnum",
      "CENTRAL_ID"
   )
   .log_info("Checking if id variables are duplicated.")
   for (var in vars) {
      var                                                                        <- as.symbol(var)
      nhsss$harp_tx$outcome.converted$check[[paste0("dup_", as.character(var))]] <- nhsss$harp_tx$outcome.converted$data %>%
         filter(
            !is.na(!!var)
         ) %>%
         get_dupes(!!var) %>%
         arrange(curr_hub)
   }

   # unknown data
   vars <- c(
      "tx_reg",
      "tx_prov",
      "tx_munc"
   )
   .log_info("Checking if required variables have UNKNOWN data or unpaired NHSSS versions.")
   for (var in vars) {
      var                                          <- as.symbol(var)
      nhsss$harp_tx$outcome.converted$check[[var]] <- nhsss$harp_tx$outcome.converted$data %>%
         filter(
            !!var %in% c("UNKNOWN", "OTHERS", NA_character_)
         ) %>%
         arrange(curr_hub)
   }

   # special checks
   .log_info("Checking for extreme dispensing.")
   nhsss$harp_tx$outcome.converted$check[["mmd"]] <- nhsss$harp_tx$outcome.converted$data %>%
      mutate(
         months_to_pickup = floor(days_to_pickup / 30)
      ) %>%
      filter(
         months_to_pickup >= 7
      ) %>%
      arrange(curr_hub)

   .log_info("Checking for extreme dispensing.")
   nhsss$harp_tx$outcome.converted$check[["mismatch_faci"]] <- nhsss$harp_tx$outcome.converted$data %>%
      filter(
         prev_ffup == curr_ffup,
         curr_hub != prev_hub
      ) %>%
      arrange(curr_hub)

   .log_info("Checking for resurrected clients.")
   nhsss$harp_tx$outcome.converted$check[["resurrect"]] <- nhsss$harp_tx$outcome.converted$data %>%
      filter(
         prev_outcome == "dead" & (curr_outcome != "dead" | is.na(curr_outcome)) | use_db == 1 & prev_outcome == "daed"
      ) %>%
      arrange(curr_hub)

   .log_info("Checking for mismatch artstart dates.")
   nhsss$harp_tx$outcome.converted$check[["oh_earlier_start"]] <- nhsss$harp_tx$outcome.converted$data %>%
      filter(
         artstart_date > oh_artstart
      ) %>%
      arrange(curr_hub)

   .log_info("Checking new mortalities.")
   nhsss$harp_tx$outcome.converted$check[["new_mort"]] <- nhsss$harp_tx$outcome.converted$data %>%
      filter(
         curr_outcome == "dead" & (prev_outcome != "dead" | is.na(prev_outcome))
      ) %>%
      arrange(curr_hub)

   # range-median
   vars <- c(
      "curr_age",
      "birthdate",
      "artstart_date",
      "oh_artstart",
      "prev_ffup",
      "prev_pickup",
      "curr_ffup",
      "curr_pickup",
      "days_to_pickup",
      "ref_death_date",
      "prev_num_drugs",
      "curr_num_drugs"
   )
   .log_info("Checking range-median of data.")
   nhsss$harp_tx$outcome.converted$check$tabstat <- data.frame()
   for (var in vars) {
      var <- as.symbol(var)
      df  <- nhsss$harp_tx$outcome.converted$data

      nhsss$harp_tx$outcome.converted$check$tabstat <- df %>%
         summarise(
            VARIABLE = as.character(var),
            MIN      = suppress_warnings(min(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            MEDIAN   = suppress_warnings(median(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            MAX      = suppress_warnings(max(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            NAs      = sum(if_else(is.na(!!var), 1, 0, 0))
         ) %>%
         mutate_all(~as.character(.)) %>%
         bind_rows(nhsss$harp_tx$outcome.converted$check$tabstat)
   }
}

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
data_name <- "outcome.converted"
if (!is.empty(nhsss$harp_tx[[data_name]]$check))
   .validation_gsheets(
      data_name   = data_name,
      parent_list = nhsss$harp_tx[[data_name]]$check,
      drive_path  = paste0(nhsss$harp_tx$gdrive$path$report, "Validation/"),
      surv_name   = "HARP Tx"
   )

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
