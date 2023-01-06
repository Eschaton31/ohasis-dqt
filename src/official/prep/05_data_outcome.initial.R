##  Append to the previous art registry ----------------------------------------

clean_data <- function(forms, new_reg) {
   # get forms and relevant dates
   data <- new_reg %>%
      select(
         -(starts_with("prep") & !matches("prep_id")),
         -starts_with("hts"),
         -starts_with("lab"),
         -ends_with("screen"),
         -any_of(c("REC_ID",
                   "PATIENT_ID",
                   "self_identity",
                   "self_identity_other",
                   "gender_identity",
                   "sti_visit",
                   "eligible",
                   "with_hts",
                   "dispensed"))
      ) %>%
      left_join(
         y  = forms$prepdisp_first %>%
            select(
               CENTRAL_ID,
               PREP_START_DATE = VISIT_DATE
            ),
         by = "CENTRAL_ID"
      ) %>%
      left_join(
         y  = forms$prep_init_p12m %>%
            select(
               CENTRAL_ID,
               INITIATION_DATE = VISIT_DATE
            ),
         by = "CENTRAL_ID"
      ) %>%
      left_join(
         y  = forms$prep_last,
         by = "CENTRAL_ID"
      ) %>%
      mutate(
         # date variables
         encoded_date    = as.Date(CREATED_AT),
         LATEST_VISIT    = VISIT_DATE,

         # Age
         AGE_DTA         = calc_age(birthdate, VISIT_DATE),

         # tag those without PREP_FACI
         use_record_faci = if_else(
            condition = is.na(SERVICE_FACI),
            true      = 1,
            false     = 0
         ),
         SERVICE_FACI    = if_else(
            condition = use_record_faci == 1,
            true      = FACI_ID,
            false     = SERVICE_FACI
         ),

         # tag sail clinics as ship
         sail_clinic     = case_when(
            FACI_ID == "040200" ~ 1,
            SERVICE_FACI == "040200" ~ 1,
            FACI_ID == "040211" ~ 1,
            SERVICE_FACI == "040211" ~ 1,
            FACI_ID == "130748" ~ 1,
            SERVICE_FACI == "130748" ~ 1,
            TRUE ~ 0
         ),

         # tag tly clinic
         tly_clinic      = case_when(
            FACI_ID == "070021" ~ 1,
            FACI_ID == "040198" ~ 1,
            FACI_ID == "070021" ~ 1,
            FACI_ID == "130173" ~ 1,
            FACI_ID == "130707" ~ 1,
            FACI_ID == "130708" ~ 1,
            FACI_ID == "130749" ~ 1,
            FACI_ID == "130751" ~ 1,
            FACI_ID == "130001" ~ 1,
            SERVICE_FACI == "070021" ~ 1,
            SERVICE_FACI == "040198" ~ 1,
            SERVICE_FACI == "070021" ~ 1,
            SERVICE_FACI == "130173" ~ 1,
            SERVICE_FACI == "130707" ~ 1,
            SERVICE_FACI == "130708" ~ 1,
            SERVICE_FACI == "130749" ~ 1,
            SERVICE_FACI == "130751" ~ 1,
            SERVICE_FACI == "130001" ~ 1,
            TRUE ~ 0
         ),
      )

   return(data)
}

##  Sort by earliest visit of client for the report ----------------------------

prioritize_reports <- function(data) {
   data %<>%
      arrange(desc(VISIT_DATE), desc(LATEST_NEXT_DATE), CENTRAL_ID) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      rename(
         PREP_FACI     = SERVICE_FACI,
         PREP_SUB_FACI = SERVICE_SUB_FACI,
      )
   return(data)
}

##  Facilities -----------------------------------------------------------------

attach_faci_names <- function(data) {
   # record faci
   data <- data %>%
      ohasis$get_faci(
         list(FACI_CODE = c("FACI_ID", "SUB_FACI_ID")),
         "code"
      ) %>%
      ohasis$get_faci(
         list(PREP_FACI_CODE = c("PREP_FACI", "PREP_SUB_FACI")),
         "code",
         c("prep_reg", "prep_prov", "prep_munc")
      ) %>%
      # arrange via faci
      mutate(
         PREP_BRANCH = if_else(
            condition = nchar(PREP_FACI_CODE) > 3,
            true      = PREP_FACI_CODE,
            false     = NA_character_
         ),
         .after      = PREP_FACI_CODE
      ) %>%
      mutate(
         PREP_BRANCH = case_when(
            PREP_FACI_CODE == "HASH" & PREP_BRANCH == "HASH" ~ "HASH-QC",
            PREP_FACI_CODE == "HASH" & is.na(PREP_BRANCH) ~ "HASH-QC",
            PREP_FACI_CODE == "TLY" & is.na(PREP_BRANCH) ~ "TLY-ANGLO",
            TRUE ~ PREP_BRANCH
         ),
      ) %>%
      mutate_at(
         .vars = vars(PREP_FACI_CODE),
         ~case_when(
            stri_detect_regex(., "^HASH") ~ "HASH",
            stri_detect_regex(., "^SAIL") ~ "SAIL",
            stri_detect_regex(., "^TLY") ~ "TLY",
            TRUE ~ .
         )
      ) %>%
      mutate(
         PREP_BRANCH = case_when(
            PREP_FACI_CODE == "HASH" & is.na(PREP_BRANCH) ~ "HASH-QC",
            PREP_FACI_CODE == "TLY" & is.na(PREP_BRANCH) ~ "TLY-ANGLO",
            PREP_FACI_CODE == "SHP" & is.na(PREP_BRANCH) ~ "SHIP-MAKATI",
            TRUE ~ PREP_BRANCH
         ),
      ) %>%
      arrange(PREP_FACI_CODE, VISIT_DATE, LATEST_NEXT_DATE)

   return(data)
}

##  Flag data for validation ---------------------------------------------------

get_checks <- function(data) {
   check  <- list()
   update <- input(
      prompt  = "Run `outcome.initial` validations?",
      options = c("1" = "yes", "2" = "no"),
      default = "1"
   )
   update <- substr(toupper(update), 1, 1)
   if (update == "1") {
      # initialize checking layer

      view_vars <- c(
         "REC_ID",
         "CENTRAL_ID",
         "FORM_VERSION",
         "prep_id",
         "uic",
         "px_code",
         "philhealth_no",
         "philsys_id",
         "first",
         "middle",
         "last",
         "suffix",
         "birthdate",
         "sex",
         "PREP_FACI_CODE",
         "PREP_BRANCH",
         "VISIT_DATE",
         "CLINIC_NOTES",
         "COUNSEL_NOTES",
         "with_hts",
         "risk_screen",
         "ars_screen",
         "sti_screen",
         "eligible",
         "dispensed",
         "prep_on",
         "prep_plan",
         "prep_type"
      )

      # dates
      date_vars <- c(
         "encoded_date",
         "VISIT_DATE",
         "DISP_DATE"
      )
      check     <- check_dates(data, check, view_vars, date_vars)

      # non-negotiable variables
      nonnegotiables <- c(
         "PREP_FACI_CODE",
         "PREP_TYPE",
         "PREP_PLAN",
         "MEDICINE_SUMMARY"
      )
      check          <- check_nonnegotiables(data, check, view_vars, nonnegotiables)

      .log_info("Checking for dispensing later than next pick-up.")
      check[["disp_>_next"]] <- data %>%
         filter(
            VISIT_DATE > LATEST_NEXT_DATE
         ) %>%
         select(
            any_of(view_vars),
            LATEST_NEXT_DATE
         )

      # special checks
      .log_info("Checking for dispensed with no meds.")
      check[["dispensed_no_meds"]] <- data %>%
         filter(
            dispensed == 1,
            is.na(MEDICINE_SUMMARY)
         ) %>%
         select(
            any_of(view_vars),
            MEDICINE_SUMMARY,
            DISP_DATE,
         )

      .log_info("Checking for no hts form.")
      check[["no_hts"]] <- data %>%
         filter(
            is.na(with_hts)
         ) %>%
         select(
            any_of(view_vars)
         )

      .log_info("Checking for reactive.")
      check[["hts_reactive"]] <- data %>%
         filter(
            hts_result == "R"
         ) %>%
         select(
            any_of(view_vars),
            HTS_REC,
            hts_result
         )

      .log_info("Checking for incomplete prep info.")
      check[["inc_prep"]] <- data %>%
         filter(
            dispensed == 1,
            prep_type == "(no data)" | prep_plan == "(no data)"
         ) %>%
         select(
            any_of(view_vars)
         )

      .log_info("Checking for mismatch dispensed and visit dates.")
      check[["mismatch_disp"]] <- data %>%
         filter(
            as.Date(DISP_DATE) != RECORD_DATE
         ) %>%
         select(
            any_of(view_vars),
            RECORD_DATE,
            DISP_DATE
         )

      .log_info("Checking for new clients that are not enrollees.")
      check[["non_enrollee"]] <- data %>%
         filter(
            VISIT_DATE < ohasis$date
         ) %>%
         select(
            any_of(view_vars),
         )

      .log_info("Checking for mismatch record vs art faci.")
      check[["mismatch_faci"]] <- data %>%
         filter(
            FACI_CODE != PREP_FACI_CODE
         ) %>%
         select(
            any_of(view_vars),
            FACI_CODE,
         )

      .log_info("Checking for possible PMTCT-N clients.")
      check[["possible_pmtct"]] <- data %>%
         filter(
            (NUM_OF_DRUGS == 1 & stri_detect_fixed(MEDICINE_SUMMARY, "syr")) |
               AGE <= 5 |
               AGE_DTA <= 5
         ) %>%
         select(
            any_of(view_vars),
            MEDICINE_SUMMARY,
         )

      .log_info("Checking for young clients.")
      check[["young_prep"]] <- data %>%
         filter(
            AGE < 15
         ) %>%
         select(
            any_of(view_vars),
            MEDICINE_SUMMARY,
         )

      .log_info("Checking PrEP reports tagged as DOH-EB.")
      check[["prep_eb"]] <- data %>%
         filter(
            PREP_FACI_CODE == "DOH"
         ) %>%
         select(
            any_of(view_vars),
         )

      # range-median
      tabstat <- c(
         "encoded_date",
         "VISIT_DATE",
         "DISP_DATE",
         "LATEST_NEXT_DATE",
         "BIRTHDATE",
         "AGE",
         "AGE_MO"
      )
      check   <- check_tabstat(data, check, tabstat)
   }
   return(check)
}

##  Actual flow ----------------------------------------------------------------

.init <- function() {
   p <- parent.env(environment())
   local(envir = p, {
      forms <- .GlobalEnv$nhsss$prep$forms
      data  <- clean_data(forms, .GlobalEnv$nhsss$prep$official$new_reg) %>%
         prioritize_reports() %>%
         attach_faci_names()

      rm(forms)

      write_rds(data, file.path(wd, "outcome.initial.RDS"))

      check <- get_checks(data)
   })

   local(envir = .GlobalEnv, flow_validation(nhsss$prep, "outcome.initial", ohasis$ym))
}