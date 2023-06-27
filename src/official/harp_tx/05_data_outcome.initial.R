##  Append to the previous art registry ----------------------------------------
clean_data <- function(art_last, new_reg) {
   data <- art_last %>%
      arrange(desc(LATEST_NEXT_DATE)) %>%
      filter(VISIT_DATE <= ohasis$next_date) %>%
      mutate(LATEST_VISIT = VISIT_DATE) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE)

   data %<>%
      right_join(
         y  = new_reg %>%
            select(-PATIENT_ID) %>%
            rename(artstart_rec = REC_ID),
         by = "CENTRAL_ID"
      ) %>%
      mutate(
         # date variables
         encoded_date       = as.Date(CREATED_AT),

         # Age
         AGE                = if_else(
            condition = is.na(AGE) & !is.na(AGE_MO),
            true      = AGE_MO / 12,
            false     = as.double(AGE)
         ),
         AGE_DTA            = if_else(
            condition = !is.na(birthdate),
            true      = floor((VISIT_DATE - birthdate) / 365.25) %>% as.numeric(),
            false     = as.numeric(NA)
         ),

         # tag those without ART_FACI
         use_record_faci    = if_else(
            condition = is.na(SERVICE_FACI),
            true      = 1,
            false     = 0
         ),
         SERVICE_FACI       = if_else(
            condition = use_record_faci == 1,
            true      = FACI_ID,
            false     = SERVICE_FACI
         ),

         # tag sail clinics as ship
         sail_clinic        = case_when(
            FACI_ID == "040200" ~ 1,
            SERVICE_FACI == "040200" ~ 1,
            FACI_ID == "040211" ~ 1,
            SERVICE_FACI == "040211" ~ 1,
            FACI_ID == "130748" ~ 1,
            SERVICE_FACI == "130748" ~ 1,
            FACI_ID == "130814" ~ 1,
            SERVICE_FACI == "130814" ~ 1,
            TRUE ~ 0
         ),

         # tag tly clinic
         tly_clinic         = case_when(
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

         # convert to HARP facility
         ACTUAL_FACI        = SERVICE_FACI,
         ACTUAL_SUB_FACI    = SERVICE_SUB_FACI,
         SERVICE_FACI       = case_when(
            tly_clinic == 1 ~ "130001",
            sail_clinic == 1 ~ "130025",
            TRUE ~ SERVICE_FACI
         ),

         # satellite
         SATELLITE_FACI     = if_else(
            condition = StrLeft(CLIENT_TYPE, 1) == "5",
            true      = FACI_DISP,
            false     = NA_character_
         ),
         SATELLITE_SUB_FACI = if_else(
            condition = StrLeft(CLIENT_TYPE, 1) == "5",
            true      = SUB_FACI_DISP,
            false     = NA_character_
         ),

         # transient
         TRANSIENT_FACI     = if_else(
            condition = StrLeft(CLIENT_TYPE, 1) == "6",
            true      = FACI_DISP,
            false     = NA_character_
         ),
         TRANSIENT_SUB_FACI = if_else(
            condition = StrLeft(CLIENT_TYPE, 1) == "6",
            true      = SUB_FACI_DISP,
            false     = NA_character_
         ),

         LATEST_NEXT_DATE   = as.Date(LATEST_NEXT_DATE),
         DISP_DATE          = as.Date(DISP_DATE),
      )
   return(data)
}

##  Sort by earliest visit of client for the report ----------------------------

prioritize_reports <- function(data) {
   data %<>%
      arrange(desc(VISIT_DATE), desc(LATEST_NEXT_DATE), CENTRAL_ID) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      rename(
         ART_FACI     = SERVICE_FACI,
         ART_SUB_FACI = SERVICE_SUB_FACI,
      )
   return(data)
}

##  Facilities -----------------------------------------------------------------

attach_faci_names <- function(data) {
   # record faci
   data %<>%
      ohasis$get_faci(
         list("FACI_CODE" = c("FACI_ID", "SUB_FACI_ID")),
         "code"
      ) %>%
      # art faci
      ohasis$get_faci(
         list("ART_FACI_CODE" = c("ART_FACI", "ART_SUB_FACI")),
         "code",
         c("tx_reg", "tx_prov", "tx_munc")
      ) %>%
      # epic / gf faci
      ohasis$get_faci(
         list("ACTUAL_FACI_CODE" = c("ACTUAL_FACI", "ACTUAL_SUB_FACI")),
         "code",
         c("real_reg", "real_prov", "real_munc")
      ) %>%
      # satellite
      ohasis$get_faci(
         list("SATELLITE_FACI_CODE" = c("SATELLITE_FACI", "SATELLITE_SUB_FACI")),
         "code"
      ) %>%
      # satellite
      ohasis$get_faci(
         list("TRANSIENT_FACI_CODE" = c("TRANSIENT_FACI", "TRANSIENT_SUB_FACI")),
         "code"
      ) %>%
      mutate(
         ART_BRANCH = if_else(
            condition = nchar(ART_FACI_CODE) > 3,
            true      = ART_FACI_CODE,
            false     = NA_character_
         ),
         .after     = ART_FACI_CODE
      ) %>%
      mutate(
         ACTUAL_BRANCH = if_else(
            condition = nchar(ACTUAL_FACI_CODE) > 3,
            true      = ACTUAL_FACI_CODE,
            false     = NA_character_
         ),
         .after        = ACTUAL_FACI_CODE
      ) %>%
      mutate(
         ART_BRANCH    = case_when(
            ART_FACI_CODE == "TLY" & is.na(ART_BRANCH) ~ "TLY-ANGLO",
            TRUE ~ ART_BRANCH
         ),
         ACTUAL_BRANCH = case_when(
            ACTUAL_FACI_CODE == "TLY" & is.na(ACTUAL_BRANCH) ~ "TLY-ANGLO",
            TRUE ~ ACTUAL_BRANCH
         ),
      ) %>%
      mutate_at(
         .vars = vars(FACI_CODE, ACTUAL_FACI_CODE, ART_FACI_CODE),
         ~case_when(
            stri_detect_regex(., "^SAIL") ~ "SAIL",
            stri_detect_regex(., "^TLY") ~ "TLY",
            TRUE ~ .
         )
      ) %>%
      mutate(
         ART_BRANCH = case_when(
            sail_clinic == 1 ~ ACTUAL_BRANCH,
            tly_clinic == 1 & is.na(ACTUAL_BRANCH) ~ "TLY-ANGLO",
            tly_clinic == 1 & ACTUAL_BRANCH == "TLY" ~ "TLY-ANGLO",
            tly_clinic == 1 & ACTUAL_BRANCH != "TLY" ~ ACTUAL_BRANCH,
            TRUE ~ ART_BRANCH
         ),
      ) %>%
      arrange(ART_FACI_CODE, VISIT_DATE, LATEST_NEXT_DATE) %>%
      mutate(
         ART_BRANCH    = case_when(
            ART_FACI_CODE == "SHP" & is.na(ART_BRANCH) ~ "SHIP-MAKATI",
            ART_FACI_CODE == "TLY" & is.na(ART_BRANCH) ~ "TLY-ANGLO",
            TRUE ~ ART_BRANCH
         ),
         ACTUAL_BRANCH = case_when(
            ACTUAL_FACI_CODE == "SHP" & is.na(ACTUAL_BRANCH) ~ "SHIP-MAKATI",
            ACTUAL_FACI_CODE == "TLY" & is.na(ACTUAL_BRANCH) ~ "TLY-ANGLO",
            TRUE ~ ACTUAL_BRANCH
         ),
      )

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
         "FACI_CODE",
         "ACTUAL_FACI_CODE",
         "ACTUAL_BRANCH",
         "FORM_VERSION",
         "art_id",
         "confirmatory_code",
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
         "TX_STATUS",
         "VISIT_DATE",
         "LATEST_NEXT_DATE",
         "MEDICINE_SUMMARY",
         "CLINIC_NOTES",
         "COUNSEL_NOTES"
      )

      # dates
      date_vars <- c(
         "VISIT_DATE",
         "DISP_DATE"
      )
      check     <- check_dates(data, check, view_vars, date_vars)

      # non-negotiable variables
      nonnegotiables <- c(
         "ART_FACI_CODE",
         "MEDICINE_SUMMARY"
      )
      check          <- check_nonnegotiables(data, check, view_vars, nonnegotiables)

      log_info("Checking for dispensing later than next pick-up.")
      check[["disp_>_next"]] <- data %>%
         filter(
            VISIT_DATE > LATEST_NEXT_DATE
         ) %>%
         select(
            any_of(view_vars),
            LATEST_NEXT_DATE
         )

      log_info("Checking for mismatch record vs art faci.")
      check[["mismatch_faci"]] <- data %>%
         filter(
            FACI_CODE != ART_FACI_CODE,
            sail_clinic != 1
         ) %>%
         select(
            any_of(view_vars),
         )

      log_info("Checking for mismatch visit vs disp date.")
      check[["mismatch_disp"]] <- data %>%
         mutate(
            diff = abs(as.numeric(difftime(RECORD_DATE, as.Date(DISP_DATE), units = "days")))
         ) %>%
         filter(
            diff > 21
         ) %>%
         select(
            any_of(view_vars),
            FACI_CODE,
            ART_FACI_CODE,
            RECORD_DATE,
            DISP_DATE,
            diff
         )

      log_info("Checking for possible PMTCT-N clients.")
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

      log_info("Checking for possible PrEP clients.")
      check[["possible_prep"]] <- data %>%
         filter(
            stri_detect_fixed(MEDICINE_SUMMARY, "FTC")
         ) %>%
         select(
            any_of(view_vars),
            MEDICINE_SUMMARY,
         )

      log_info("Checking calculated age vs computed age.")
      check[["mismatch_age"]] <- data %>%
         filter(
            AGE != AGE_DTA
         ) %>%
         select(
            any_of(view_vars),
            AGE,
            AGE_DTA
         )

      log_info("Checking ART reports tagged as DOH-EB.")
      check[["art_eb"]] <- data %>%
         filter(
            ART_FACI_CODE == "DOH"
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
      data <- clean_data(.GlobalEnv$nhsss$harp_tx$forms$art_last, .GlobalEnv$nhsss$harp_tx$official$new_reg) %>%
         prioritize_reports() %>%
         attach_faci_names()

      write_rds(data, file.path(wd, "outcome.initial.RDS"))

      check <- get_checks(data)
   })

   local(envir = .GlobalEnv, flow_validation(nhsss$harp_tx, "outcome.initial", ohasis$ym))
}