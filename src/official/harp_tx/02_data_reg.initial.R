##  Filter Initial Data & Remove Already Reported ------------------------------
clean_data <- function(art_first, old_reg) {
   data <- art_first %>%
      anti_join(
         y  = old_reg %>%
            select(CENTRAL_ID),
         by = "CENTRAL_ID"
      ) %>%
      filter(
         VISIT_DATE <= ohasis$next_date
      ) %>%
      mutate_at(
         .vars = vars(FIRST, MIDDLE, LAST, SUFFIX),
         ~toupper(.)
      ) %>%
      mutate(
         # date variables
         encoded_date       = as.Date(CREATED_AT),
         visit_date         = VISIT_DATE,

         # name
         name               = paste0(
            if_else(
               condition = is.na(LAST),
               true      = "",
               false     = LAST
            ), ", ",
            if_else(
               condition = is.na(FIRST),
               true      = "",
               false     = FIRST
            ), " ",
            if_else(
               condition = is.na(MIDDLE),
               true      = "",
               false     = MIDDLE
            ), " ",
            if_else(
               condition = is.na(SUFFIX),
               true      = "",
               false     = SUFFIX
            )
         ),
         name               = str_squish(name),
         STANDARD_FIRST     = stri_trans_general(FIRST, "latin-ascii"),

         # Age
         AGE                = if_else(
            condition = is.na(AGE) & !is.na(AGE_MO),
            true      = AGE_MO / 12,
            false     = as.double(AGE)
         ),
         AGE_DTA            = if_else(
            condition = !is.na(BIRTHDATE),
            true      = floor((VISIT_DATE - BIRTHDATE) / 365.25) %>% as.numeric(),
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
      )
   return(data)
}

##  Sort by earliest visit of client for the report ----------------------------

prioritize_reports <- function(data) {
   data %<>%
      arrange(VISIT_DATE, desc(LATEST_NEXT_DATE), CENTRAL_ID) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      rename(
         ART_FACI     = SERVICE_FACI,
         ART_SUB_FACI = SERVICE_SUB_FACI,
      )
   return(data)
}

##  Adding CD4 results ---------------------------------------------------------

get_cd4 <- function(data) {
   data %<>%
      # get cd4 data
      # TODO: attach max dates for filtering of cd4 data
      left_join(
         y  = .GlobalEnv$nhsss$harp_tx$forms$lab_cd4 %>%
            select(
               CD4_DATE,
               CD4_RESULT,
               CENTRAL_ID
            ),
         by = "CENTRAL_ID"
      ) %>%
      mutate(
         # calculate distance from confirmatory date
         CD4_DATE     = as.Date(CD4_DATE),
         CD4_ENROLL   = difftime(as.Date(VISIT_DATE), CD4_DATE, units = "days") %>% as.numeric(),

         # baseline is within 182 days
         BASELINE_CD4 = if_else(
            CD4_ENROLL >= -182 & CD4_ENROLL <= 182,
            1,
            0
         ),

         # make values absolute to take date nearest to confirmatory
         CD4_ENROLL   = abs(CD4_ENROLL %>% as.numeric()),
      ) %>%
      arrange(CENTRAL_ID, CD4_ENROLL) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE)
   return(data)
}

##  Facilities -----------------------------------------------------------------

attach_faci_names <- function(data) {
   # record faci
   data <- ohasis$get_faci(
      data,
      list("FACI_CODE" = c("FACI_ID", "SUB_FACI_ID")),
      "code"
   )
   # art faci
   data <- ohasis$get_faci(
      data,
      list("ART_FACI_CODE" = c("ART_FACI", "ART_SUB_FACI")),
      "code",
      c("tx_reg", "tx_prov", "tx_munc")
   )
   # epic / gf faci
   data <- ohasis$get_faci(
      data,
      list("ACTUAL_FACI_CODE" = c("ACTUAL_FACI", "ACTUAL_SUB_FACI")),
      "code",
      c("real_reg", "real_prov", "real_munc")
   )
   # satellite
   data <- ohasis$get_faci(
      data,
      list("SATELLITE_FACI_CODE" = c("SATELLITE_FACI", "SATELLITE_SUB_FACI")),
      "code"
   )
   # satellite
   data <- ohasis$get_faci(
      data,
      list("TRANSIENT_FACI_CODE" = c("TRANSIENT_FACI", "TRANSIENT_SUB_FACI")),
      "code"
   )

   data %<>%
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
      arrange(ART_FACI_CODE, VISIT_DATE, LATEST_NEXT_DATE)

   return(data)
}

##  Flag data for validation ---------------------------------------------------

get_checks <- function(data) {
   check  <- list()
   update <- input(
      prompt  = "Run `reg.initial` validations?",
      options = c("1" = "yes", "2" = "no"),
      default = "1"
   )
   if (update == "1") {
      # initialize checking layer

      view_vars <- c(
         "REC_ID",
         "PATIENT_ID",
         "FORM_VERSION",
         "CONFIRMATORY_CODE",
         "UIC",
         "PATIENT_CODE",
         "PHILHEALTH_NO",
         "PHILSYS_ID",
         "ART_FACI_CODE",
         "FIRST",
         "MIDDLE",
         "LAST",
         "SUFFIX",
         "BIRTHDATE",
         "SEX",
         "ART_BRANCH",
         "SATELLITE_FACI_CODE",
         "TRANSIENT_FACI_CODE",
         "ACTUAL_FACI_CODE",
         "ACTUAL_BRANCH",
         "VISIT_DATE",
         "CLINIC_NOTES",
         "COUNSEL_NOTES"
      )
      check     <- check_pii(data, check, view_vars)
      check     <- check_addr(data, check, view_vars, "curr")

      # dates
      date_vars <- c(
         "encoded_date",
         "VISIT_DATE",
         "DISP_DATE",
         "LATEST_NEXT_DATE"
      )
      check     <- check_dates(data, check, view_vars, date_vars)

      # non-negotiable variables
      nonnegotiables <- c(
         "FORM_VERSION",
         "ART_FACI_CODE",
         "CENTRAL_ID",
         "AGE",
         "UIC",
         "MEDICINE_SUMMARY"
      )
      check          <- check_nonnegotiables(data, check, view_vars, nonnegotiables)
      check          <- check_preggy(data, check, view_vars)
      check          <- check_age(data, check, view_vars)

      # special checks
      .log_info("Checking for new clients tagged as refills.")
      check[["refill_enroll"]] <- data %>%
         filter(
            StrLeft(TX_STATUS, 1) == "2"
         ) %>%
         select(
            any_of(view_vars),
            TX_STATUS,
            VISIT_TYPE
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
            FACI_CODE != ART_FACI_CODE,
            sail_clinic != 1
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

      .log_info("Checking for possible PrEP clients.")
      check[["possible_prep"]] <- data %>%
         filter(
            stri_detect_fixed(MEDICINE_SUMMARY, "FTC")
         ) %>%
         select(
            any_of(view_vars),
            MEDICINE_SUMMARY,
         )

      .log_info("Checking ART reports tagged as DOH-EB.")
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

      # Remove already tagged data from validation
      exclude <- input(
         prompt  = "Exclude clients initially tagged for dropping from validations?",
         options = c("1" = "yes", "2" = "no"),
         default = "1"
      )
      exclude <- substr(toupper(exclude), 1, 1)
      if (exclude == "1") {
         .log_info("Dropping unwanted records.")
         if (update == "1") {
            for (drop in c("drop_notart", "drop_notyet")) {
               if (drop %in% names(.GlobalEnv$nhsss$harp_tx$corr))
                  for (check in names(check)) {
                     if (check != "tabstat")
                        check[[check]] %<>%
                           anti_join(
                              y  = .GlobalEnv$nhsss$harp_tx$corr[[drop]],
                              by = "REC_ID"
                           )
                  }
            }
         }
      }
   }

   return(check)
}

##  Actual flow ----------------------------------------------------------------

.init <- function() {
   p <- parent.env(environment())
   local(envir = p, {
      data <- clean_data(.GlobalEnv$nhsss$harp_tx$forms$art_first, .GlobalEnv$nhsss$harp_tx$official$old_reg) %>%
         prioritize_reports() %>%
         get_cd4() %>%
         attach_faci_names()

      write_rds(data, file.path(wd, "reg.initial.RDS"))

      check <- get_checks(data)
   })

   local(envir = .GlobalEnv, flow_validation(nhsss$harp_tx, "reg.initial", ohasis$ym))
}