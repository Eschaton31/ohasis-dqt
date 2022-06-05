##  Filter Initial Data & Remove Already Reported ------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
.log_info("Generating `prep`.`reg-initial`.")
.log_info("Opening connections.")
lw_conn <- ohasis$conn("lw")

# Form PrEP not yet reported
.log_info("Dropping already-reported records.")
data <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "prep_first")) %>%
   select(CENTRAL_ID, REC_ID) %>%
   anti_join(
      y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "prep_old")),
      by = "CENTRAL_ID"
   ) %>%
   left_join(
      y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_prep")),
      by = "REC_ID"
   ) %>%
   collect()
dbDisconnect(lw_conn)

.log_info("Performing initial cleaning.")
data %<>%
   mutate_at(
      .vars = vars(FIRST, MIDDLE, LAST, SUFFIX),
      ~toupper(.)
   ) %>%
   mutate(
      # date variables
      encoded_date    = as.Date(CREATED_AT),

      # name
      name            = paste0(
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
      name            = str_squish(name),

      # Age
      AGE_DTA         = if_else(
         condition = !is.na(BIRTHDATE),
         true      = floor((VISIT_DATE - BIRTHDATE) / 365.25) %>% as.numeric(),
         false     = as.numeric(NA)
      ),

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
   )

##  Sort by earliest visit of client for the report ----------------------------

.log_info("Prioritizing reports.")
data %<>%
   arrange(VISIT_DATE, desc(LATEST_NEXT_DATE), CENTRAL_ID) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   rename(
      PREP_FACI     = SERVICE_FACI,
      PREP_SUB_FACI = SERVICE_SUB_FACI,
   )

##  Facilities -----------------------------------------------------------------

.log_info("Attaching facility names (OHASIS versions).")
# record faci
data <- ohasis$get_faci(
   data,
   list("FACI_CODE" = c("FACI_ID", "SUB_FACI_ID")),
   "code"
)
# art faci
data <- ohasis$get_faci(
   data,
   list("PREP_FACI_CODE" = c("PREP_FACI", "PREP_SUB_FACI")),
   "code"
)

# arrange via faci
data %<>%
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

##  Flag data for validation ---------------------------------------------------

update <- input(
   prompt  = "Run `reg-initial` validations?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
update <- substr(toupper(update), 1, 1)

nhsss$prep$reg.initial$check <- list()
if (update == "1") {
   # initialize checking layer

   view_vars <- c(
      "REC_ID",
      "PATIENT_ID",
      "FORM_VERSION",
      "UIC",
      "PATIENT_CODE",
      "PHILHEALTH_NO",
      "PHILSYS_ID",
      "FIRST",
      "MIDDLE",
      "LAST",
      "SUFFIX",
      "BIRTHDATE",
      "SEX",
      "PREP_FACI_CODE",
      "PREP_BRANCH",
      "VISIT_DATE",
      "CLINIC_NOTES",
      "COUNSEL_NOTES"
   )

   # dates
   vars <- c(
      "encoded_date",
      "VISIT_DATE",
      "DISP_DATE",
      "BIRTHDATE",
      "LATEST_NEXT_DATE"
   )
   .log_info("Checking dates.")
   for (var in vars) {
      var                                 <- as.symbol(var)
      nhsss$prep$reg.initial$check[[var]] <- data %>%
         filter(
            is.na(!!var) |
               !!var >= ohasis$next_date |
               !!var <= as.Date("1900-01-01")
         ) %>%
         select(
            any_of(view_vars),
            !!var
         )

      if (as.character(var) %in% c("LATEST_NEXT_DATE", "encoded_date"))
         nhsss$prep$reg.initial$check[[var]] <- nhsss$prep$reg.initial$check[[var]] %>%
            filter(
               is.na(!!var) |
                  !!var <= as.Date("1900-01-01")
            )
   }

   # non-negotiable variables
   vars <- c(
      "FORM_VERSION",
      "PREP_FACI_CODE",
      "FIRST",
      "LAST",
      "CENTRAL_ID",
      "AGE",
      "SEX",
      "CONFIRMATORY_CODE",
      "UIC",
      "MEDICINE_SUMMARY"
   )
   .log_info("Checking if non-negotiable variables are missing.")
   for (var in vars) {
      var                                 <- as.symbol(var)
      nhsss$prep$reg.initial$check[[var]] <- data %>%
         filter(
            is.na(!!var)
         ) %>%
         select(
            any_of(view_vars),
            !!var
         )
   }

   # special checks
   .log_info("Checking for mismatch dispensed and visit dates.")
   nhsss$prep$reg.initial$check[["mismatch_disp"]] <- data %>%
      filter(
         as.Date(DISP_DATE) != RECORD_DATE
      ) %>%
      select(
         any_of(view_vars),
         RECORD_DATE,
         DISP_DATE
      )

   .log_info("Checking for short names.")
   nhsss$prep$reg.initial$check[["short_name"]] <- data %>%
      mutate(
         n_first  = nchar(FIRST),
         n_middle = nchar(MIDDLE),
         n_last   = nchar(LAST),
         n_name   = n_first + n_middle + n_last,
      ) %>%
      filter(
         n_name <= 10 | n_first <= 3 | n_last <= 3
      ) %>%
      select(
         any_of(view_vars),
      )

   .log_info("Checking for new clients that are not enrollees.")
   nhsss$prep$reg.initial$check[["non_enrollee"]] <- data %>%
      filter(
         VISIT_DATE < ohasis$date
      ) %>%
      select(
         any_of(view_vars),
      )

   .log_info("Checking for mismatch record vs art faci.")
   nhsss$prep$reg.initial$check[["mismatch_faci"]] <- data %>%
      filter(
         FACI_CODE != PREP_FACI_CODE
      ) %>%
      select(
         any_of(view_vars),
         FACI_CODE,
      )

   .log_info("Checking for possible PMTCT-N clients.")
   nhsss$prep$reg.initial$check[["possible_pmtct"]] <- data %>%
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
   nhsss$prep$reg.initial$check[["young_prep"]] <- data %>%
      filter(
         AGE < 15
      ) %>%
      select(
         any_of(view_vars),
         MEDICINE_SUMMARY,
      )

   .log_info("Checking calculated age vs computed age.")
   nhsss$prep$reg.initial$check[["mismatch_age"]] <- data %>%
      filter(
         AGE != AGE_DTA
      ) %>%
      select(
         any_of(view_vars),
         AGE,
         AGE_DTA
      )

   .log_info("Checking PrEP reports tagged as DOH-EB.")
   nhsss$prep$reg.initial$check[["prep_eb"]] <- data %>%
      filter(
         PREP_FACI_CODE == "DOH"
      ) %>%
      select(
         any_of(view_vars),
      )

   # range-median
   vars <- c(
      "encoded_date",
      "VISIT_DATE",
      "DISP_DATE",
      "LATEST_NEXT_DATE",
      "BIRTHDATE",
      "AGE",
      "AGE_MO"
   )
   .log_info("Checking range-median of data.")
   nhsss$prep$reg.initial$check$tabstat <- data.frame()
   for (var in vars) {
      var <- as.symbol(var)
      df  <- data

      nhsss$prep$reg.initial$check$tabstat <- df %>%
         summarise(
            VARIABLE = as.character(var),
            MIN      = suppress_warnings(min(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            MEDIAN   = suppress_warnings(median(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            MAX      = suppress_warnings(max(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            NAs      = sum(if_else(is.na(!!var), 1, 0, 0))
         ) %>%
         mutate_all(~as.character(.)) %>%
         bind_rows(nhsss$prep$reg.initial$check$tabstat)
   }
}

##  Remove already tagged data from validation ---------------------------------

exclude <- input(
   prompt  = "Exlude clients initially tagged for dropping from validations?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
exclude <- substr(toupper(exclude), 1, 1)
if (exclude == "1") {
   .log_info("Dropping unwanted records.")
   if (update == "1") {
      for (drop in c("drop_notart", "drop_notyet")) {
         if (drop %in% names(nhsss$prep$corr))
            for (check in names(nhsss$prep$reg.initial$check)) {
               if (check != "tabstat")
                  nhsss$prep$reg.initial$check[[check]] %<>%
                     anti_join(
                        y  = nhsss$prep$corr[[drop]],
                        by = "REC_ID"
                     )
            }
      }
   }
}

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
data_name <- "reg.initial"
if (!is.empty(nhsss$prep[[data_name]]$check))
   .validation_gsheets(
      data_name   = data_name,
      parent_list = nhsss$prep[[data_name]]$check,
      drive_path  = paste0(nhsss$prep$gdrive$path$report, "Validation/"),
      surv_name   = "PrEP"
   )

nhsss$prep$reg.initial$data <- data
.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
