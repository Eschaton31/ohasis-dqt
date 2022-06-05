##  Append to the previous art registry ----------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
.log_info("Generating `prep`.`outcome.initial`.")
.log_info("Opening connections.")
lw_conn <- ohasis$conn("lw")

# get latest visits
.log_info("Getting latest visits from OHASIS.")
nhsss$prep$outcome.initial$data <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "art_last")) %>%
   select(CENTRAL_ID, REC_ID, LATEST_VISIT = VISIT_DATE) %>%
   left_join(
      y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_art_bc")),
      by = "REC_ID"
   ) %>%
   collect() %>%
   filter(LATEST_VISIT == VISIT_DATE)

.log_info("Performing initial cleaning.")
nhsss$prep$outcome.initial$data %<>%
   right_join(
      y  = nhsss$prep$official$new_reg %>%
         select(-REC_ID, -PATIENT_ID),
      by = "CENTRAL_ID"
   ) %>%
   mutate(
      # date variables
      encoded_date    = as.Date(CREATED_AT),

      # Age
      AGE_DTA         = if_else(
         condition = !is.na(birthdate),
         true      = floor((VISIT_DATE - birthdate) / 365.25) %>% as.numeric(),
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
nhsss$prep$outcome.initial$data %<>%
   # arrange(desc(VISIT_DATE), desc(LATEST_NEXT_DATE), CENTRAL_ID) %>%
   # distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   rename(
      PREP_FACI     = SERVICE_FACI,
      PREP_SUB_FACI = SERVICE_SUB_FACI,
   )

.log_info("Closing connections.")
dbDisconnect(lw_conn)

##  Facilities -----------------------------------------------------------------

.log_info("Attaching facility names (OHASIS versions).")
local(envir = nhsss$prep, {
   # record faci
   outcome.initial$data <- ohasis$get_faci(
      outcome.initial$data,
      list("FACI_CODE" = c("FACI_ID", "SUB_FACI_ID")),
      "code"
   )
   # art faci
   outcome.initial$data <- ohasis$get_faci(
      outcome.initial$data,
      list("PREP_FACI_CODE" = c("PREP_FACI", "PREP_SUB_FACI")),
      "code",
      c("prep_reg", "prep_prov", "prep_munc")
   )
})

# arrange via faci
nhsss$prep$outcome.initial$data %<>%
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
   prompt  = "Run `outcome.initial` validations?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
update <- substr(toupper(update), 1, 1)

nhsss$prep$outcome.initial$check <- list()
if (update == "1") {
   # initialize checking layer

   view_vars <- c(
      "REC_ID",
      "PATIENT_ID",
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
      "PREP_FACI_CODE",
      "PREP_BRANCH",
      "SATELLITE_FACI_CODE",
      "TRANSIENT_FACI_CODE",
      "ACTUAL_FACI_CODE",
      "ACTUAL_BRANCH",
      "VISIT_DATE",
      "CLINIC_NOTES",
      "COUNSEL_NOTES"
   )

   # dates
   vars <- c(
      "encoded_date",
      "VISIT_DATE",
      "DISP_DATE",
      "LATEST_NEXT_DATE"
   )
   .log_info("Checking dates.")
   for (var in vars) {
      var                                        <- as.symbol(var)
      nhsss$prep$outcome.initial$check[[var]] <- nhsss$prep$outcome.initial$data %>%
         filter(
            is.na(!!var) |
               !!var >= ohasis$next_date |
               !!var < as.Date("2002-01-01")
         ) %>%
         select(
            any_of(view_vars),
            !!var
         )

      if (as.character(var) %in% c("LATEST_NEXT_DATE", "encoded_date"))
         nhsss$prep$outcome.initial$check[[var]] <- nhsss$prep$outcome.initial$check[[var]] %>%
            filter(
               is.na(!!var) |
                  !!var <= as.Date("1900-01-01")
            )
   }

   # non-negotiable variables
   vars <- c(
      "PREP_FACI_CODE",
      "MEDICINE_SUMMARY"
   )
   .log_info("Checking if non-negotiable variables are missing.")
   for (var in vars) {
      var                                        <- as.symbol(var)
      nhsss$prep$outcome.initial$check[[var]] <- nhsss$prep$outcome.initial$data %>%
         filter(
            is.na(!!var)
         ) %>%
         select(
            any_of(view_vars),
            !!var
         )

      if (as.character(var) == "FORM_VERSION")
         nhsss$prep$outcome.initial$check[[var]] <- nhsss$prep$outcome.initial$check[[var]] %>%
            filter(year(VISIT_DATE) >= 2021)
   }

   .log_info("Checking for dispensing later than next pick-up.")
   nhsss$prep$outcome.initial$check[["disp_>_next"]] <- nhsss$prep$outcome.initial$data %>%
      filter(
         VISIT_DATE > LATEST_NEXT_DATE
      ) %>%
      select(
         any_of(view_vars),
         LATEST_NEXT_DATE
      )

   .log_info("Checking for mismatch record vs art faci.")
   nhsss$prep$outcome.initial$check[["mismatch_faci"]] <- nhsss$prep$outcome.initial$data %>%
      filter(
         FACI_CODE != PREP_FACI_CODE
      ) %>%
      select(
         any_of(view_vars),
      )

   .log_info("Checking for mismatch visit vs disp date.")
   nhsss$prep$outcome.initial$check[["mismatch_disp"]] <- nhsss$prep$outcome.initial$data %>%
      mutate(
         diff = abs(as.numeric(difftime(RECORD_DATE, as.Date(DISP_DATE), units = "days")))
      ) %>%
      filter(
         diff > 21
      ) %>%
      select(
         any_of(view_vars),
         FACI_CODE,
         PREP_FACI_CODE,
         RECORD_DATE,
         DISP_DATE,
         diff
      )

   .log_info("Checking for possible PMTCT-N clients.")
   nhsss$prep$outcome.initial$check[["possible_pmtct"]] <- nhsss$prep$outcome.initial$data %>%
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
   nhsss$prep$outcome.initial$check[["possible_prep"]] <- nhsss$prep$outcome.initial$data %>%
      filter(
         stri_detect_fixed(MEDICINE_SUMMARY, "FTC")
      ) %>%
      select(
         any_of(view_vars),
         MEDICINE_SUMMARY,
      )

   .log_info("Checking for males tagged as pregnant.")
   nhsss$prep$outcome.initial$check[["pregnant_m"]] <- nhsss$prep$outcome.initial$data %>%
      filter(
         StrLeft(IS_PREGNANT, 1) == '1',
         StrLeft(sex, 1) == 'M'
      ) %>%
      select(
         any_of(view_vars),
         IS_PREGNANT,
      )

   .log_info("Checking for pregnant females.")
   nhsss$prep$outcome.initial$check[["pregnant_f"]] <- nhsss$prep$outcome.initial$data %>%
      filter(
         StrLeft(IS_PREGNANT, 1) == '1',
         StrLeft(sex, 1) == 'F'
      ) %>%
      select(
         any_of(view_vars),
         IS_PREGNANT,
      )

   .log_info("Checking calculated age vs computed age.")
   nhsss$prep$outcome.initial$check[["mismatch_age"]] <- nhsss$prep$outcome.initial$data %>%
      filter(
         AGE != AGE_DTA
      ) %>%
      select(
         any_of(view_vars),
         AGE,
         AGE_DTA
      )

   .log_info("Checking ART reports tagged as DOH-EB.")
   nhsss$prep$outcome.initial$check[["art_eb"]] <- nhsss$prep$outcome.initial$data %>%
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
   nhsss$prep$outcome.initial$check$tabstat <- data.frame()
   for (var in vars) {
      var <- as.symbol(var)
      df  <- nhsss$prep$outcome.initial$data

      nhsss$prep$outcome.initial$check$tabstat <- df %>%
         summarise(
            VARIABLE = as.character(var),
            MIN      = suppress_warnings(min(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            MEDIAN   = suppress_warnings(median(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            MAX      = suppress_warnings(max(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            NAs      = sum(if_else(is.na(!!var), 1, 0, 0))
         ) %>%
         mutate_all(~as.character(.)) %>%
         bind_rows(nhsss$prep$outcome.initial$check$tabstat)
   }
}

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
data_name <- "outcome.initial"
if (!is.empty(nhsss$prep[[data_name]]$check))
   .validation_gsheets(
      data_name   = data_name,
      parent_list = nhsss$prep[[data_name]]$check,
      drive_path  = paste0(nhsss$prep$gdrive$path$report, "Validation/"),
      surv_name   = "HARP Tx"
   )

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
