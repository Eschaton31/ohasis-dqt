##  Append to the previous art registry ----------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
.log_info("Generating `harp_tx`.`outcome.initial`.")

# get latest visits
.log_info("Getting latest visits from OHASIS.")
nhsss$harp_tx$outcome.initial$data <- nhsss$harp_tx$forms$art_last %>%
   arrange(desc(LATEST_NEXT_DATE)) %>%
   filter(VISIT_DATE <= ohasis$next_date) %>%
   mutate(LATEST_VISIT = VISIT_DATE) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE)

.log_info("Performing initial cleaning.")
nhsss$harp_tx$outcome.initial$data %<>%
   right_join(
      y  = nhsss$harp_tx$official$new_reg %>%
         select(-REC_ID, -PATIENT_ID),
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

##  Sort by earliest visit of client for the report ----------------------------

.log_info("Prioritizing reports.")
nhsss$harp_tx$outcome.initial$data %<>%
   arrange(desc(VISIT_DATE), desc(LATEST_NEXT_DATE), CENTRAL_ID) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   rename(
      ART_FACI     = SERVICE_FACI,
      ART_SUB_FACI = SERVICE_SUB_FACI,
   )

##  Facilities -----------------------------------------------------------------

.log_info("Attaching facility names (OHASIS versions).")
local(envir = nhsss$harp_tx, {
   # record faci
   outcome.initial$data <- ohasis$get_faci(
      outcome.initial$data,
      list("FACI_CODE" = c("FACI_ID", "SUB_FACI_ID")),
      "code"
   )
   # art faci
   outcome.initial$data <- ohasis$get_faci(
      outcome.initial$data,
      list("ART_FACI_CODE" = c("ART_FACI", "ART_SUB_FACI")),
      "code",
      c("tx_reg", "tx_prov", "tx_munc")
   )
   # epic / gf faci
   outcome.initial$data <- ohasis$get_faci(
      outcome.initial$data,
      list("ACTUAL_FACI_CODE" = c("ACTUAL_FACI", "ACTUAL_SUB_FACI")),
      "code",
      c("real_reg", "real_prov", "real_munc")
   )
   # satellite
   outcome.initial$data <- ohasis$get_faci(
      outcome.initial$data,
      list("SATELLITE_FACI_CODE" = c("SATELLITE_FACI", "SATELLITE_SUB_FACI")),
      "code"
   )
   # satellite
   outcome.initial$data <- ohasis$get_faci(
      outcome.initial$data,
      list("TRANSIENT_FACI_CODE" = c("TRANSIENT_FACI", "TRANSIENT_SUB_FACI")),
      "code"
   )
})

# arrange via faci
nhsss$harp_tx$outcome.initial$data %<>%
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

##  Flag data for validation ---------------------------------------------------

update <- input(
   prompt  = "Run `outcome.initial` validations?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
update <- substr(toupper(update), 1, 1)

nhsss$harp_tx$outcome.initial$check <- list()
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
      "ART_FACI_CODE",
      "ART_BRANCH",
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
      nhsss$harp_tx$outcome.initial$check[[var]] <- nhsss$harp_tx$outcome.initial$data %>%
         filter(
            is.na(!!var) |
               !!var >= ohasis$next_date |
               !!var <= -25567
         ) %>%
         select(
            any_of(view_vars),
            !!var
         )

      if (as.character(var) %in% c("LATEST_NEXT_DATE", "encoded_date"))
         nhsss$harp_tx$outcome.initial$check[[var]] <- nhsss$harp_tx$outcome.initial$check[[var]] %>%
            filter(
               is.na(!!var) |
                  !!var <= -25567
            )
   }

   # non-negotiable variables
   vars <- c(
      "ART_FACI_CODE",
      "MEDICINE_SUMMARY"
   )
   .log_info("Checking if non-negotiable variables are missing.")
   for (var in vars) {
      var                                        <- as.symbol(var)
      nhsss$harp_tx$outcome.initial$check[[var]] <- nhsss$harp_tx$outcome.initial$data %>%
         filter(
            is.na(!!var)
         ) %>%
         select(
            any_of(view_vars),
            !!var
         )

      if (as.character(var) == "FORM_VERSION")
         nhsss$harp_tx$outcome.initial$check[[var]] <- nhsss$harp_tx$outcome.initial$check[[var]] %>%
            filter(year(VISIT_DATE) >= 2021)
   }

   .log_info("Checking for dispensing later than next pick-up.")
   nhsss$harp_tx$outcome.initial$check[["disp_>_next"]] <- nhsss$harp_tx$outcome.initial$data %>%
      filter(
         VISIT_DATE > LATEST_NEXT_DATE
      ) %>%
      select(
         any_of(view_vars),
         LATEST_NEXT_DATE
      )

   .log_info("Checking for mismatch record vs art faci.")
   nhsss$harp_tx$outcome.initial$check[["mismatch_faci"]] <- nhsss$harp_tx$outcome.initial$data %>%
      filter(
         FACI_CODE != ART_FACI_CODE,
         sail_clinic != 1
      ) %>%
      select(
         any_of(view_vars),
      )

   .log_info("Checking for mismatch visit vs disp date.")
   nhsss$harp_tx$outcome.initial$check[["mismatch_disp"]] <- nhsss$harp_tx$outcome.initial$data %>%
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

   .log_info("Checking for possible PMTCT-N clients.")
   nhsss$harp_tx$outcome.initial$check[["possible_pmtct"]] <- nhsss$harp_tx$outcome.initial$data %>%
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
   nhsss$harp_tx$outcome.initial$check[["possible_prep"]] <- nhsss$harp_tx$outcome.initial$data %>%
      filter(
         stri_detect_fixed(MEDICINE_SUMMARY, "FTC")
      ) %>%
      select(
         any_of(view_vars),
         MEDICINE_SUMMARY,
      )

   .log_info("Checking for males tagged as pregnant.")
   nhsss$harp_tx$outcome.initial$check[["pregnant_m"]] <- nhsss$harp_tx$outcome.initial$data %>%
      filter(
         StrLeft(IS_PREGNANT, 1) == '1',
         StrLeft(sex, 1) == 'M'
      ) %>%
      select(
         any_of(view_vars),
         IS_PREGNANT,
      )

   .log_info("Checking for pregnant females.")
   nhsss$harp_tx$outcome.initial$check[["pregnant_f"]] <- nhsss$harp_tx$outcome.initial$data %>%
      filter(
         StrLeft(IS_PREGNANT, 1) == '1',
         StrLeft(sex, 1) == 'F'
      ) %>%
      select(
         any_of(view_vars),
         IS_PREGNANT,
      )

   .log_info("Checking calculated age vs computed age.")
   nhsss$harp_tx$outcome.initial$check[["mismatch_age"]] <- nhsss$harp_tx$outcome.initial$data %>%
      filter(
         AGE != AGE_DTA
      ) %>%
      select(
         any_of(view_vars),
         AGE,
         AGE_DTA
      )

   .log_info("Checking ART reports tagged as DOH-EB.")
   nhsss$harp_tx$outcome.initial$check[["art_eb"]] <- nhsss$harp_tx$outcome.initial$data %>%
      filter(
         ART_FACI_CODE == "DOH"
      ) %>%
      select(
         any_of(view_vars),
      )

   # range-median
   .log_info("Checking range-median of data.")
   nhsss$harp_tx$outcome.initial$check$tabstat <- nhsss$harp_tx$outcome.initial$data %>%
      tabstat(
         encoded_date,
         VISIT_DATE,
         DISP_DATE,
         LATEST_NEXT_DATE,
         BIRTHDATE,
         AGE,
         AGE_MO
      )
}

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
gdrive_validation(nhsss$harp_tx, "outcome.initial", ohasis$ym)

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
