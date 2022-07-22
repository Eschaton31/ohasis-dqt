##  Filter Initial Data & Remove Already Reported ------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
.log_info("Generating `harp_tx`.`reg-initial`.")

# Form BC not yet reported
.log_info("Dropping already-reported records.")
nhsss$harp_tx$reg.initial$data <- nhsss$harp_tx$forms$art_first %>%
   anti_join(
      y  = nhsss$harp_tx$official$old_reg %>%
         select(CENTRAL_ID),
      by = "CENTRAL_ID"
   ) %>%
   filter(
      VISIT_DATE <= ohasis$next_date
   )

.log_info("Performing initial cleaning.")
nhsss$harp_tx$reg.initial$data %<>%
   mutate_at(
      .vars = vars(FIRST, MIDDLE, LAST, SUFFIX),
      ~toupper(.)
   ) %>%
   mutate(
      # date variables
      encoded_date       = as.Date(CREATED_AT),

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

##  Sort by earliest visit of client for the report ----------------------------

.log_info("Prioritizing reports.")
nhsss$harp_tx$reg.initial$data %<>%
   arrange(VISIT_DATE, desc(LATEST_NEXT_DATE), CENTRAL_ID) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   rename(
      ART_FACI     = SERVICE_FACI,
      ART_SUB_FACI = SERVICE_SUB_FACI,
   )

##  Adding CD4 results ---------------------------------------------------------

.log_info("Attaching baseline CD4 data.")
nhsss$harp_tx$reg.initial$data %<>%
   # get cd4 data
   # TODO: attach max dates for filtering of cd4 data
   left_join(
      y  = nhsss$harp_tx$forms$lab_cd4 %>%
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

##  Facilities -----------------------------------------------------------------

.log_info("Attaching facility names (OHASIS versions).")
local(envir = nhsss$harp_tx, {
   # record faci
   reg.initial$data <- ohasis$get_faci(
      reg.initial$data,
      list("FACI_CODE" = c("FACI_ID", "SUB_FACI_ID")),
      "code"
   )
   # art faci
   reg.initial$data <- ohasis$get_faci(
      reg.initial$data,
      list("ART_FACI_CODE" = c("ART_FACI", "ART_SUB_FACI")),
      "code",
      c("tx_reg", "tx_prov", "tx_munc")
   )
   # epic / gf faci
   reg.initial$data <- ohasis$get_faci(
      reg.initial$data,
      list("ACTUAL_FACI_CODE" = c("ACTUAL_FACI", "ACTUAL_SUB_FACI")),
      "code",
      c("real_reg", "real_prov", "real_munc")
   )
   # satellite
   reg.initial$data <- ohasis$get_faci(
      reg.initial$data,
      list("SATELLITE_FACI_CODE" = c("SATELLITE_FACI", "SATELLITE_SUB_FACI")),
      "code"
   )
   # satellite
   reg.initial$data <- ohasis$get_faci(
      reg.initial$data,
      list("TRANSIENT_FACI_CODE" = c("TRANSIENT_FACI", "TRANSIENT_SUB_FACI")),
      "code"
   )
})

# arrange via faci
nhsss$harp_tx$reg.initial$data %<>%
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

##  Flag data for validation ---------------------------------------------------

update <- input(
   prompt  = "Run `reg-initial` validations?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
update <- substr(toupper(update), 1, 1)

nhsss$harp_tx$reg.initial$check <- list()
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
      "FIRST",
      "MIDDLE",
      "LAST",
      "SUFFIX",
      "BIRTHDATE",
      "SEX",
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
      "BIRTHDATE",
      "LATEST_NEXT_DATE"
   )
   .log_info("Checking dates.")
   for (var in vars) {
      var                                    <- as.symbol(var)
      nhsss$harp_tx$reg.initial$check[[var]] <- nhsss$harp_tx$reg.initial$data %>%
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
         nhsss$harp_tx$reg.initial$check[[var]] <- nhsss$harp_tx$reg.initial$check[[var]] %>%
            filter(
               is.na(!!var) |
                  !!var <= -25567
            )
   }

   # non-negotiable variables
   vars <- c(
      "FORM_VERSION",
      "ART_FACI_CODE",
      "CURR_PSGC_REG",
      "CURR_PSGC_PROV",
      "CURR_PSGC_MUNC",
      "FIRST",
      "LAST",
      "CENTRAL_ID",
      "AGE",
      "SEX",
      "UIC",
      "MEDICINE_SUMMARY"
   )
   .log_info("Checking if non-negotiable variables are missing.")
   for (var in vars) {
      var                                    <- as.symbol(var)
      nhsss$harp_tx$reg.initial$check[[var]] <- nhsss$harp_tx$reg.initial$data %>%
         filter(
            is.na(!!var)
         ) %>%
         select(
            any_of(view_vars),
            !!var
         )
   }

   # special checks
   .log_info("Checking for new clients tagged as refills.")
   nhsss$harp_tx$reg.initial$check[["refill_enroll"]] <- nhsss$harp_tx$reg.initial$data %>%
      filter(
         StrLeft(TX_STATUS, 1) == "2"
      ) %>%
      select(
         any_of(view_vars),
         TX_STATUS,
         VISIT_TYPE
      )

   .log_info("Checking for mismatch dispensed and visit dates.")
   nhsss$harp_tx$reg.initial$check[["mismatch_disp"]] <- nhsss$harp_tx$reg.initial$data %>%
      filter(
         as.Date(DISP_DATE) != RECORD_DATE
      ) %>%
      select(
         any_of(view_vars),
         RECORD_DATE,
         DISP_DATE
      )

   .log_info("Checking for short names.")
   nhsss$harp_tx$reg.initial$check[["short_name"]] <- nhsss$harp_tx$reg.initial$data %>%
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
   nhsss$harp_tx$reg.initial$check[["non_enrollee"]] <- nhsss$harp_tx$reg.initial$data %>%
      filter(
         VISIT_DATE < ohasis$date
      ) %>%
      select(
         any_of(view_vars),
      )

   .log_info("Checking for mismatch record vs art faci.")
   nhsss$harp_tx$reg.initial$check[["mismatch_faci"]] <- nhsss$harp_tx$reg.initial$data %>%
      filter(
         FACI_CODE != ART_FACI_CODE
      ) %>%
      select(
         any_of(view_vars),
         FACI_CODE,
      )

   .log_info("Checking for possible PMTCT-N clients.")
   nhsss$harp_tx$reg.initial$check[["possible_pmtct"]] <- nhsss$harp_tx$reg.initial$data %>%
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
   nhsss$harp_tx$reg.initial$check[["possible_prep"]] <- nhsss$harp_tx$reg.initial$data %>%
      filter(
         stri_detect_fixed(MEDICINE_SUMMARY, "FTC")
      ) %>%
      select(
         any_of(view_vars),
         MEDICINE_SUMMARY,
      )

   .log_info("Checking for males tagged as pregnant.")
   nhsss$harp_tx$reg.initial$check[["pregnant_m"]] <- nhsss$harp_tx$reg.initial$data %>%
      filter(
         StrLeft(IS_PREGNANT, 1) == '1',
         StrLeft(SEX, 1) == '1'
      ) %>%
      select(
         any_of(view_vars),
         IS_PREGNANT,
      )

   .log_info("Checking for pregnant females.")
   nhsss$harp_tx$reg.initial$check[["pregnant_f"]] <- nhsss$harp_tx$reg.initial$data %>%
      filter(
         StrLeft(IS_PREGNANT, 1) == '1',
         StrLeft(SEX, 1) == '2'
      ) %>%
      select(
         any_of(view_vars),
         IS_PREGNANT,
      )

   .log_info("Checking calculated age vs computed age.")
   nhsss$harp_tx$reg.initial$check[["mismatch_age"]] <- nhsss$harp_tx$reg.initial$data %>%
      filter(
         AGE != AGE_DTA
      ) %>%
      select(
         any_of(view_vars),
         AGE,
         AGE_DTA
      )

   .log_info("Checking ART reports tagged as DOH-EB.")
   nhsss$harp_tx$reg.initial$check[["art_eb"]] <- nhsss$harp_tx$reg.initial$data %>%
      filter(
         ART_FACI_CODE == "DOH"
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
   nhsss$harp_tx$reg.initial$check$tabstat <- data.frame()
   for (var in vars) {
      var <- as.symbol(var)
      df  <- nhsss$harp_tx$reg.initial$data

      nhsss$harp_tx$reg.initial$check$tabstat <- df %>%
         summarise(
            VARIABLE = as.character(var),
            MIN      = suppress_warnings(min(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            MEDIAN   = suppress_warnings(median(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            MAX      = suppress_warnings(max(!!var, na.rm = TRUE), "returning [\\-]*Inf"),
            NAs      = sum(if_else(is.na(!!var), 1, 0, 0))
         ) %>%
         mutate_all(~as.character(.)) %>%
         bind_rows(nhsss$harp_tx$reg.initial$check$tabstat)
   }

   # Remove already tagged data from validation
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
            if (drop %in% names(nhsss$harp_tx$corr))
               for (check in names(nhsss$harp_tx$reg.initial$check)) {
                  if (check != "tabstat")
                     nhsss$harp_tx$reg.initial$check[[check]] %<>%
                        anti_join(
                           y  = nhsss$harp_tx$corr[[drop]],
                           by = "REC_ID"
                        )
               }
         }
      }
   }
}

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
data_name <- "reg.initial"
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
