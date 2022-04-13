##  Filter Initial Data & Remove Already Reported ------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
.log_info("Generating `harp_tx`.`reg-initial`.")
.log_info("Opening connections.")
lw_conn <- ohasis$conn("lw")
db_conn <- ohasis$conn("db")

# Form BC not yet reported
.log_info("Dropping already-reported records.")
nhsss$harp_tx$reg.initial$data <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "art_first")) %>%
   select(CENTRAL_ID, REC_ID) %>%
   anti_join(
      y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "harp_tx_old")),
      by = "CENTRAL_ID"
   ) %>%
   left_join(
      y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_art_bc")),
      by = "REC_ID"
   ) %>%
   collect()

.log_info("Performing initial cleaning.")
nhsss$harp_tx$reg.initial$data %<>%
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
      AGE             = if_else(
         condition = is.na(AGE) & !is.na(AGE_MO),
         true      = AGE_MO / 12,
         false     = as.double(AGE)
      ),
      AGE_DTA         = if_else(
         condition = !is.na(BIRTHDATE),
         true      = floor((VISIT_DATE - BIRTHDATE) / 365.25) %>% as.numeric(),
         false     = as.numeric(NA)
      ),

      # tag those without ART_FACI
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
         FACI_ID == "130748" ~ 1,
         SERVICE_FACI == "130748" ~ 1,
         TRUE ~ 0
      ),
      SERVICE_FACI    = if_else(
         condition = sail_clinic == 1,
         true      = "130025",
         false     = SERVICE_FACI
      ),

      # tag tly clinic
      tly_clinic      = case_when(
         FACI_ID == "070021" ~ 1,
         SERVICE_FACI == "070021" ~ 1,
         TRUE ~ 0
      ),
      SERVICE_FACI    = if_else(
         condition = tly_clinic == 1,
         true      = "130001",
         false     = SERVICE_FACI
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
ceiling_date <- ohasis$next_date
nhsss$harp_tx$reg.initial$data %<>%
   # get cd4 data
   # TODO: attach max dates for filtering of cd4 data
   left_join(
      y  = tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "lab_cd4")) %>%
         filter(
            is.na(DELETED_AT),
            as.Date(CD4_DATE) < ceiling_date
         ) %>%
         left_join(
            y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "id_registry")) %>%
               select(CENTRAL_ID, PATIENT_ID),
            by = 'PATIENT_ID'
         ) %>%
         collect() %>%
         mutate(
            CENTRAL_ID = if_else(
               condition = is.na(CENTRAL_ID),
               true      = PATIENT_ID,
               false     = CENTRAL_ID
            ),
         ) %>%
         select(-PATIENT_ID, -REC_ID),
      by = 'CENTRAL_ID'
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

.log_info("Closing connections.")
dbDisconnect(lw_conn)
dbDisconnect(db_conn)

##  Facilities -----------------------------------------------------------------

.log_info("Attaching facility names (OHASIS versions).")
faci_ids <- list(
   c("FACI_ID", "SUB_FACI_ID", "FACI_CODE"),
   c("ART_FACI", "ART_SUB_FACI", "ART_FACI_CODE")
)

for (i in seq_len(length(faci_ids))) {
   faci_id     <- faci_ids[[i]][1] %>% as.symbol()
   faci_name   <- faci_ids[[i]][3] %>% as.symbol()
   sub_faci_id <- faci_ids[[i]][2] %>% as.symbol()

   # rename columns
   nhsss$harp_tx$reg.initial$data %<>%
      # clean variables first
      mutate(
         !!faci_id     := if_else(
            condition = is.na(!!faci_id),
            true      = "",
            false     = !!faci_id
         ),
         !!sub_faci_id := case_when(
            is.na(!!sub_faci_id) ~ "",
            StrLeft(!!sub_faci_id, 6) != !!faci_id ~ "",
            TRUE ~ !!sub_faci_id
         )
      ) %>%
      # get referenced data
      left_join(
         y  = ohasis$ref_faci %>%
            select(
               !!faci_id     := FACI_ID,
               !!sub_faci_id := SUB_FACI_ID,
               !!faci_name   := FACI_CODE
            ),
         by = c(as.character(faci_id), as.character(sub_faci_id))
      ) %>%
      # move then rename to old version
      relocate(!!faci_name, .after = !!sub_faci_id)
}

# arrange via faci
nhsss$harp_tx$reg.initial$data %<>%
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
      "VISIT_DATE"
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
               !!var <= as.Date("1900-01-01")
         ) %>%
         select(
            any_of(view_vars),
            !!var
         )

      if (as.character(var) %in% c("LATEST_NEXT_DATE", "encoded_date"))
         nhsss$harp_tx$reg.initial$check[[var]] <- nhsss$harp_tx$reg.initial$check[[var]] %>%
            filter(
               is.na(!!var) |
                  !!var <= as.Date("1900-01-01")
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
      "CONFIRMATORY_CODE",
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
            MIN      = min(!!var, na.rm = TRUE),
            MEDIAN   = median(!!var, na.rm = TRUE),
            MAX      = max(!!var, na.rm = TRUE),
            NAs      = sum(if_else(is.na(!!var), 1, 0, 0))
         ) %>%
         mutate_all(~as.character(.)) %>%
         bind_rows(nhsss$harp_tx$reg.initial$check$tabstat)
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
