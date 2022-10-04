##  Filter Initial Data & Remove Already Reported ------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
.log_info("Generating `harp_dead`.`initial`.")
.log_info("Opening connections.")
lw_conn <- ohasis$conn("lw")
db_conn <- ohasis$conn("db")

# Form A + HTS Forms
.log_info("Dropping already-reported records.")
nhsss$harp_dead$initial$data <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_d")) %>%
   # get latest central ids
   left_join(
      y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "id_registry")) %>%
         select(CENTRAL_ID, PATIENT_ID),
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   ) %>%
   # # remove data already reported in previous registry
   # anti_join(
   #    y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "harp_dead_old")),
   #    by = "CENTRAL_ID"
   # ) %>%
   collect() %>%
   # keep only patients not in registry
   anti_join(
      y  = nhsss$harp_dead$official$old %>%
         select(CENTRAL_ID),
      by = "CENTRAL_ID"
   )

.log_info("Performing initial cleaning.")
nhsss$harp_dead$initial$data %<>%
   mutate_at(
      .vars = vars(FIRST, MIDDLE, LAST, SUFFIX),
      ~toupper(.)
   ) %>%
   mutate(
      # date variables
      encoded_date   = as.Date(CREATED_AT),
      report_date    = RECORD_DATE,

      # name
      fullname       = paste0(
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
      fullname       = str_squish(fullname),

      # Permanent
      use_curr       = if_else(
         condition = (is.na(PERM_PSGC_MUNC) & !is.na(CURR_PSGC_MUNC)) | StrLeft(PERM_PSGC_MUNC, 2) == '99',
         true      = 1,
         false     = 0
      ),
      PERM_PSGC_REG  = if_else(
         condition = use_curr == 1,
         true      = CURR_PSGC_REG,
         false     = PERM_PSGC_REG
      ),
      PERM_PSGC_PROV = if_else(
         condition = use_curr == 1,
         true      = CURR_PSGC_PROV,
         false     = PERM_PSGC_PROV
      ),
      PERM_PSGC_MUNC = if_else(
         condition = use_curr == 1,
         true      = CURR_PSGC_MUNC,
         false     = PERM_PSGC_MUNC
      ),

      # Age
      AGE            = if_else(
         condition = is.na(AGE) & !is.na(AGE_MO),
         true      = AGE_MO / 12,
         false     = as.double(AGE)
      ),
      AGE_DTA        = case_when(
         !is.na(BIRTHDATE) & !is.na(DEATH_DATE) ~ floor((DEATH_DATE - BIRTHDATE) / 365.25),
         !is.na(BIRTHDATE) & is.na(DEATH_DATE) ~ floor((report_date - BIRTHDATE) / 365.25),
      ) %>% as.numeric(),

      # tag wrong reports
      not_dead       = if_else(
         condition = StrLeft(EB_VALIDATED, 1) == "0",
         true      = 1,
         false     = 0,
         missing   = 0
      )
   )

##  Getting summary of initial unfiltered reports ------------------------------

.log_info("Getting summary of reports before prioritization and dropping.")
nhsss$harp_dead$initial$raw_data <- nhsss$harp_dead$initial$data

##  Sorting confirmatory results -----------------------------------------------

.log_info("Prioritizing reports.")
nhsss$harp_dead$initial$data %<>%
   # remove invalid reports
   filter(not_dead == 0) %>%
   # prioritize form d over form bc
   arrange(desc(REPORTING_FORM), report_date, DEATH_DATE) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   filter(report_date < ohasis$next_date |
             DEATH_DATE < ohasis$next_date |
             is.na(report_date)) %>%
   rename(
      MORT_FACI     = SERVICE_FACI,
      MORT_SUB_FACI = SERVICE_SUB_FACI,
   )

.log_info("Closing connections.")
dbDisconnect(lw_conn)
dbDisconnect(db_conn)

##  Facilities -----------------------------------------------------------------

.log_info("Attaching facility names (OHASIS versions).")
faci_ids <- list(
   c("FACI_ID", "SUB_FACI_ID", "FACI_NAME"),
   c("MORT_FACI", "MORT_SUB_FACI", "MORT_FACI_NAME")
)

for (i in seq_len(length(faci_ids))) {
   faci_id     <- faci_ids[[i]][1] %>% as.symbol()
   faci_name   <- faci_ids[[i]][3] %>% as.symbol()
   sub_faci_id <- faci_ids[[i]][2] %>% as.symbol()

   # rename columns
   nhsss$harp_dead$initial$data %<>%
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
               !!faci_name   := FACI_LABEL
            ),
         by = c(as.character(faci_id), as.character(sub_faci_id))
      ) %>%
      # move then rename to old version
      relocate(!!faci_name, .after = !!sub_faci_id)
}

##  Flag data for validation ---------------------------------------------------

update <- input(
   prompt  = "Run `initial` validations?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
update <- substr(toupper(update), 1, 1)

nhsss$harp_dead$initial$check <- list()
if (update == "1") {
   # initialize checking layer
   view_vars <- c(
      "REC_ID",
      "PATIENT_ID",
      "REPORTING_FORM",
      "CONFIRMATORY_CODE",
      "UIC",
      "PATIENT_CODE",
      "FIRST",
      "MIDDLE",
      "LAST",
      "SUFFIX",
      "BIRTHDATE",
      "SEX",
      "MORT_FACI_NAME",
      "VISIT_DATE",
      "DEATH_DATE",
      "IMMEDIATE_CAUSES",
      "ANTECEDENT_CAUSES",
      "UNDERLYING_CAUSES"
   )
   # dates
   vars      <- c(
      "encoded_date",
      "report_date",
      "DEATH_DATE",
      "BIRTHDATE"
   )
   .log_info("Checking dates.")
   for (var in vars) {
      var                                  <- as.symbol(var)
      nhsss$harp_dead$initial$check[[var]] <- nhsss$harp_dead$initial$data %>%
         mutate_if(
            .predicate = is.character,
            ~str_squish(.) %>% if_else(. == "", NA_character_, .)
         ) %>%
         filter(
            is.na(!!var) |
               !!var >= ohasis$next_date |
               !!var <= -25567
         ) %>%
         select(
            any_of(view_vars),
            !!var
         )
   }

   # non-negotiable variables
   vars <- c(
      "FORM_VERSION",
      "MORT_FACI",
      "PERM_PSGC_REG",
      "PERM_PSGC_PROV",
      "PERM_PSGC_MUNC",
      "DEATH_PSGC_REG",
      "DEATH_PSGC_PROV",
      "DEATH_PSGC_MUNC",
      "FIRST",
      "LAST",
      "CENTRAL_ID",
      "AGE",
      "SEX",
      "DISEASE_HIV"
   )
   .log_info("Checking if non-negotiable variables are missing.")
   for (var in vars) {
      var                                  <- as.symbol(var)
      nhsss$harp_dead$initial$check[[var]] <- nhsss$harp_dead$initial$data %>%
         filter(
            is.na(!!var)
         ) %>%
         select(
            any_of(view_vars),
            !!var
         )

      if (as.character(var) %in% c("IDNUM", "CONFIRM_CODE"))
         nhsss$harp_dead$initial$check[[var]] <- nhsss$harp_dead$initial$check[[var]] %>%
            filter(StrLeft(CONFIRM_TYPE, 1) == "1")

      if (as.character(var) %in% c("T1_DATE", "T2_DATE", "T3_DATE", "SPECIMEN_REFER_TYPE"))
         nhsss$harp_dead$initial$check[[var]] <- nhsss$harp_dead$initial$check[[var]] %>%
            filter(StrLeft(CONFIRM_TYPE, 1) == "2")
   }

   # special checks
   .log_info("Checking for data w/o cause of death.")
   nhsss$harp_dead$initial$check[["no_cause"]] <- nhsss$harp_dead$initial$data %>%
      filter(
         is.na(IMMEDIATE_CAUSES),
         is.na(ANTECEDENT_CAUSES),
         is.na(UNDERLYING_CAUSES),
      ) %>%
      select(
         any_of(view_vars),
      )

   .log_info("Checking for death report from Form BC.")
   nhsss$harp_dead$initial$check[["formbc_dead"]] <- nhsss$harp_dead$initial$data %>%
      filter(
         REPORTING_FORM == "Form BC"
      ) %>%
      select(
         any_of(view_vars),
      )

   .log_info("Checking for death reports that are still for investigation.")
   nhsss$harp_dead$initial$check[["confirm_if_dead"]] <- nhsss$harp_dead$initial$data %>%
      filter(
         StrLeft(EB_VALIDATED, 1) == "3" | is.na(EB_VALIDATED)
      ) %>%
      select(
         any_of(view_vars),
      )

   .log_info("Checking for short names.")
   nhsss$harp_dead$initial$check[["short_name"]] <- nhsss$harp_dead$initial$data %>%
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

   .log_info("Checking calculated age vs computed age.")
   nhsss$harp_dead$initial$check[["mismatch_age"]] <- nhsss$harp_dead$initial$data %>%
      filter(
         AGE != AGE_DTA
      ) %>%
      select(
         any_of(view_vars),
         AGE,
         AGE_DTA
      )

   # range-median
   vars <- c(
      "encoded_date",
      "report_date",
      "DEATH_DATE",
      "BIRTHDATE",
      "AGE",
      "AGE_MO"
   )
   .log_info("Checking range-median of data.")
   nhsss$harp_dead$initial$check$tabstat <- data.frame()
   for (var in vars) {
      var <- as.symbol(var)
      df  <- nhsss$harp_dead$initial$data

      nhsss$harp_dead$initial$check$tabstat <- df %>%
         summarise(
            VARIABLE = as.character(var),
            MIN      = min(!!var, na.rm = TRUE),
            MEDIAN   = median(!!var, na.rm = TRUE),
            MAX      = max(!!var, na.rm = TRUE),
            NAs      = sum(if_else(is.na(!!var), 1, 0, 0))
         ) %>%
         mutate_all(~as.character(.)) %>%
         bind_rows(nhsss$harp_dead$initial$check$tabstat)
   }
}

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
gdrive_validation(nhsss$harp_dead, "initial", ohasis$ym)

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
