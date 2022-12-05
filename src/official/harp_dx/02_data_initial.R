##  Filter Initial Data & Remove Already Reported ------------------------------

download_data <- function(path_to_sql) {
   lw_conn <- ohasis$conn("lw")

   # read queries
   sql           <- list()
   sql$form_a    <- read_file(file.path(path_to_sql, "form_a.sql"))
   sql$form_hts  <- read_file(file.path(path_to_sql, "form_hts.sql"))
   sql$form_cfbs <- read_file(file.path(path_to_sql, "form_cfbs.sql"))

   # read data
   data           <- list()
   data$form_a    <- tracked_select(lw_conn, sql$form_a, "New Form A")
   data$form_hts  <- tracked_select(lw_conn, sql$form_hts, "New HTS Form")
   data$form_cfbs <- tracked_select(lw_conn, sql$form_cfbs, "New CFBS Form")

   dbDisconnect(lw_conn)

   # standardize hts form
   hts_data <- process_hts(data$form_hts, data$form_a, data$form_cfbs)
   return(hts_data)
}

##  Initial Cleaning -----------------------------------------------------------
clean_data <- function(data, dup_munc) {
   data %<>%
      mutate_at(
         .vars = vars(FIRST, MIDDLE, LAST, SUFFIX),
         ~toupper(.)
      ) %>%
      mutate(
         # month of labcode/date received
         lab_month_1           = month(DATE_RECEIVE) %>% as.character(),
         lab_month_1           = stri_pad_left(lab_month_1, 2, "0"),
         lab_month_2           = substr(
            CONFIRM_CODE,
            stri_locate_first_fixed(CONFIRM_CODE, "-") + 1,
            stri_locate_first_fixed(CONFIRM_CODE, "-") + 2
         ),
         lab_month_2           = case_when(
            !stri_detect_fixed(CONFIRM_CODE, "-") ~ NA_character_,
            stri_detect_fixed(lab_month_2, "-") ~ NA_character_,
            TRUE ~ lab_month_2
         ),
         lab_month             = if_else(
            condition = is.na(DATE_RECEIVE),
            true      = lab_month_2,
            false     = lab_month_1
         ),

         # year of labcode/date received
         lab_year_1            = year(DATE_RECEIVE) %>% as.character(),
         lab_year_2            = case_when(
            StrLeft(CONFIRM_TYPE, 1) == "1" & stri_detect_fixed(CONFIRM_CODE, "-") ~ paste0("20", substr(CONFIRM_CODE, 2, 3)),
            StrLeft(CONFIRM_TYPE, 1) == "2" & stri_detect_fixed(CONFIRM_CODE, "-") ~ paste0("20", substr(CONFIRM_CODE, 4, 5)),
            TRUE ~ NA_character_
         ),
         lab_year              = if_else(
            condition = is.na(DATE_RECEIVE),
            true      = lab_year_2,
            false     = lab_year_1
         ),

         # date variables
         encoded_date          = as.Date(CREATED_AT),
         visit_date            = RECORD_DATE,
         blood_extract_date    = as.Date(DATE_COLLECT),
         specimen_receipt_date = as.Date(DATE_RECEIVE),
         confirm_date          = as.Date(DATE_CONFIRM),

         # date var for keeping
         report_date           = if_else(
            condition = !is.na(lab_year) & !is.na(lab_month),
            true      = paste(sep = "-", lab_year, lab_month, "01"),
            false     = NA_character_
         ),
         report_date           = as.Date(report_date),
         yr_rec                = year(RECORD_DATE),

         # name
         name                  = paste0(
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
         name                  = str_squish(name),
         STANDARD_FIRST        = stri_trans_general(FIRST, "latin-ascii"),

         # Permanent
         use_curr              = if_else(
            condition = (is.na(PERM_PSGC_MUNC) & !is.na(CURR_PSGC_MUNC)) | StrLeft(PERM_PSGC_MUNC, 2) == '99',
            true      = 1,
            false     = 0
         ),
         PERM_PSGC_REG         = if_else(
            condition = use_curr == 1,
            true      = CURR_PSGC_REG,
            false     = PERM_PSGC_REG
         ),
         PERM_PSGC_PROV        = if_else(
            condition = use_curr == 1,
            true      = CURR_PSGC_PROV,
            false     = PERM_PSGC_PROV
         ),
         PERM_PSGC_MUNC        = if_else(
            condition = use_curr == 1,
            true      = CURR_PSGC_MUNC,
            false     = PERM_PSGC_MUNC
         ),

         # Age
         AGE                   = if_else(
            condition = is.na(AGE) & !is.na(AGE_MO),
            true      = AGE_MO / 12,
            false     = as.double(AGE)
         ),
         AGE_DTA               = if_else(
            condition = !is.na(BIRTHDATE),
            true      = floor((visit_date - BIRTHDATE) / 365.25) %>% as.numeric(),
            false     = as.numeric(NA)
         ),
      ) %>%
      left_join(
         y  = dup_munc %>%
            select(
               PERM_PSGC_MUNC = PSGC_MUNC,
               DUP_MUNC
            ),
         by = "PERM_PSGC_MUNC"
      )

   return(data)
}

##  Sorting confirmatory results -----------------------------------------------

prioritize_reports <- function(data) {
   data %<>%
      arrange(lab_year, lab_month, desc(CONFIRM_TYPE), visit_date, confirm_date) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      filter(report_date < ohasis$next_date | is.na(report_date)) %>%
      rename(
         TEST_FACI     = SERVICE_FACI,
         TEST_SUB_FACI = SERVICE_SUB_FACI,
      )
   return(data)
}

##  Adding CD4 results ---------------------------------------------------------

get_cd4 <- function(data, path_to_sql, ceiling_date) {
   lw_conn <- ohasis$conn("lw")

   # read queries
   cd4_sql  <- read_file(file.path(path_to_sql, "lab_cd4.sql"))
   cd4_data <- tracked_select(lw_conn, cd4_sql, "Baseline CD4", list(as.character(ceiling_date)))

   dbDisconnect(lw_conn)

   data %<>%
      # get cd4 data
      # TODO: attach max dates for filtering of cd4 data
      left_join(
         y  = cd4_data,
         by = 'CENTRAL_ID'
      ) %>%
      mutate(
         # calculate distance from confirmatory date
         CD4_DATE     = as.Date(CD4_DATE),
         CD4_CONFIRM  = difftime(as.Date(confirm_date), CD4_DATE, units = "days") %>% as.numeric(),

         # baseline is within 182 days
         BASELINE_CD4 = if_else(
            CD4_CONFIRM >= -182 & CD4_CONFIRM <= 182,
            1,
            0
         ),

         # make values absolute to take date nearest to confirmatory
         CD4_CONFIRM  = abs(CD4_CONFIRM %>% as.numeric()),
      ) %>%
      arrange(REC_ID, CD4_CONFIRM) %>%
      distinct(REC_ID, .keep_all = TRUE) %>%
      arrange(desc(CONFIRM_TYPE), IDNUM, CONFIRM_CODE)

   return(data)
}

##  Facilities -----------------------------------------------------------------

attach_faci_names <- function(data) {
   faci_ids <- list(
      c("FACI_ID", "SUB_FACI_ID", "FACI_NAME"),
      c("TEST_FACI", "TEST_SUB_FACI", "TEST_FACI_NAME"),
      c("SPECIMEN_SOURCE", "SPECIMEN_SUB_SOURCE", "SOURCE_FACI")
   )

   for (i in seq_len(length(faci_ids))) {
      faci_id     <- faci_ids[[i]][1] %>% as.symbol()
      faci_name   <- faci_ids[[i]][3] %>% as.symbol()
      sub_faci_id <- faci_ids[[i]][2] %>% as.symbol()

      # rename columns
      data %<>%
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
   return(data)
}

##  Flag data for validation ---------------------------------------------------

get_checks <- function(data) {
   check  <- list()
   update <- input(
      prompt  = "Run `initial` validations?",
      options = c("1" = "yes", "2" = "no"),
      default = "1"
   )

   if (update == "1") {
      # initialize checking layer
      view_vars <- c(
         "REC_ID",
         "PATIENT_ID",
         "FORM_VERSION",
         "CONFIRM_CODE",
         "UIC",
         "PATIENT_CODE",
         "FIRST",
         "MIDDLE",
         "LAST",
         "SUFFIX",
         "BIRTHDATE",
         "SEX",
         "TEST_FACI_NAME",
         "CONFIRM_TYPE",
         "SPECIMEN_REFER_TYPE",
         get_names(data, "risk_")
      )
      check     <- check_pii(data, check, view_vars)
      check     <- check_addr(data, check, view_vars, "curr")
      check     <- check_addr(data, check, view_vars, "perm")

      # dates
      date_vars <- c(
         "encoded_date",
         "report_date",
         "visit_date",
         "blood_extract_date",
         "specimen_receipt_date",
         "confirm_date",
         "BIRTHDATE"
      )
      check     <- check_dates(data, check, view_vars, date_vars)

      # non-negotiable variables
      nonnegotiables <- c(
         "FORM_VERSION",
         "TEST_FACI",
         "SPECIMEN_REFER_TYPE",
         "CENTRAL_ID",
         "AGE",
         "SELF_IDENT",
         "NATIONALITY",
         "CONFIRM_CODE",
         "T1_DATE",
         "T1_RESULT",
         "T2_DATE",
         "T2_RESULT",
         "T3_DATE",
         "T3_RESULT"
      )
      check <- check_nonnegotiables(data, check, view_vars, nonnegotiables)
      check <- check_preggy(data, check, view_vars)
      check <- check_age(data, check, view_vars)

      # special checks
      .log_info("Checking for mismatch facilities (source != test).")
      check[["SOURCE_FACI"]] <- data %>%
         mutate(
            CHECK = case_when(
               (StrLeft(SOURCE_FACI, 6) != StrLeft(TEST_FACI_NAME, 6)) ~ 1,
               (is.na(SPECIMEN_REFER_TYPE) | StrLeft(SPECIMEN_REFER_TYPE, 1) == "4") & is.na(SOURCE_FACI) ~ 1
            )
         ) %>%
         filter(CHECK == 1) %>%
         select(
            any_of(view_vars),
            SOURCE_FACI
         )

      .log_info("Checking for similarly named municipalities.")
      check[["dup_munc"]] <- data %>%
         filter(
            DUP_MUNC == 1
         ) %>%
         mutate_at(
            .vars = vars(contains("_PSGC_")),
            ~if_else(is.na(.), "", .)
         ) %>%
         # permanent
         rename_at(
            .vars = vars(starts_with("PERM_PSGC")),
            ~stri_replace_all_fixed(., "PERM_", "")
         ) %>%
         left_join(
            y  = ohasis$ref_addr %>%
               select(
                  PSGC_REG,
                  PSGC_PROV,
                  PSGC_MUNC,
                  NAME_REG,
                  NAME_PROV,
                  NAME_MUNC,
               ),
            by = c("PSGC_REG", "PSGC_PROV", "PSGC_MUNC")
         ) %>%
         select(
            any_of(view_vars),
            NAME_REG,
            NAME_PROV,
            NAME_MUNC,
         )

      .log_info("Checking NRL-SACCL reports not assigned to DOH-EB.")
      check[["saccl_not_eb"]] <- data %>%
         filter(
            StrLeft(CONFIRM_TYPE, 1) == "1",
            FACI_ID != "130000"
         ) %>%
         select(
            any_of(view_vars),
            FACI_NAME
         )

      # test kits
      .log_info("Checking invalid test kits.")
      check[["T1_KIT"]] <- data %>%
         filter(
            !(T1_KIT %in% c("SD Bioline HIV 1/2 3.0", "Abbott Bioline HIV 1/2 3.0", "SYSMEX HISCL HIV Ag + Ab Assay")) | is.na(T1_KIT)
         ) %>%
         select(
            any_of(view_vars),
            T1_KIT
         )
      check[["T2_KIT"]] <- data %>%
         filter(
            !(T2_KIT %in% c("Alere Determine HIV-1/2", "Abbott Determine HIV 1/2", "VIDAS HIV DUO Ultra")) | is.na(T2_KIT)
         ) %>%
         select(
            any_of(view_vars),
            T2_KIT
         )
      check[["T3_KIT"]] <- data %>%
         filter(
            !(T3_KIT %in% c("Geenius HIV 1/2 Confirmatory Assay", "HIV 1/2 STAT-PAK Assay")) | is.na(T3_KIT)
         ) %>%
         select(
            any_of(view_vars),
            T3_KIT
         )

      # range-median
      tabstat <- c(
         "encoded_date",
         "visit_date",
         "blood_extract_date",
         "specimen_receipt_date",
         "confirm_date",
         "T1_DATE",
         "T2_DATE",
         "T3_DATE",
         "EXPOSE_SEX_M_AV_DATE",
         "EXPOSE_SEX_M_AV_NOCONDOM_DATE",
         "EXPOSE_SEX_F_AV_DATE",
         "EXPOSE_SEX_F_AV_NOCONDOM_DATE",
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
      data <- download_data(wd) %>%
         clean_data(corr$dup_munc) %>%
         prioritize_reports() %>%
         get_cd4(wd, ohasis$next_date) %>%
         attach_faci_names()

      write_rds(data, file.path(wd, "initial.RDS"))

      check <- get_checks(data)
   })

   local(envir = .GlobalEnv, flow_validation(nhsss$harp_dx, "initial", ohasis$ym))
}