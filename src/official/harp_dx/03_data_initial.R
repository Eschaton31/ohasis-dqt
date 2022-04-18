##  Filter Initial Data & Remove Already Reported ------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# open connections
.log_info("Generating `harp_dx`.`initial`.")
.log_info("Opening connections.")
lw_conn <- ohasis$conn("lw")
db_conn <- ohasis$conn("db")

# Form A + HTS Forms
.log_info("Dropping already-reported records.")
nhsss$harp_dx$initial$data <- tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_a")) %>%
   union_all(tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_hts"))) %>%
   # keep only positive results
   filter(substr(CONFIRM_RESULT, 1, 1) == "1") %>%
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
   # remove data already reported in previous registry
   anti_join(
      y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "harp_dx_old")),
      by = "REC_ID"
   ) %>%
   collect() %>%
   # keep only patients not in registry
   anti_join(
      y  = nhsss$harp_dx$official$old %>%
         select(CENTRAL_ID),
      by = "CENTRAL_ID"
   )

.log_info("Performing initial cleaning.")
nhsss$harp_dx$initial$data %<>%
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
      y  = nhsss$harp_dx$corr$dup_munc %>%
         select(
            PERM_PSGC_MUNC = PSGC_MUNC,
            DUP_MUNC
         ),
      by = "PERM_PSGC_MUNC"
   )

##  Getting summary of initial unfiltered reports ------------------------------

.log_info("Getting summary of reports before prioritization and dropping.")
nhsss$harp_dx$initial$raw_data       <- nhsss$harp_dx$initial$data
nhsss$harp_dx$initial$report_summary <- nhsss$harp_dx$initial$data %>%
   # confirmlab
   mutate(
      CONFIRM_FACI     = if_else(
         condition = is.na(CONFIRM_FACI),
         true      = "",
         false     = CONFIRM_FACI
      ),
      CONFIRM_SUB_FACI = case_when(
         is.na(CONFIRM_SUB_FACI) ~ "",
         StrLeft(CONFIRM_SUB_FACI, 6) != CONFIRM_FACI ~ "",
         TRUE ~ CONFIRM_SUB_FACI
      )
   ) %>%
   left_join(
      na_matches = "never",
      y          = nhsss$harp_dx$corr$confirmlab %>%
         mutate(
            FACI_ID     = if_else(
               condition = is.na(FACI_ID),
               true      = "",
               false     = FACI_ID
            ),
            SUB_FACI_ID = case_when(
               is.na(SUB_FACI_ID) ~ "",
               StrLeft(SUB_FACI_ID, 6) != FACI_ID ~ "",
               TRUE ~ SUB_FACI_ID
            )
         ) %>%
         rename(
            CONFIRM_FACI     = FACI_ID,
            CONFIRM_SUB_FACI = SUB_FACI_ID,
         ),
      by         = c("CONFIRM_FACI", "CONFIRM_SUB_FACI")
   ) %>%
   group_by(CONFIRM_FACI, confirmlab) %>%
   summarize(
      reports = n()
   )

##  Sorting confirmatory results -----------------------------------------------

.log_info("Prioritizing reports.")
nhsss$harp_dx$initial$data %<>%
   arrange(lab_year, lab_month, desc(CONFIRM_TYPE), visit_date, confirm_date) %>%
   distinct(CENTRAL_ID, .keep_all = TRUE) %>%
   filter(report_date < ohasis$next_date | is.na(report_date)) %>%
   rename(
      TEST_FACI     = SERVICE_FACI,
      TEST_SUB_FACI = SERVICE_SUB_FACI,
   )

##  Adding CD4 results ---------------------------------------------------------

.log_info("Attaching baseline CD4 data.")
ceiling_date <- ohasis$next_date
nhsss$harp_dx$initial$data %<>%
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

.log_info("Closing connections.")
dbDisconnect(lw_conn)
dbDisconnect(db_conn)

##  Facilities -----------------------------------------------------------------

.log_info("Attaching facility names (OHASIS versions).")
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
   nhsss$harp_dx$initial$data %<>%
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

nhsss$harp_dx$initial$check <- list()
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
      "SPECIMEN_REFER_TYPE"
   )
   # dates
   vars      <- c(
      "encoded_date",
      "report_date",
      "visit_date",
      "blood_extract_date",
      "specimen_receipt_date",
      "confirm_date",
      "BIRTHDATE"
   )
   .log_info("Checking dates.")
   for (var in vars) {
      var                                <- as.symbol(var)
      nhsss$harp_dx$initial$check[[var]] <- nhsss$harp_dx$initial$data %>%
         filter(
            is.na(!!var) |
               !!var >= ohasis$next_date |
               !!var <= as.Date("1900-01-01")
         ) %>%
         select(
            any_of(view_vars),
            !!var
         )

      if (as.character(var) %in% c("blood_extract_date", "specimen_receipt_date"))
         nhsss$harp_dx$initial$check[[var]] <- nhsss$harp_dx$initial$check[[var]] %>%
            filter(StrLeft(CONFIRM_TYPE, 1) == "2")
   }

   # non-negotiable variables
   vars <- c(
      "FORM_VERSION",
      "TEST_FACI",
      "SPECIMEN_REFER_TYPE",
      "PERM_PSGC_REG",
      "PERM_PSGC_PROV",
      "PERM_PSGC_MUNC",
      "CURR_PSGC_REG",
      "CURR_PSGC_PROV",
      "CURR_PSGC_MUNC",
      "FIRST",
      "LAST",
      "CENTRAL_ID",
      "AGE",
      "SEX",
      "SELF_IDENT",
      "IDNUM",
      "NATIONALITY",
      "CONFIRM_CODE",
      "T1_DATE",
      "T1_RESULT",
      "T2_DATE",
      "T2_RESULT",
      "T3_DATE",
      "T3_RESULT"
   )
   .log_info("Checking if non-negotiable variables are missing.")
   for (var in vars) {
      var                                <- as.symbol(var)
      nhsss$harp_dx$initial$check[[var]] <- nhsss$harp_dx$initial$data %>%
         filter(
            is.na(!!var)
         ) %>%
         select(
            any_of(view_vars),
            !!var
         )

      if (as.character(var) %in% c("IDNUM", "CONFIRM_CODE"))
         nhsss$harp_dx$initial$check[[var]] <- nhsss$harp_dx$initial$check[[var]] %>%
            filter(StrLeft(CONFIRM_TYPE, 1) == "1")

      if (as.character(var) %in% c("T1_DATE", "T2_DATE", "T3_DATE", "SPECIMEN_REFER_TYPE"))
         nhsss$harp_dx$initial$check[[var]] <- nhsss$harp_dx$initial$check[[var]] %>%
            filter(StrLeft(CONFIRM_TYPE, 1) == "2")
   }

   # special checks
   .log_info("Checking for mismatch facilities (source != test).")
   nhsss$harp_dx$initial$check[["SOURCE_FACI"]] <- nhsss$harp_dx$initial$data %>%
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

   .log_info("Checking for short names.")
   nhsss$harp_dx$initial$check[["short_name"]] <- nhsss$harp_dx$initial$data %>%
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

   .log_info("Checking for similarly named municipalities.")
   nhsss$harp_dx$initial$check[["dup_munc"]] <- nhsss$harp_dx$initial$data %>%
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

   .log_info("Checking for males tagged as pregnant.")
   nhsss$harp_dx$initial$check[["pregnant_m"]] <- nhsss$harp_dx$initial$data %>%
      filter(
         StrLeft(IS_PREGNANT, 1) == '1' | StrLeft(MED_IS_PREGNANT, 1) == '1',
         StrLeft(SEX, 1) == '1'
      ) %>%
      select(
         any_of(view_vars),
         IS_PREGNANT,
         MED_IS_PREGNANT
      )

   .log_info("Checking for pregnant females.")
   nhsss$harp_dx$initial$check[["pregnant_f"]] <- nhsss$harp_dx$initial$data %>%
      filter(
         StrLeft(IS_PREGNANT, 1) == '1' | StrLeft(MED_IS_PREGNANT, 1) == '1',
         StrLeft(SEX, 1) == '2'
      ) %>%
      select(
         any_of(view_vars),
         IS_PREGNANT,
         MED_IS_PREGNANT
      )

   .log_info("Checking calculated age vs computed age.")
   nhsss$harp_dx$initial$check[["mismatch_age"]] <- nhsss$harp_dx$initial$data %>%
      filter(
         AGE != AGE_DTA
      ) %>%
      select(
         any_of(view_vars),
         AGE,
         AGE_DTA
      )

   .log_info("Checking NRL-SACCL reports not assigned to DOH-EB.")
   nhsss$harp_dx$initial$check[["saccl_not_eb"]] <- nhsss$harp_dx$initial$data %>%
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
   nhsss$harp_dx$initial$check[["T1_KIT"]] <- nhsss$harp_dx$initial$data %>%
      filter(
         !(T1_KIT %in% c("SD Bioline HIV 1/2 3.0", "SYSMEX HISCL HIV Ag + Ab Assay")) | is.na(T1_KIT)
      ) %>%
      select(
         any_of(view_vars),
         T1_KIT
      )
   nhsss$harp_dx$initial$check[["T2_KIT"]] <- nhsss$harp_dx$initial$data %>%
      filter(
         !(T2_KIT %in% c("Alere Determine HIV-1/2", "VIDAS HIV DUO Ultra")) | is.na(T2_KIT)
      ) %>%
      select(
         any_of(view_vars),
         T2_KIT
      )
   nhsss$harp_dx$initial$check[["T3_KIT"]] <- nhsss$harp_dx$initial$data %>%
      filter(
         !(T3_KIT %in% c("Geenius HIV 1/2 Confirmatory Assay", "HIV 1/2 STAT-PAK Assay")) | is.na(T3_KIT)
      ) %>%
      select(
         any_of(view_vars),
         T3_KIT
      )

   # range-median
   vars <- c(
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
   .log_info("Checking range-median of data.")
   nhsss$harp_dx$initial$check$tabstat <- data.frame()
   for (var in vars) {
      var <- as.symbol(var)
      df  <- nhsss$harp_dx$initial$data

      # HTS forms only
      if (as.character(var) %in% c("EXPOSE_SEX_M_AV_DATE",
                                   "EXPOSE_SEX_M_AV_NOCONDOM_DATE",
                                   "EXPOSE_SEX_F_AV_DATE",
                                   "EXPOSE_SEX_F_AV_NOCONDOM_DATE"))
         df <- df %>% filter(FORM_VERSION == "HTS Form (v2021)")

      # CrCLs only
      if (as.character(var) %in% c("blood_extract_date",
                                   "specimen_receipt_date",
                                   "T1_DATE",
                                   "T2_DATE",
                                   "T3_DATE"))
         df <- df %>% filter(StrLeft(CONFIRM_TYPE, 1) == "2")

      nhsss$harp_dx$initial$check$tabstat <- df %>%
         summarise(
            VARIABLE = as.character(var),
            MIN      = min(!!var, na.rm = TRUE),
            MEDIAN   = median(!!var, na.rm = TRUE),
            MAX      = max(!!var, na.rm = TRUE),
            NAs      = sum(if_else(is.na(!!var), 1, 0, 0))
         ) %>%
         mutate_all(~as.character(.)) %>%
         bind_rows(nhsss$harp_dx$initial$check$tabstat)
   }
}

##  Consolidate issues ---------------------------------------------------------

# write into NHSSS GSheet
data_name <- "initial"
if (!is.empty(nhsss$harp_dx[[data_name]]$check))
   .validation_gsheets(
      data_name   = data_name,
      parent_list = nhsss$harp_dx[[data_name]]$check,
      drive_path  = paste0(nhsss$harp_dx$gdrive$path$report, "Validation/"),
      surv_name   = "HARP Dx"
   )

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
