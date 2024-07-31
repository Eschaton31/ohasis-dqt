
tables <-
   c("px_pii", "px_faci_info", "px_ob", "px_hiv_testing", "px_consent", "px_occupation", "px_ofw", "px_risk", "px_expose_profile", "px_med_profile", "px_test_reason", "px_test_previous", "px_staging", "px_cfbs", "px_reach", "px_linkage", "px_other_service", "px_test_refuse")
lapply(tables, function(tbl) ohasis$data_factory("lake", tbl, "upsert", TRUE))

tables <- c("form_a", "form_hts", "form_cfbs", "form_prep", "form_art_bc")
lapply(tables, function(tbl) ohasis$data_factory("warehouse", tbl, "upsert", TRUE))

conn       <- ohasis$conn("db")
px_confirm <- dbTable(conn, "ohasis_interim", "px_confirm")
dbDisconnect(conn)

dir_info(file.path(Sys.getenv("DRIVE_DROPBOX"), "File requests", "SACCL Submissions", "Census 2020,2021, and 2022"))

saccl_2021 <- "R:/File requests/SACCL Submissions/Census 2020,2021, and 2022/saccl_hiv - DATABASE HIV 2021.xlsx"
saccl_2022 <- "R:/File requests/SACCL Submissions/Census 2020,2021, and 2022/saccl_hiv - 2022.xlsx"
saccl_2020 <- "R:/File requests/SACCL Submissions/Census 2020,2021, and 2022/saccl_hiv - DATABASE HIV 2020.xlsx"

df_2021 <- read_xlsx(saccl_2021, col_names = FALSE, col_types = "text") %>%
   rename(
      `D.R.`         = 2,
      `LAB NUMBER`   = 3,
      NAME           = 4,
      DOB            = 5,
      A              = 6,
      S              = 7,
      SOURCE         = 8,
      INTERPRETATION = 13
   )
df_2020 <- read_xlsx(saccl_2020, col_types = "text")
df_2022 <- read_xlsx(saccl_2022, col_types = "text")

df <- df_2021 %>%
   mutate(
      year         = 2021,
      FINAL_RESULT = INTERPRETATION
   ) %>%
   bind_rows(
      df_2020 %>%
         mutate(
            year         = 2020,
            FINAL_RESULT = GEENIUS
         )
   ) %>%
   bind_rows(
      df_2022 %>%
         mutate(
            year         = 2022,
            FINAL_RESULT = `FINAL INTERPRETATION`
         )
   ) %>%
   mutate(
      FINAL_RESULT = case_when(
         FINAL_RESULT %in% c("NEGATIVE", "NONREACTIVE") ~ "NR",
         FINAL_RESULT == "POSITIVE" ~ "R",
         FINAL_RESULT == "REACTIVE" ~ "R",
         FINAL_RESULT == "INDETERMINATE" ~ "IND",
         FINAL_RESULT == "DUPLICATE" ~ "DUP",
         FINAL_RESULT == "duplicate" ~ "DUP",
         is.na(FINAL_RESULT) ~ "(no result)",
         TRUE ~ FINAL_RESULT
      )
   ) %>%
   rename(CONFIRM_CODE = `LAB NUMBER`) %>%
   left_join(
      y = px_confirm %>%
         mutate(
            ohasis       = 1,
            FINAL_RESULT = case_when(
               FINAL_RESULT %in% c("Negative", "Nonreactive") ~ "NR",
               FINAL_RESULT == "Positive" ~ "R",
               FINAL_RESULT == "Indeterminate" ~ "IND",
               FINAL_RESULT == "Duplicate" ~ "DUP",
               is.na(FINAL_RESULT) ~ "(no result)",
               TRUE ~ FINAL_RESULT
            )
         ) %>%
         select(CONFIRM_CODE, OH_RESULT = FINAL_RESULT, ohasis, REC_ID)
   ) %>%
   mutate(
      ISSUE = case_when(
         ohasis == 1 & OH_RESULT != FINAL_RESULT ~ "mismatch encoded result vs. SACCL census",
         ohasis == 1 & OH_RESULT == FINAL_RESULT ~ "no issues",
         is.na(ohasis) ~ "not in OHASIS"
      )
   ) %>%
   select(
      DATE_REQUEST    = 2,
      CONFIRM_CODE,
      NAME,
      BIRTHDATE       = DOB,
      AGE             = A,
      SEX             = S,
      SOURCE,
      SACCL_ML_RESULT = FINAL_RESULT,
      OH_RESULT,
      IN_OHASIS       = ohasis,
      ISSUE,
      REC_ID
   ) %>%
   mutate_at(
      .vars = vars(BIRTHDATE, DATE_REQUEST),
      ~excel_numeric_to_date(as.numeric(.))
   )

df %>%
   filter(ISSUE != "no issues") %>%
   slackr_csv("20221211175840_saccl_2020-2022_not_in_ohasis.csv", channels = c("dqt", "data-controllers"))


conn   <- ohasis$conn("lw")
form_d <- dbTable(conn, "ohasis_warehouse", "form_d")
id_reg <- dbTable(conn, "ohasis_warehouse", "id_registry", cols = c("PATIENT_ID", "CENTRAL_ID"))
dbDisconnect(conn)
spm <- form_d %>%
   filter(
      FACI_ID == "110002" | SERVICE_FACI == "110002"
   ) %>%
   rename(CREATED = CREATED_BY) %>%
   rename(UPDATED = UPDATED_BY) %>%
   select(-starts_with("DELETED_")) %>%
   select(-DISEASE, -MODULE) %>%
   ohasis$get_staff(c(CREATED_BY = "CREATED")) %>%
   ohasis$get_staff(c(UPDATED_BY = "UPDATED")) %>%
   ohasis$get_faci(
      list(RECORD_FACI = c("FACI_ID", "SUB_FACI_ID")),
      "name"
   )
spm <- convert_faci_id(spm, ohasis$ref_faci, list(RECORD_FACI = c("FACI_ID", "SUB_FACI_ID")))
spm <- convert_faci_id(spm, ohasis$ref_faci, list(DEATH_FACI = c("SERVICE_FACI", "SERVICE_SUB_FACI")))
spm <- ohasis$get_addr(
   spm,
   c(
      "PERM_REG"  = "PERM_PSGC_REG",
      "PERM_PROV" = "PERM_PSGC_PROV",
      "PERM_MUNC" = "PERM_PSGC_MUNC"
   ),
   "name"
)
spm <- ohasis$get_addr(
   spm,
   c(
      "CURR_REG"  = "CURR_PSGC_REG",
      "CURR_PROV" = "CURR_PSGC_PROV",
      "CURR_MUNC" = "CURR_PSGC_MUNC"
   ),
   "name"
)
spm <- ohasis$get_addr(
   spm,
   c(
      "BIRTH_REG"  = "BIRTH_PSGC_REG",
      "BIRTH_PROV" = "BIRTH_PSGC_PROV",
      "BIRTH_MUNC" = "BIRTH_PSGC_MUNC"
   ),
   "name"
)
spm <- ohasis$get_addr(
   spm,
   c(
      "DEATH_REG"  = "DEATH_PSGC_REG",
      "DEATH_PROV" = "DEATH_PSGC_PROV",
      "DEATH_MUNC" = "DEATH_PSGC_MUNC"
   ),
   "name"
)

spm %<>%
   select(
      -FACI_ID,
      -SUB_FACI_ID,
      -SERVICE_FACI,
      -SERVICE_SUB_FACI,
      -SNAPSHOT
   ) %>%
   left_join(id_reg) %>%
   mutate(
      CENTRAL_ID = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),
   ) %>%
   relocate(CENTRAL_ID, .before = PATIENT_ID)

spm %>%
   slackr_csv("20221223-spm-form_d.csv", channels = "@macabreros.doh")

get_cid(spm, id_reg, PATIENT_ID)
spm <- convert_faci_id(spm, ohasis$ref_faci, list(RECORD_FACI = c("FACI_ID", "SUB_FACI_ID")))

#####

tx         <- list()
tx$periods <- list(
   "2022" = c("03", "06", "09"),
   "2021" = c("03", "06", "09", "12"),
   "2020" = c("03", "06", "09", "12"),
   "2019" = "12"
)
tx$data    <- list()
invisible(lapply(names(tx$periods), function(yr) {
   cols <- c("sacclcode", "art_id", "outcome", "onart")
   for (mo in tx$periods[[yr]]) {
      if (yr == 2022) {
         .GlobalEnv$tx$data[[paste0("art", yr, mo)]] <- hs_data("harp_tx", "reg", yr, mo) %>%
            read_dta(col_select = c(art_id, confirmatory_code)) %>%
            left_join(
               y  = hs_data("harp_tx", "outcome", yr, mo) %>%
                  read_dta(col_select = c(art_id, outcome)),
               by = "art_id"
            ) %>%
            mutate(
               sacclcode = if_else(!is.na(confirmatory_code), str_replace_all(confirmatory_code, "[^[:alnum:]]", ""), NA_character_)
            ) %>%
            select(-confirmatory_code)
      } else {
         .GlobalEnv$tx$data[[paste0("art", yr, mo)]] <- hs_data("harp_tx", "outcome", yr, mo) %>%
            read_dta(col_select = c(sacclcode, outcome))
      }

      .GlobalEnv$tx$data[[paste0("art", yr, mo)]] %<>%
         mutate_at(
            .vars = vars(starts_with("outcome")),
            ~case_when(
               . == "alive on arv" ~ "onart",
               . == "alive on art" ~ "onart",
               . == "lost to follow up" ~ "ltfu",
               . == "trans out" ~ "transout",
               . == "dead" ~ "dead",
               TRUE ~ .
            )
         )
   }
}))


base <- tx$data$art202209 %>%
   rename(outcome202209 = outcome) %>%
   left_join(select(tx$data$art202206, art_id, outcome202206 = outcome)) %>%
   left_join(select(tx$data$art202203, art_id, outcome202202 = outcome))
for (period in names(tx$data)[!grepl("2022", names(tx$data))]) {
   new_col <- sub("art", "outcome", period)
   base %<>%
      left_join(
         y  = tx$data[[period]] %>%
            select(
               sacclcode,
               !!new_col := outcome
            ),
         by = "sacclcode"
      )
}

base_long <- base %>%
   pivot_longer(
      cols = starts_with("outcome")
   ) %>%
   mutate(
      outcome_period = StrRight(name, 6),
      outcome_yr     = StrLeft(outcome_period, 4),
      outcome_mo     = StrRight(outcome_period, 2),
   )

outcome_rtt_itt <- base %>%
   left_join(
      y  = base_long %>%
         filter(outcome_yr == 2022) %>%
         group_by(art_id) %>%
         summarise(outcome_2022 = paste0(collapse = ", ", sort(unique(value)))) %>%
         ungroup(),
      by = "art_id"
   ) %>%
   left_join(
      y  = base_long %>%
         filter(outcome_yr == 2021) %>%
         group_by(art_id) %>%
         summarise(outcome_2021 = paste0(collapse = ", ", sort(unique(value)))) %>%
         ungroup(),
      by = "art_id"
   ) %>%
   left_join(
      y  = base_long %>%
         filter(outcome_yr == 2020) %>%
         group_by(art_id) %>%
         summarise(outcome_2020 = paste0(collapse = ", ", sort(unique(value)))) %>%
         ungroup(),
      by = "art_id"
   ) %>%
   mutate(
      rtt_2022 = if_else(
         condition = outcome202112 == "ltfu" & stri_detect_fixed(outcome_2022, "onart"),
         true      = "rtt",
         false     = "ltfu",
         missing   = "new"
      ),
      rtt_2021 = if_else(
         condition = outcome202012 == "ltfu" & stri_detect_fixed(outcome_2021, "onart"),
         true      = "rtt",
         false     = "ltfu",
         missing   = "new"
      ),
      rtt_2020 = if_else(
         condition = outcome201912 == "ltfu" & stri_detect_fixed(outcome_2020, "onart"),
         true      = "rtt",
         false     = "ltfu",
         missing   = "new"
      )
   )

outcome_rtt_itt <- base %>%
   mutate(
      rtt_2022 = case_when(
         outcome202203 == 'alive on arv' & outcome202112 == 'lost to follow up' ~ 1,
         outcome202206 == 'alive on arv' & outcome202112 == 'lost to follow up' ~ 1,
         outcome == 'alive on arv' & outcome202112 == 'lost to follow up' ~ 1
      ),
      rtt_2021 = case_when(
         outcome202103 == 'alive on arv' & outcome202012 == 'lost to follow up' ~ 1,
         outcome202106 == 'alive on arv' & outcome202012 == 'lost to follow up' ~ 1,
         outcome202109 == 'alive on arv' & outcome202012 == 'lost to follow up' ~ 1,
         outcome202112 == 'alive on arv' & outcome202012 == 'lost to follow up' ~ 1,
      ),
      rtt_2020 = case_when(
         outcome202003 == 'alive on arv' & outcome201912 == 'lost to follow up' ~ 1,
         outcome202006 == 'alive on arv' & outcome201912 == 'lost to follow up' ~ 1,
         outcome202009 == 'alive on arv' & outcome201912 == 'lost to follow up' ~ 1,
         outcome202012 == 'alive on arv' & outcome201912 == 'lost to follow up' ~ 1,
      )
   )

ritt_2022 <- outcome_rtt_itt %>%
   group_by(rtt_2022) %>%
   summarise(Counts = n())

ritt_2021 <- outcome_rtt_itt %>%
   group_by(rtt_2021) %>%
   summarise(Counts = n())

ritt_2020 <- outcome_rtt_itt %>%
   group_by(rtt_2020) %>%
   summarise(Counts = n())


#####################################

db_conn   <- ohasis$conn("lw")
db_name   <- "ohasis_interim"
px_record <- dbplyr::in_schema(db_name, "px_record")
px_info   <- dbplyr::in_schema(db_name, "px_info")
px_name   <- dbplyr::in_schema(db_name, "px_name")

tbl(db_conn, dbplyr::in_schema(db_name, "px_profile")) %>%
   mutate(
      SELF_IDENT   = case_when(
         SELF_IDENT == 1 ~ '1_Male',
         SELF_IDENT == 2 ~ '2_Female',
         SELF_IDENT == 3 ~ '3_Other',
         TRUE ~ NA_character_
      ),
      EDUC_LEVEL   = case_when(
         EDUC_LEVEL == 1 ~ '1_None',
         EDUC_LEVEL == 2 ~ '2_Elementary',
         EDUC_LEVEL == 3 ~ '3_High School',
         EDUC_LEVEL == 4 ~ '4_College',
         EDUC_LEVEL == 5 ~ '5_Vocational',
         EDUC_LEVEL == 6 ~ '6_Post-Graduate',
         EDUC_LEVEL == 7 ~ '7_Pre-school',
         TRUE ~ NA_character_
      ),
      CIVIL_STATUS = case_when(
         CIVIL_STATUS == 1 ~ '1_Single',
         CIVIL_STATUS == 2 ~ '2_Married',
         CIVIL_STATUS == 3 ~ '3_Separated',
         CIVIL_STATUS == 4 ~ '4_Widowed',
         CIVIL_STATUS == 5 ~ '5_Divorced',
         TRUE ~ NA_character_
      )
   ) %>%
   show_query()

dbplyr::join_query()
tbl(db_conn, px_record) %>%
   dbplyr::join_query(
      y      = tbl(db_conn, px_info),
      vars   = c("REC_ID", "PATIENT_ID"),
      type   = "inner",
      by     = "REC_ID",
      suffix = c("rec", "info")
   ) %>%
   show_query()

dbDisconnect(db_conn)

#####################################

lw_conn   <- ohasis$conn("lw")
db_name   <- "ohasis_warehouse"
hts_where <- r"(REC_ID IN (SELECT SOURCE_REC FROM ohasis_warehouse.rec_link))"
form_prep <- dbTable(lw_conn, db_name, "form_prep")
form_hts  <- dbTable(lw_conn, db_name, "form_hts", where = hts_where, raw_where = TRUE)
form_a    <- dbTable(lw_conn, db_name, "form_a", where = hts_where, raw_where = TRUE)
form_cfbs <- dbTable(lw_conn, db_name, "form_cfbs", where = hts_where, raw_where = TRUE)
rec_link  <- dbTable(lw_conn, db_name, "rec_link")
id_reg    <- dbTable(lw_conn, db_name, "id_registry")
dbDisconnect(lw_conn)

hts_data <- process_hts(form_hts, form_a, form_cfbs)
prep     <- process_prep(form_prep, hts_data, rec_link) %>%
   get_cid(id_reg, PATIENT_ID)

setdiff(names(nhsss$prep$official$new_reg), names(nhsss$prep$official$old_reg))
old_reg <- read_dta("H:/_R/library/prep/data/20221110_reg-prep_2022-10.dta") %>%
   select(-self_identity, -self_identity_other, -CENTRAL_ID) %>%
   get_cid(id_reg, PATIENT_ID) %>%
   left_join(
      y  = nhsss$prep$corr$old_reg %>%
         select(
            prep_id = PREP_ID,
            NEW_VALUE
         ),
      by = "prep_id"
   ) %>%
   mutate(
      REC_ID = if_else(
         !is.na(NEW_VALUE),
         NEW_VALUE,
         REC_ID,
         REC_ID
      )
   ) %>%
   left_join(
      y  = prep %>%
         mutate(
            self_identity       = if_else(
               condition = !is.na(SELF_IDENT),
               true      = substr(stri_trans_toupper(SELF_IDENT), 3, stri_length(SELF_IDENT)),
               false     = NA_character_
            ),
            self_identity_other = toupper(SELF_IDENT_OTHER),
            self_identity       = case_when(
               self_identity_other == "N/A" ~ NA_character_,
               self_identity_other == "no answer" ~ NA_character_,
               self_identity == "OTHER" ~ "OTHERS",
               self_identity == "MAN" ~ "MALE",
               self_identity == "WOMAN" ~ "FEMALE",
               self_identity == "MALE" ~ "MALE",
               self_identity == "FEMALE" ~ "FEMALE",
               TRUE ~ self_identity
            ),
            self_identity_other = case_when(
               self_identity_other == "NO ANSWER" ~ NA_character_,
               self_identity_other == "N/A" ~ NA_character_,
               TRUE ~ self_identity_other
            ),
            FIRST_TIME          = keep_code(FIRST_TIME),
            FIRST_TIME          = case_when(
               FIRST_TIME == 1 ~ as.integer(1),
               FIRST_TIME == 0 ~ NA_integer_,
               TRUE ~ NA_integer_
            ),
            FORM_VERSION        = case_when(
               src == "screen2020" ~ "PrEP Screening (v2020)",
               src == "ffup2020" ~ "PrEP Follow-up (v2020)",
            )
         ) %>%
         generate_gender_identity(SEX, SELF_IDENT, SELF_IDENT_OTHER, gender_identity) %>%
         select(
            CENTRAL_ID,
            REC_ID,
            prep_form       = FORM_VERSION,
            self_identity,
            self_identity_other,
            gender_identity,
            weight          = WEIGHT,
            body_temp       = FEVER,
            risk_screen,
            ars_screen,
            sti_screen,
            sti_visit,
            starts_with("lab_", ignore.case = FALSE),
            clin_screen,
            dispensed,
            eligible,
            with_hts,
            HTS_REC,
            hts_form,
            hts_modality,
            hts_result,
            hts_date,
            prep_hts_date   = PREP_HIV_DATE,
            starts_with("prep_risk_", ignore.case = FALSE),
            starts_with("hts_risk_", ignore.case = FALSE),
            prep_first_time = FIRST_TIME,
         ) %>%
         mutate(exists = 1),
      by = c("CENTRAL_ID", "REC_ID")
   )

write_dta(old_reg, "H:/_R/library/prep/data/20221110_reg-prep_2022-10_mod.dta")

data_factory <- function(db_type = NULL, table_name = NULL, update_type = NULL, default_yes = FALSE, from = NULL, to = NULL) {
   # append "ohasis_" as db name
   db_name     <- ifelse(db_type %in% c("lake", "warehouse"), paste0("ohasis_", db_type), db_type)
   table_space <- Id(schema = db_name, table = table_name)

   # get input
   if (default_yes == TRUE) {
      update <- "1"
   } else {
      update <- input(
         prompt  = paste0("Update ", red(table_name), "?"),
         options = c("1" = "yes", "2" = "no"),
         default = "1"
      )
   }

   # update
   if (update == "1") {
      .log_info("Updating {red(table_name)} @ the {red(db_type)}.")

      # open connections
      .log_info("Opening connections.")
      db_conn <- ohasis$conn("db")
      lw_conn <- ohasis$conn("lw")

      # data for deletion (warehouse)
      for_delete <- data.frame()

      # read sql first, then parse for necessary snapshots
      sql_query      <- read_file("../src/data_lake/upsert/px_pii.sql")
      sql_tables     <- substr(sql_query,
                               stri_locate_first_fixed(sql_query, "FROM "),
                               nchar(sql_query))
      query_table    <- sql_query
      query_nrow     <- paste0("SELECT COUNT(*) AS rows ", sql_tables)
      query_snapshot <- paste0("SELECT MAX(SNAPSHOT) AS snapshot FROM ", db_name, "..", table_name)

      # snapshots are the reference for scoping
      # get start date of records  to be fetched
      if (!is.null(from)) {
         snapshot_old <- from
      } else if (dbExistsTable(lw_conn, table_space)) {
         snapshot_old <- as.character(dbGetQuery(lw_conn, query_snapshot)$snapshot)
      } else if (update_type == "refresh") {
         snapshot_old <- "1970-01-01 00:00:00"

         if (dbExistsTable(lw_conn, table_space))
            dbExecute(lw_conn, glue(r"(DROP TABLE `{db_name}`.`{table_name}`;)"))
      }

      # get end date of records  to be fetched
      if (!is.null(to)) {
         snapshot_new <- str_split(ohasis$timestamp, "\\.")[[1]]
         snapshot_new <- paste0(snapshot_new[1], "-",
                                snapshot_new[2], "-",
                                snapshot_new[3], " ",
                                substr(snapshot_new[4], 1, 2), ":",
                                substr(snapshot_new[4], 3, 4), ":",
                                substr(snapshot_new[4], 5, 6))
      }

      # # rollback 1 month to get other changes
      # snapshot_old <- as.POSIXct(snapshot_old) %m-%
      #    days(3) %>%
      #    format("%Y-%m-%d %H:%M:%S")

      # run data lake script for object
      .log_info("Getting new/updated data.")
      factory_file <- file.path(getwd(), "../src", paste0("data_", db_type), "refresh", paste0(table_name, '.R'))
      if (!file.exists(factory_file))
         factory_file <- file.path(getwd(), "../src", paste0("data_", db_type), "upsert", paste0(table_name, '.R'))

      source(factory_file, local = TRUE)

      # keep connection alive
      dbDisconnect(lw_conn)
      lw_conn <- ohasis$conn("lw")

      # check if there is data for deletion
      if (nrow(for_delete) > 0 && dbExistsTable(lw_conn, table_space)) {
         .log_info("Number of invalidated records = {red(formatC(nrow(for_delete), big.mark = ','))}.")
         dbxDelete(
            lw_conn,
            table_space,
            for_delete,
            batch_size = 1000
         )
         .log_success("Invalidated records removed.")
      }

      if (continue > 0) {
         .log_info("Payload = {red(formatC(nrow(object), big.mark = ','))} rows.")
         ohasis$upsert(lw_conn, db_type, table_name, object, id_col)
         # update reference
         df <- data.frame(
            user      = Sys.getenv("LW_USER"),
            report_yr = ohasis$yr,
            report_mo = ohasis$mo,
            run_title = paste0(ohasis$timestamp, " (", ohasis$run_title, ")"),
            table     = table_name,
            rows      = nrow(object),
            snapshot  = snapshot_new
         )

         # log if successful
         dbAppendTable(
            lw_conn,
            DBI::SQL(paste0('`', db_name, '`.`logs`')),
            df
         )
         .log_success("Done.")
      } else {
         .log_info("No new/updated data found.")
      }

      # close connections
      dbDisconnect(db_conn)
      dbDisconnect(lw_conn)
   }
}

lw_conn <- ohasis$conn("db")
db      <- "ohasis_interim"
dbDisconnect(lw_conn)
rec <- dbTable2(
   lw_conn,
   db,
   c(rec = "px_record"),
   cols  = c(reg.CENTRAL_ID, "rec.*"),
   join  = list(
      left_join = list(table = c(reg = "registry"), on = c(rec.PATIENT_ID = "reg.PATIENT_ID"))
   ),
   where = c(rec.MODULE == '6'),
   name  = "px_labs"
)

harp_22 <- ohasis$get_data("harp_full", 2022, 12) %>%
   read_dta() %>%
   mutate(
      dx              = if_else(!is.na(idnum), 1, 0, 0),
      dx_plhiv        = if_else(!is.na(idnum) &
                                   (dead != 1 | is.na(dead)) &
                                   (is.na(outcome) | outcome != "dead"), 1, 0, 0),
      plhiv           = if_else((dead != 1 | is.na(dead)) & (is.na(outcome) | outcome != "dead"), 1, 0, 0),
      everonart       = if_else(everonart == 1, 1, 0, 0),
      everonart_plhiv = if_else(everonart == 1 & plhiv == 1, 1, 0, 0),
      onart           = if_else(onart == 1, 1, 0, 0),
      onart_1mo       = if_else(onart == 1 & latest_nextpickup >= as.Date('2022-12-01'), 1, 0, 0),
      onart_30dy      = if_else(onart == 1 & floor(interval(latest_nextpickup, as.Date('2022-12-31')) / days(1)) <= 30, 1, 0, 0),
   ) %>%
   mutate(
      .after       = outcome,
      outcome_1mo  = case_when(
         outcome == "dead" ~ "dead",
         grepl("stopped", outcome) ~ "stopped",
         grepl("trans", outcome) ~ "transout",
         everonart == 1 & onart_1mo == 0 ~ "lost to follow up",
         onart_1mo == 1 ~ "alive on arv",
         outcome == "lost to follow up" ~ "ltfu",
         # TRUE ~ "(no data)"
      ),
      outcome_30dy = case_when(
         outcome == "dead" ~ "dead",
         grepl("stopped", outcome) ~ "stopped",
         grepl("trans", outcome) ~ "transout",
         everonart == 1 & onart_30dy == 0 ~ "lost to follow up",
         onart_30dy == 1 ~ "alive on arv",
         outcome == "lost to follow up" ~ "lost to follow up",
         # TRUE ~ "(no data)"
      )
   )

harp_21 <- ohasis$get_data("harp_full", 2021, 12) %>%
   read_dta() %>%
   mutate(
      dx              = if_else(!is.na(idnum), 1, 0, 0),
      dx_plhiv        = if_else(!is.na(idnum) &
                                   (dead != 1 | is.na(dead)) &
                                   (is.na(outcome) | outcome != "dead"), 1, 0, 0),
      plhiv           = if_else((dead != 1 | is.na(dead)) & (is.na(outcome) | outcome != "dead"), 1, 0, 0),
      everonart       = if_else(everonart == 1, 1, 0, 0),
      everonart_plhiv = if_else(everonart == 1 & plhiv == 1, 1, 0, 0),
      onart           = if_else(onart == 1, 1, 0, 0),
      onart_1mo       = if_else(onart == 1 & latest_nextpickup >= as.Date('2021-12-01'), 1, 0, 0),
      onart_30dy      = if_else(onart == 1 & floor(interval(latest_nextpickup, as.Date('2021-12-31')) / days(1)) <= 30, 1, 0, 0),
   ) %>%
   mutate(
      .after       = outcome,
      outcome_1mo  = case_when(
         outcome == "dead" ~ "dead",
         grepl("stopped", outcome) ~ "stopped",
         grepl("trans", outcome) ~ "transout",
         everonart == 1 & onart_1mo == 0 ~ "lost to follow up",
         onart_1mo == 1 ~ "alive on arv",
         outcome == "lost to follow up" ~ "ltfu",
         # TRUE ~ "(no data)"
      ),
      outcome_30dy = case_when(
         outcome == "dead" ~ "dead",
         grepl("stopped", outcome) ~ "stopped",
         grepl("trans", outcome) ~ "transout",
         everonart == 1 & onart_30dy == 0 ~ "lost to follow up",
         onart_30dy == 1 ~ "alive on arv",
         outcome == "lost to follow up" ~ "lost to follow up",
         # TRUE ~ "(no data)"
      )
   )

art_22 <- hs_data("harp_tx", "outcome", 2022, 12) %>%
   read_dta() %>%
   left_join(
      y = hs_data("harp_tx", "reg", 2022, 12) %>%
         read_dta(col_select = c(art_id, confirmatory_code))
   ) %>%
   mutate(
      sacclcode  = if_else(!is.na(confirmatory_code), str_replace_all(confirmatory_code, "[^[:alnum:]]", ""), NA_character_),
      everonart  = 1,
      onart      = if_else(onart == 1, 1, 0, 0),
      onart_1mo  = if_else(onart == 1 & latest_nextpickup >= as.Date('2022-12-01'), 1, 0, 0),
      onart_30dy = if_else(onart == 1 & floor(interval(latest_nextpickup, as.Date('2022-12-31')) / days(1)) <= 30, 1, 0, 0),
   ) %>%
   mutate(
      .after       = outcome,
      outcome_1mo  = case_when(
         outcome == "dead" ~ "dead",
         grepl("stopped", outcome) ~ "stopped",
         grepl("trans", outcome) ~ "transout",
         everonart == 1 & onart_1mo == 0 ~ "lost to follow up",
         onart_1mo == 1 ~ "alive on arv",
         outcome == "lost to follow up" ~ "ltfu",
         # TRUE ~ "(no data)"
      ),
      outcome_30dy = case_when(
         outcome == "dead" ~ "dead",
         grepl("stopped", outcome) ~ "stopped",
         grepl("trans", outcome) ~ "transout",
         everonart == 1 & onart_30dy == 0 ~ "lost to follow up",
         onart_30dy == 1 ~ "alive on arv",
         outcome == "lost to follow up" ~ "lost to follow up",
         # TRUE ~ "(no data)"
      )
   )

art_21 <- hs_data("harp_tx", "outcome", 2021, 12) %>%
   read_dta() %>%
   mutate(
      sacclcode  = if_else(!is.na(sacclcode), str_replace_all(sacclcode, "[^[:alnum:]]", ""), NA_character_),
      everonart  = 1,
      onart      = if_else(onart == 1, 1, 0, 0),
      onart_1mo  = if_else(onart == 1 & latest_nextpickup >= as.Date('2021-12-01'), 1, 0, 0),
      onart_30dy = if_else(onart == 1 & floor(interval(latest_nextpickup, as.Date('2021-12-31')) / days(1)) <= 30, 1, 0, 0),
   ) %>%
   mutate(
      .after       = outcome,
      outcome_1mo  = case_when(
         outcome == "dead" ~ "dead",
         grepl("stopped", outcome) ~ "stopped",
         grepl("trans", outcome) ~ "transout",
         everonart == 1 & onart_1mo == 0 ~ "lost to follow up",
         onart_1mo == 1 ~ "alive on arv",
         outcome == "lost to follow up" ~ "ltfu",
         # TRUE ~ "(no data)"
      ),
      outcome_30dy = case_when(
         outcome == "dead" ~ "dead",
         grepl("stopped", outcome) ~ "stopped",
         grepl("trans", outcome) ~ "transout",
         everonart == 1 & onart_30dy == 0 ~ "lost to follow up",
         onart_30dy == 1 ~ "alive on arv",
         outcome == "lost to follow up" ~ "lost to follow up",
         # TRUE ~ "(no data)"
      )
   )

harp_aem <- harp_22 %>%
   rename_at(
      .vars = vars(starts_with("onart"),
                   starts_with("outcome"),
                   cur_age),
      ~paste0(., "2022")
   ) %>%
   left_join(
      y  = harp_21 %>%
         select(
            idnum,
            starts_with("onart"),
            starts_with("outcome"),
            cur_age
         ) %>%
         rename_at(
            .vars = vars(starts_with("onart"),
                         starts_with("outcome"),
                         cur_age),
            ~paste0(., "2021")
         ),
      by = "idnum"
   )

art_aem <- art_22 %>%
   rename_at(
      .vars = vars(starts_with("onart"),
                   starts_with("outcome"),
                   curr_age),
      ~paste0(., "2022")
   ) %>%
   left_join(
      y  = art_21 %>%
         select(
            sacclcode,
            starts_with("onart"),
            starts_with("outcome"),
            curr_age = age
         ) %>%
         rename_at(
            .vars = vars(starts_with("onart"),
                         starts_with("outcome"),
                         curr_age),
            ~paste0(., "2021")
         ),
      by = "sacclcode"
   )

write_dta(format_stata(harp_aem), "H:/20230130_harp_2022-12_forAEM.dta")
write_dta(format_stata(art_aem), "H:/20230130_onart_2022-12_forAEM.dta")


art <- read_dta(hs_data("harp_tx", "reg", 2022, 12), col_select = c(CENTRAL_ID, art_id, confirmatory_code, first, middle, last, uic, px_code)) %>%
   left_join(
      y  = read_dta(hs_data("harp_tx", "outcome", 2022, 12)) %>%
         select(-CENTRAL_ID),
      by = join_by(art_id)
   )

art202206 <- hs_data("harp_tx", "outcome", 2022, 6) %>%
   read_dta(col_select = c(art_id, onart, artstart_date, latest_nextpickup, outcome, baseline_vl, vlp12m)) %>%
   mutate(
      vl_elig   = if_else(
         condition = onart == 1 & (interval(artstart_date, "2022-06-30") / days(1)) > 92,
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      vl_tested = if_else(
         condition = onart == 1 &
            coalesce(baseline_vl, 0) == 0 &
            !is.na(vlp12m),
         true      = 1,
         false     = 0,
         missing   = 0
      ),
   )
art202209 <- hs_data("harp_tx", "outcome", 2022, 9) %>%
   read_dta(col_select = c(art_id, onart, artstart_date, latest_nextpickup, outcome, baseline_vl, vlp12m)) %>%
   mutate(
      vl_elig   = if_else(
         condition = onart == 1 & (interval(artstart_date, "2022-09-30") / days(1)) > 92,
         true      = 1,
         false     = 0,
         missing   = 0
      ),
      vl_tested = if_else(
         condition = onart == 1 &
            coalesce(baseline_vl, 0) == 0 &
            !is.na(vlp12m),
         true      = 1,
         false     = 0,
         missing   = 0
      ),
   )

art202209 %>%
   tab(onart, vl_elig, vl_tested)

art202206 %>%
   tab(onart, vl_elig, vl_tested)

art %>%
   filter(realhub == "TLY") %>%
   write_xlsx("H:/20230315_onart-tly_2022-12.xlsx")

prep <- hs_data("prep", "outcome", 2022, 9) %>%
   read_dta() %>%
   faci_code_to_id(
      ohasis$ref_faci_code,
      c(FACI_ID = "faci", SUB_FACI_ID = "branch")
   ) %>%
   left_join(
      y = read_sheet("1qunK5aO5-TDj7mAz7rQzCpN1plLGS3kSJArptcFtfsw") %>%
         distinct(FACI_ID, .keep_all = TRUE),
      join_by(FACI_ID)
   )

prep %>%
   mutate(
      PREP_CURR = if_else(
         condition = !is.na(latest_regimen) & latest_ffupdate %within% interval("2022-07-01", "2022-09-30"),
         true      = 1,
         false     = 0,
         missing   = 0
      ),
   ) %>%
   filter(prep_reg == "NCR", prep_munc == "QUEZON") %>%
   tab(PREP_CURR, site_epic_2022)

lst        <- tabulizer::extract_tables(file = "C:/Users/johnb/Downloads/EXPORTED-PDF-Jay-Chinjen.pdf", method = "lattice")
confirm_df <- lapply(lst, function(data) {
   # name1       <- stri_c("X", seq_along(data[1,]))
   # name2       <- stri_c(data[1,], data[2,])
   # name_final  <- ifelse(name2 == "", name1, name2)
   # names(data) <- name_final

   data %<>%
      as.data.frame() %>%
      slice(-1, -2) %>%
      select(
         DATE_RECEIVE = V3,
         CONFIRM_CODE = V4,
         FULLNAME     = V5,
         BIRTHDATE    = V6,
         AGE          = V7,
         SEX          = V8,
         SOURCE       = V9,
         RAPID        = V10,
         SYSMEX       = V11,
         VIDAS        = V14,
         GEENIUS      = V15,
         REMARKS      = V16,
         DATE_CONFIRM = V17
      )

   return(data)
})
confirm_df %<>%
   bind_rows() %>%
   as_tibble() %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(toupper(.))
   ) %>%
   mutate(
      T1_RESULT    = as.numeric(SYSMEX),
      T1_RESULT    = case_when(
         SYSMEX == ">100.000" ~ "10",
         T1_RESULT >= 1 ~ "10",
         T1_RESULT < 1 ~ "20",
         TRUE ~ "  "
      ),
      T2_RESULT    = case_when(
         VIDAS == "REACTIVE" ~ "10",
         VIDAS == "NONREACTIVE" ~ "20",
         TRUE ~ "  "
      ),

      T3_KIT       = case_when(
         RAPID != "" ~ "STAT-PAK",
         GEENIUS != "" ~ "GEENIUS",
      ),
      T3_RESULT    = case_when(
         RAPID == "REACTIVE" ~ "10",
         RAPID == "NONREACTIVE" ~ "20",
         GEENIUS == "POSITIVE" ~ "10",
         GEENIUS == "NEGATIVE" ~ "20",
         GEENIUS == "INDETERMINATE" ~ "30",
         TRUE ~ "  "
      ),
      FINAL_RESULT = stri_c(T1_RESULT, T2_RESULT, T3_RESULT),
      FINAL_RESULT = case_when(
         FINAL_RESULT == "101010" ~ "Positive",
         FINAL_RESULT == "202020" ~ "Negative",
         FINAL_RESULT == "2020  " ~ "Negative",
         FINAL_RESULT == "20    " ~ "Negative",
         grepl("30", FINAL_RESULT) ~ "Indeterminate",
         grepl("20", FINAL_RESULT) ~ "Indeterminate",
         grepl("^SAME AS", REMARKS) ~ "Duplicate"
      ),

      DATE_RECEIVE = as.Date(DATE_RECEIVE, "%m/%d/%y"),
      DATE_CONFIRM = as.Date(DATE_CONFIRM, "%m/%d/%y"),
      BIRTHDATE    = as.Date(BIRTHDATE, "%m/%d/%Y"),
   )

saccl21 <- dir_info("R:/File requests/SACCL Submissions/EB2021", glob = "*.pdf", recurse = TRUE)
saccl22 <- dir_info("R:/File requests/SACCL Submissions/eb2022 FROM LIST", glob = "*.pdf", recurse = TRUE)

unreported <- bind_rows(saccl21, saccl22) %>%
   mutate(
      FILENAME     = basename(path),
      CONFIRM_CODE = FILENAME %>%
         stri_replace_all_fixed(".pdf", "") %>%
         substr(1, 12),
   ) %>%
   anti_join(
      y = px_confirm
   )

pb <- progress_bar$new(format = ":current of :total files | [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed", total = nrow(unreported), width = 100, clear = FALSE)
pb$tick(0)
for (i in seq_len(nrow(unreported))) {
   file_copy(unreported[i,]$path, file.path("H:/SACCL 2021-2022 Missing", basename(unreported[i,]$path)), overwrite = TRUE)
   pb$tick(1)
}


saccl_feb <- dir_info("R:/File requests/SACCL Submissions/2023.01", recurse = TRUE) %>%
   mutate(
      FILENAME     = basename(path),
      CONFIRM_CODE = FILENAME %>%
         stri_replace_all_fixed(".pdf", "") %>%
         substr(1, 12),
      NEW_FILE     = stri_c(CONFIRM_CODE, "..", path_ext(FILENAME))
   ) %>%
   filter(type == "file")
dir_nc    <- "N:/HARP Cloud/HARP Forms/Confirmatory/2023.01"
dir_nc    <- "H:/saccl_jan2021"
pb        <- progress_bar$new(format = ":current of :total files | [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed", total = nrow(saccl_feb), width = 100, clear = FALSE)
pb$tick(0)
for (i in seq_len(nrow(saccl_feb))) {
   file_copy(saccl_feb[i,]$path, file.path(dir_nc, saccl_feb[i,]$NEW_FILE), overwrite = TRUE)
   pb$tick(1)
}

## ART Submissions
lw_conn  <- ohasis$conn("lw")
art_data <- tracked_select(lw_conn, r"(
SELECT COALESCE(SERVICE_FACI, FACI_ID) AS SERVICE_FACI,
       SERVICE_SUB_FACI,
       YEAR(VISIT_DATE)  AS VISIT_YR,
       MONTH(VISIT_DATE) AS VISIT_MO
FROM ohasis_warehouse.form_art_bc AS art
WHERE YEAR(VISIT_DATE) = 2023
  AND MEDICINE_SUMMARY IS NOT NULL
)", "ART Submissions")
dbDisconnect(lw_conn)

avg_submit <- art_data %>%
   filter(VISIT_MO < 4) %>%
   ohasis$get_faci(
      list(TX_HUB = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
      "code",
      c("TX_REG", "TX_PROV", "TX_MUNC")
   ) %>%
   group_by(TX_REG, TX_HUB, VISIT_MO) %>%
   summarise(
      VISITS = n()
   ) %>%
   ungroup() %>%
   group_by(TX_REG, TX_HUB) %>%
   summarise(
      AVG_MONTHLY_2023 = floor(mean(VISITS))
   ) %>%
   ungroup()

submissions <- avg_submit %>%
   full_join(
      art_data %>%
         filter(VISIT_MO == 4) %>%
         ohasis$get_faci(
            list(TX_HUB = c("SERVICE_FACI", "SERVICE_SUB_FACI")),
            "code",
            c("TX_REG", "TX_PROV", "TX_MUNC")
         ) %>%
         group_by(TX_REG, TX_HUB) %>%
         summarise(
            VISITS_2023_04 = n()
         ) %>%
         ungroup()
   ) %>%
   mutate_at(
      .vars = vars(AVG_MONTHLY_2023, VISITS_2023_04),
      ~as.integer(.)
   )

submissions %>%
   mutate(
      NEW_COMPARED_TO_AVG = (VISITS_2023_04 / AVG_MONTHLY_2023),
      SUBMISSION_REMARKS  = case_when(
         NEW_COMPARED_TO_AVG <= .60 ~ "partial",
         is.na(VISITS_2023_04) ~ "no submission",
         is.na(AVG_MONTHLY_2023) ~ "first time submission",
         TRUE ~ ""
      )
   ) %>%
   as_tibble() %>%
   write_sheet("1QZ8Tb1BcE6djJxFUqPHvz8QENXRDPAx2OXBUdApb7R4", "OHASIS-Encoded")

options <- callr::rscript_process_options(
   script = file.path(getwd(), '../src', 'misc', 'pii_search', '00_main.R'),
   env    = c(callr::rcmd_safe_env(), DEDUP_PII = Sys.getenv("DEDUP_PII"))
)
rp      <- callr::rscript_process$new(options)
rp$wait()
rp$finalize()
rp$kill()
