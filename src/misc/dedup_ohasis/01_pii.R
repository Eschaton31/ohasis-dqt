lw_conn       <- ohasis$conn("lw")
oh_id_schema  <- dbplyr::in_schema("ohasis_warehouse", "id_registry")
px_pii_schema <- dbplyr::in_schema("ohasis_lake", "px_pii")

dedup$id_registry <- tbl(lw_conn, oh_id_schema) %>%
   select(CENTRAL_ID, PATIENT_ID) %>%
   collect()

reload <- input(
   prompt  = "Do you want to re-download all PIIs?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
if (reload == "1") {
   dedup$pii <- tbl(lw_conn, px_pii_schema) %>%
      select(
         PATIENT_ID,
         FIRST,
         MIDDLE,
         LAST,
         UIC,
         CONFIRMATORY_CODE,
         PATIENT_CODE,
         BIRTHDATE,
         PHILSYS_ID,
         PHILHEALTH_NO,
         SEX,
         DELETED_AT,
         SNAPSHOT
      ) %>%
      collect()
}
dbDisconnect(lw_conn)

cols <- c(
   "FIRST",
   "MIDDLE",
   "LAST",
   "UIC",
   "CONFIRMATORY_CODE",
   "PATIENT_CODE",
   "BIRTHDATE",
   "PHILSYS_ID",
   "PHILHEALTH_NO",
   "SEX"
)

for (col in cols) {
   .log_info("Getting latest data for {green(col)}.")
   col_name          <- as.name(col)
   dedup$vars[[col]] <- dedup$pii %>%
      filter(is.na(DELETED_AT), !is.na(!!col_name)) %>%
      left_join(
         y  = dedup$id_registry,
         by = "PATIENT_ID"
      ) %>%
      mutate(
         CENTRAL_ID = if_else(
            condition = is.na(CENTRAL_ID),
            true      = PATIENT_ID,
            false     = CENTRAL_ID
         )
      ) %>%
      arrange(desc(SNAPSHOT)) %>%
      distinct(
         CENTRAL_ID,
         !!col_name
      ) %>%
      rename(
         DATA = 1
      ) %>%
      mutate(
         VAR = col
      ) %>%
      mutate_all(~as.character(.)) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE)
}

rm(lw_conn, oh_id_schema, px_pii_schema, col_name, col, cols)

dedup$linelist <- bind_rows(dedup$vars) %>%
   pivot_wider(
      id_cols     = CENTRAL_ID,
      names_from  = VAR,
      values_from = DATA
   )


dedup$standard <- dedup_prep(
   data         = dedup$linelist %>%
      mutate(
         SUFFIX    = NA_character_,
         BIRTHDATE = if_else(!is.na(BIRTHDATE), as.Date(BIRTHDATE), NA_Date_)
      ),
   name_f       = FIRST,
   name_m       = MIDDLE,
   name_l       = LAST,
   name_s       = SUFFIX,
   uic          = UIC,
   birthdate    = BIRTHDATE,
   code_confirm = CONFIRMATORY_CODE,
   code_px      = PATIENT_CODE,
   phic         = PHILHEALTH_NO,
   philsys      = PHILSYS_ID
)