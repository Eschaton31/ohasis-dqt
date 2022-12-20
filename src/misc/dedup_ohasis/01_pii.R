lw_conn <- ohasis$conn("lw")

dedup$id_registry <- dbTable(
   lw_conn,
   "ohasis_warehouse",
   "id_registry",
   cols = c("CENTRAL_ID", "PATIENT_ID")
)

reload <- input(
   prompt  = "Do you want to re-download all PIIs?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
if (reload == "1") {
   dedup$pii <- dbTable(
      lw_conn,
      "ohasis_lake",
      "px_pii",
      cols      = c(
         "PATIENT_ID",
         "FIRST",
         "MIDDLE",
         "LAST",
         "UIC",
         "CONFIRMATORY_CODE",
         "PATIENT_CODE",
         "BIRTHDATE",
         "PHILSYS_ID",
         "PHILHEALTH_NO",
         "SEX",
         "DELETED_AT",
         "SNAPSHOT"
      ),
      where     = "px_pii.DELETED_AT IS NULL",
      raw_where = TRUE
   )
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
dedup$pii %<>%
   ungroup() %>%
   select(-any_of("CENTRAL_ID")) %>%
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
   arrange(desc(SNAPSHOT))

for (col in cols) {
   .log_info("Getting latest data for {green(col)}.")
   col_name          <- as.name(col)
   dedup$vars[[col]] <- dedup$pii %>%
      select(
         CENTRAL_ID,
         !!col_name
      ) %>%
      filter(!is.na(!!col_name)) %>%
      distinct(CENTRAL_ID, .keep_all = TRUE) %>%
      # distinct(
      #    CENTRAL_ID,
      #    !!col_name
      # ) %>%
      rename(
         DATA = 2
      ) %>%
      mutate(
         VAR = col
      ) %>%
      mutate_all(~as.character(.))
}

rm(lw_conn, col_name, col, cols)

.log_info("Consolidating variables.")
dedup$linelist <- bind_rows(dedup$vars) %>%
   pivot_wider(
      id_cols     = CENTRAL_ID,
      names_from  = VAR,
      values_from = DATA
   )

.log_info("Dataset cleaning and preparation.")
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