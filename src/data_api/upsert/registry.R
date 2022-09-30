##  OHASIS ID Registry ---------------------------------------------------------

continue <- 0
id_col   <- "PATIENT_ID"
# px identifiers (demographics, address, etc.)
object   <- tbl(db_conn, dbplyr::in_schema("ohasis_interim", "registry")) %>%
   filter(
      (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
         (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
         (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
   )

# get number of affected rows
if ((object %>% count() %>% collect())$n > 0) {
   continue <- 1
   object   <- dbTable(
      db_conn,
      "ohasis_interim",
      "registry",
      where     = glue(r"(
((CREATED_AT >= '{snapshot_old}' AND CREATED_AT <= '{snapshot_new}') OR
(UPDATED_AT >= '{snapshot_old}' AND UPDATED_AT <= '{snapshot_new}') OR
(DELETED_AT >= '{snapshot_old}' AND DELETED_AT <= '{snapshot_new}'))
)"),
      raw_where = TRUE,
      name      = "registry"
   ) %>%
      # keep only form a
      mutate(
         SNAPSHOT = case_when(
            UPDATED_AT > DELETED_AT ~ DELETED_AT,
            UPDATED_AT < DELETED_AT ~ UPDATED_AT,
            !is.na(DELETED_AT) ~ DELETED_AT,
            !is.na(UPDATED_AT) ~ UPDATED_AT,
            TRUE ~ CREATED_AT
         )
      )
}