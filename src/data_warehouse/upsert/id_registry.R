##------------------------------------------------------------------------------
##  OHASIS ID Registry
##------------------------------------------------------------------------------

continue <- 0
id_col   <- "PATIENT_ID"
# px identifiers (demographics, address, etc.)
object   <- tbl(db_conn, "registry") %>%
   filter(
      (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
         (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
         (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
   )

# get number of affected rows
if ((object %>% count() %>% collect())$n > 0) {
   continue <- 1
   object   <- object %>%
      # keep only form a
      mutate(
         SNAPSHOT = case_when(
            UPDATED_AT > DELETED_AT ~ DELETED_AT,
            UPDATED_AT < DELETED_AT ~ UPDATED_AT,
            !is.na(DELETED_AT) ~ DELETED_AT,
            !is.na(UPDATED_AT) ~ UPDATED_AT,
            TRUE ~ CREATED_AT
         )
      ) %>%
      collect()
}