##  List of users/staff --------------------------------------------------------

continue <- 0
id_col   <- "STAFF_ID"
object   <- tbl(db_conn, dbplyr::in_schema("ohasis_interim", "users")) %>%
   filter(
      (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
         (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
         (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
   )

# get number of affected rows
if ((object %>% count() %>% collect())$n > 0) {
   continue <- 1
   object   <- object %>%
      mutate(
         FIRST_MIDDLE = paste(
            sep = " ",
            FIRST,
            MIDDLE
         ),
         FULLNAME     = paste(
            sep = ", ",
            LAST,
            FIRST_MIDDLE,
            SUFFIX
         ),
         SNAPSHOT     = case_when(
            !is.na(DELETED_AT) ~ DELETED_AT,
            !is.na(UPDATED_AT) ~ UPDATED_AT,
            TRUE ~ CREATED_AT
         ),
      ) %>%
      select(
         STAFF_ID    = USER_ID,
         STAFF_NAME  = FULLNAME,
         STAFF_DESIG = DESIGNATION,
         PRC_LICENSE,
         POST_NOMINAL,
         EMAIL,
         MOBILE,
         LANDLINE,
         FACI_ID,
         CREATED_AT,
         UPDATED_AT,
         DELETED_AT,
         REACTIVATE_AT,
         SNAPSHOT
      ) %>%
      collect()
}