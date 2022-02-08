##------------------------------------------------------------------------------
##  List of users/staff
##------------------------------------------------------------------------------

id_col <- "STAFF_ID"
object <- tbl(db_conn, "users") %>%
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
   filter(
      SNAPSHOT >= snapshot_old,
      SNAPSHOT < snapshot_new
   ) %>%
   select(
      STAFF_ID    = USER_ID,
      STAFF_NAME  = FULLNAME,
      STAFF_DESIG = DESIGNATION,
      CREATED_AT,
      UPDATED_AT,
      DELETED_AT,
      REACTIVATE_AT,
      SNAPSHOT
   ) %>%
   collect()