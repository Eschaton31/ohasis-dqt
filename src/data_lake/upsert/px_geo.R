##------------------------------------------------------------------------------
##  Geographic data for patients
##------------------------------------------------------------------------------

id_col <- "REC_ID"
object <- tbl(db_conn, "px_addr") %>%
   mutate(
      SNAPSHOT = case_when(
         !is.na(DELETED_AT) ~ DELETED_AT,
         !is.na(UPDATED_AT) ~ UPDATED_AT,
         TRUE ~ CREATED_AT
      )
   ) %>%
   filter(
      SNAPSHOT >= snapshot_old,
      SNAPSHOT < snapshot_new
   ) %>%
   select(
      REC_ID,
      ADDR_TYPE,
      PSGC_REG  = ADDR_REG,
      PSGC_PROV = ADDR_PROV,
      PSGC_MUNC = ADDR_MUNC,
      ADDR      = ADDR_TEXT
   ) %>%
   mutate(
      ADDR_TYPE = case_when(
         ADDR_TYPE == 1 ~ "CURR",
         ADDR_TYPE == 2 ~ "PERM",
         ADDR_TYPE == 3 ~ "BIRTH",
         ADDR_TYPE == 4 ~ "DEATH",
         ADDR_TYPE == 5 ~ "SERVICE",
         TRUE ~ as.character(ADDR_TYPE)
      )
   ) %>%
   pivot_wider(
      id_cols     = c("REC_ID", "ADDR_TYPE"),
      names_from  = ADDR_TYPE,
      values_from = c("PSGC_REG", "PSGC_PROV", "PSGC_MUNC", "ADDR"),
      names_glue  = "{ADDR_TYPE}_{.value}"
   ) %>%
   select(
      REC_ID,
      starts_with("CURR_"),
      starts_with("PERM_"),
      starts_with("BIRTH_"),
      starts_with("DEATH_"),
      starts_with("SERVICE_")
   ) %>%
   collect()