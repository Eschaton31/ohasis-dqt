##  Form A ---------------------------------------------------------------------

continue <- 0
id_col   <- "REC_ID"
# px identifiers (demographics, address, etc.)
object   <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
   filter(
      DISEASE == "HIV",
      substr(MODULE, 1, 1) == "2",
      SNAPSHOT >= snapshot_old,
      SNAPSHOT <= snapshot_new,
      is.na(DELETED_BY)
   )

for_delete_1 <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
   filter(
      DISEASE == "HIV",
      substr(MODULE, 1, 1) == "2",
      SNAPSHOT >= snapshot_old,
      SNAPSHOT <= snapshot_new,
      !is.na(DELETED_BY)
   ) %>%
   select(REC_ID) %>%
   collect()

for_delete_2 <- data.frame()
if (dbExistsTable(lw_conn, Id(schema = "ohasis_warehouse", table = "form_amc_child"))) {
   for_delete_2 <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
      inner_join(
         y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_amc_child")) %>%
            select(REC_ID),
         by = "REC_ID"
      ) %>%
      select(REC_ID) %>%
      collect()
}

for_delete <- bind_rows(for_delete_1, for_delete_2)

# get number of affected rows
if ((object %>% count() %>% collect())$n > 0) {
   continue <- 1
   object   <- object %>%
      select(
         REC_ID,
         PATIENT_ID
      ) %>%
      collect() %>%
      # ob section
      inner_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_parents")),
               by = "REC_ID"
            ) %>%
            mutate(
               PARENT_TYPE   = case_when(
                  PARENT_TYPE == '1' ~ 'DAD',
                  PARENT_TYPE == '2' ~ 'MOM',
                  TRUE ~ as.character(PARENT_TYPE)
               ),
               PARENT_RESULT = case_when(
                  PARENT_RESULT == '1' ~ '1_Positive',
                  PARENT_RESULT == '2' ~ '2_Negative',
                  PARENT_RESULT == '3' ~ '3_Indeterminate',
                  PARENT_RESULT == '4' ~ '4_Was not able to get result',
                  TRUE ~ as.character(PARENT_RESULT)
               ),
               PARENT_STATUS = case_when(
                  PARENT_STATUS == '1' ~ '1_Alive',
                  PARENT_STATUS == '2' ~ '2_Dead',
                  TRUE ~ as.character(PARENT_STATUS)
               ),
               PARENT_ON_ARV = case_when(
                  PARENT_ON_ARV == '127' ~ '9999_Don\'t Know',
                  PARENT_ON_ARV == '9999' ~ '9999_Don\'t Know',
                  PARENT_ON_ARV == '1' ~ '1_Yes',
                  PARENT_ON_ARV == '0' ~ '0_No',
                  TRUE ~ as.character(PARENT_ON_ARV)
               ),
               PARENT_FEED   = case_when(
                  PARENT_FEED == '1' ~ '1_Yes',
                  PARENT_FEED == '0' ~ '0_No',
                  TRUE ~ as.character(PARENT_FEED)
               ),
            ) %>%
            rename_at(
               .vars = vars(starts_with('PARENT_')),
               ~stri_replace_all_fixed(., 'PARENT_', '')
            ) %>%
            pivot_wider(
               id_cols     = REC_ID,
               names_from  = 'TYPE',
               values_from = c('ID',
                               'REC',
                               'RESULT',
                               'RESULT_DATE',
                               'STATUS',
                               'DATE',
                               'ON_ARV',
                               'NOT_REASON',
                               'FEED'),
               names_glue  = '{TYPE}_{.value}'
            ) %>%
            collect(),
         by = "REC_ID"
      )
}