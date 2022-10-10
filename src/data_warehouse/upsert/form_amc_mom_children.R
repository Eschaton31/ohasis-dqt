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
if (dbExistsTable(lw_conn, Id(schema = "ohasis_warehouse", table = "form_amc_mom_children"))) {
   for_delete_2 <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
      inner_join(
         y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_amc_mom_children")) %>%
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
         REC_ID
      ) %>%
      collect() %>%
      # children section
      inner_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_children")),
               by = "REC_ID"
            ) %>%
            mutate(
               CHILD_RESULT = case_when(
                  CHILD_RESULT == '1' ~ '1_Positive',
                  CHILD_RESULT == '2' ~ '2_Negative',
                  CHILD_RESULT == '3' ~ '3_Indeterminate',
                  CHILD_RESULT == '4' ~ '4_Was not able to get result',
                  TRUE ~ as.character(CHILD_RESULT)
               ),
            ) %>%
            select(
               REC_ID,
               CHILD_RESULT,
               CHILD_RESULT_DATE,
               CHILD_TEST_FACI,
               CHILD_PXID = CHILD_ID
            ) %>%
            collect(),
         by = "REC_ID"
      )
}