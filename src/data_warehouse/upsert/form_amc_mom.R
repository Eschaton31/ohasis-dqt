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
if (dbExistsTable(lw_conn, Id(schema = "ohasis_warehouse", table = "form_amc_mom"))) {
   for_delete_2 <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
      inner_join(
         y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_amc_mom")) %>%
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
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_ob")),
               by = "REC_ID"
            ) %>%
            filter_at(
               .vars           = vars(LMP,
                                      PREG_MOS,
                                      PREG_WKS,
                                      EDD,
                                      ANC_FACI,
                                      DELIVER_FACI_TYPE,
                                      DELIVER_FACI),
               .vars_predicate = any_vars(!is.na(.))
            ) %>%
            mutate(
               DELIVER_FACI_TYPE = case_when(
                  DELIVER_FACI_TYPE == '8888' ~ '8888_Others',
                  DELIVER_FACI_TYPE == '9999' ~ '9999_No plans yet',
                  DELIVER_FACI_TYPE == '20' ~ '20_Home',
                  DELIVER_FACI_TYPE == '11' ~ '11_Hospital',
                  DELIVER_FACI_TYPE == '12' ~ '12_Lying-in clinic',
                  TRUE ~ as.character(DELIVER_FACI_TYPE)
               )
            ) %>%
            select(
               REC_ID,
               LMP,
               PREG_MOS,
               PREG_WKS,
               EDD,
               ANC_FACI,
               DELIVER_FACI_TYPE,
               DELIVER_FACI
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # partner data
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_partner")),
               by = "REC_ID"
            ) %>%
            mutate(
               PARTNER_TESTED      = case_when(
                  PARTNER_TESTED == '9999' ~ '9999_Don\'t Know',
                  PARTNER_TESTED == '1' ~ '1_Yes',
                  PARTNER_TESTED == '0' ~ '0_No',
                  TRUE ~ as.character(PARTNER_TESTED)
               ),
               PARTNER_TEST_RESULT = case_when(
                  PARTNER_TEST_RESULT == '1' ~ '1_Positive',
                  PARTNER_TEST_RESULT == '2' ~ '2_Negative',
                  PARTNER_TEST_RESULT == '3' ~ '3_Indeterminate',
                  PARTNER_TEST_RESULT == '4' ~ '4_Was not able to get result',
                  TRUE ~ as.character(PARTNER_TEST_RESULT)
               ),
               PARTNER_ON_ARV      = case_when(
                  PARTNER_ON_ARV == '127' ~ '9999_Don\'t Know',
                  PARTNER_ON_ARV == '9999' ~ '9999_Don\'t Know',
                  PARTNER_ON_ARV == '1' ~ '1_Yes',
                  PARTNER_ON_ARV == '0' ~ '0_No',
                  TRUE ~ as.character(PARTNER_ON_ARV)
               ),
            ) %>%
            select(
               REC_ID,
               PARTNER_TESTED,
               PARTNER_TEST_DATE,
               PARTNER_TEST_FACI,
               PARTNER_TEST_RESULT,
               PARTNER_ON_ARV,
               PARTNER_NOT_REASON,
               PARTNER_PXID = PARTNER_ID
            ) %>%
            collect(),
         by = "REC_ID"
      )
}