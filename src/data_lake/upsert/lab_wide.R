##  Lab Data -------------------------------------------------------------------

continue <- 0
id_col   <- "REC_ID"
object   <- tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
   filter(
      (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
         (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
         (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
   ) %>%
   select(
      REC_ID,
      PATIENT_ID,
      CREATED_AT,
      UPDATED_AT,
      DELETED_AT
   )

# get number of affected rows
if ((object %>% count() %>% collect())$n > 0) {
   continue <- 1
   object   <- object %>%
      mutate(
         SNAPSHOT = case_when(
            !is.na(DELETED_AT) ~ DELETED_AT,
            !is.na(UPDATED_AT) ~ UPDATED_AT,
            TRUE ~ CREATED_AT
         ),
      ) %>%
      collect() %>%
      inner_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_labs")),
               by = "REC_ID"
            ) %>%
            select(
               REC_ID,
               TEST         = LAB_TEST,
               DATE         = LAB_DATE,
               RESULT       = LAB_RESULT,
               RESULT_OTHER = LAB_RESULT_OTHER
            ) %>%
            mutate(
               DATE   = as.Date(DATE),
               RESULT = case_when(
                  TEST == "1" & RESULT == "1" ~ "1_Reactive",
                  TEST == "1" & RESULT == "2" ~ "2_Non-reactive",
                  TEST == "3" & RESULT == "1" ~ "1_Reactive",
                  TEST == "3" & RESULT == "2" ~ "2_Non-reactive",
                  TRUE ~ RESULT %>% as.character()
               ),
               TEST   = case_when(
                  TEST == "1" ~ "HBSAG",
                  TEST == "2" ~ "CREA",
                  TEST == "3" ~ "SYPH",
                  TEST == "4" ~ "VIRAL",
                  TEST == "5" ~ "CD4",
                  TEST == "6" ~ "XRAY",
                  TEST == "7" ~ "XPERT",
                  TEST == "8" ~ "DSSM",
                  TEST == "9" ~ "HIVDR",
                  TEST == "10" ~ "HEMOG",
                  TRUE ~ TEST %>% as.character()
               ),
            ) %>%
            pivot_wider(
               id_cols     = REC_ID,
               names_from  = TEST,
               values_from = c(DATE, RESULT, RESULT_OTHER)
            ) %>%
            rename_all(
               ~case_when(
                  . == "DATE_HEMOG" ~ "LAB_HEMOG_DATE",
                  . == "RESULT_HEMOG" ~ "LAB_HEMOG_RESULT",
                  . == "DATE_VIRAL" ~ "LAB_VIRAL_DATE",
                  . == "RESULT_VIRAL" ~ "LAB_VIRAL_RESULT",
                  . == "DATE_CD4" ~ "LAB_CD4_DATE",
                  . == "RESULT_CD4" ~ "LAB_CD4_RESULT",
                  . == "DATE_CREA" ~ "LAB_CREA_DATE",
                  . == "RESULT_CREA" ~ "LAB_CREA_RESULT",
                  . == "RESULT_OTHER_CREA" ~ "LAB_CREA_CLEARANCE",
                  . == "DATE_HBSAG" ~ "LAB_HBSAG_DATE",
                  . == "RESULT_HBSAG" ~ "LAB_HBSAG_RESULT",
                  . == "DATE_XRAY" ~ "LAB_XRAY_DATE",
                  . == "RESULT_XRAY" ~ "LAB_XRAY_RESULT",
                  . == "DATE_XPERT" ~ "LAB_XPERT_DATE",
                  . == "RESULT_XPERT" ~ "LAB_XPERT_RESULT",
                  . == "DATE_DSSM" ~ "LAB_DSSM_DATE",
                  . == "RESULT_DSSM" ~ "LAB_DSSM_RESULT",
                  . == "DATE_HIVDR" ~ "LAB_HIVDR_DATE",
                  . == "RESULT_HIVDR" ~ "LAB_HIVDR_RESULT",
                  . == "DATE_SYPH" ~ "LAB_SYPH_DATE",
                  . == "RESULT_SYPH" ~ "LAB_SYPH_RESULT",
                  . == "RESULT_OTHER_SYPH" ~ "LAB_SYPH_TITER",
                  . == "DATE_HEMOG" ~ "LAB_HEMOG_DATE",
                  . == "RESULT_HEMOG" ~ "LAB_HEMOG_RESULT",
                  . == "DATE_VIRAL" ~ "LAB_VIRAL_DATE",
                  . == "RESULT_VIRAL" ~ "LAB_VIRAL_RESULT",
                  . == "DATE_CD4" ~ "LAB_CD4_DATE",
                  . == "RESULT_CD4" ~ "LAB_CD4_RESULT",
                  . == "DATE_CREA" ~ "LAB_CREA_DATE",
                  . == "RESULT_CREA" ~ "LAB_CREA_RESULT",
                  . == "RESULT_OTHER_CREA" ~ "LAB_CREA_CLEARANCE",
                  . == "DATE_HBSAG" ~ "LAB_HBSAG_DATE",
                  . == "RESULT_HBSAG" ~ "LAB_HBSAG_RESULT",
                  . == "DATE_XRAY" ~ "LAB_XRAY_DATE",
                  . == "RESULT_XRAY" ~ "LAB_XRAY_RESULT",
                  . == "DATE_XPERT" ~ "LAB_XPERT_DATE",
                  . == "RESULT_XPERT" ~ "LAB_XPERT_RESULT",
                  . == "DATE_DSSM" ~ "LAB_DSSM_DATE",
                  . == "RESULT_DSSM" ~ "LAB_DSSM_RESULT",
                  . == "DATE_HIVDR" ~ "LAB_HIVDR_DATE",
                  . == "RESULT_HIVDR" ~ "LAB_HIVDR_RESULT",
                  . == "DATE_SYPH" ~ "LAB_SYPH_DATE",
                  . == "RESULT_SYPH" ~ "LAB_SYPH_RESULT",
                  . == "RESULT_OTHER_SYPH" ~ "LAB_SYPH_TITER",
                  TRUE ~ .
               )
            ) %>%
            select(
               REC_ID,
               any_of(
                  c("LAB_HEMOG_DATE",
                    "LAB_HEMOG_RESULT",
                    "LAB_VIRAL_DATE",
                    "LAB_VIRAL_RESULT",
                    "LAB_CD4_DATE",
                    "LAB_CD4_RESULT",
                    "LAB_CREA_DATE",
                    "LAB_CREA_RESULT",
                    "LAB_CREA_CLEARANCE",
                    "LAB_HBSAG_DATE",
                    "LAB_HBSAG_RESULT",
                    "LAB_XRAY_DATE",
                    "LAB_XRAY_RESULT",
                    "LAB_XPERT_DATE",
                    "LAB_XPERT_RESULT",
                    "LAB_DSSM_DATE",
                    "LAB_DSSM_RESULT",
                    "LAB_HIVDR_DATE",
                    "LAB_HIVDR_RESULT",
                    "LAB_SYPH_DATE",
                    "LAB_SYPH_RESULT",
                    "LAB_SYPH_TITER",
                    "LAB_HEMOG_DATE",
                    "LAB_HEMOG_RESULT",
                    "LAB_VIRAL_DATE",
                    "LAB_VIRAL_RESULT",
                    "LAB_CD4_DATE",
                    "LAB_CD4_RESULT",
                    "LAB_CREA_DATE",
                    "LAB_CREA_RESULT",
                    "LAB_CREA_CLEARANCE",
                    "LAB_HBSAG_DATE",
                    "LAB_HBSAG_RESULT",
                    "LAB_XRAY_DATE",
                    "LAB_XRAY_RESULT",
                    "LAB_XPERT_DATE",
                    "LAB_XPERT_RESULT",
                    "LAB_DSSM_DATE",
                    "LAB_DSSM_RESULT",
                    "LAB_HIVDR_DATE",
                    "LAB_HIVDR_RESULT",
                    "LAB_SYPH_DATE",
                    "LAB_SYPH_RESULT",
                    "LAB_SYPH_TITER")
               )
            ) %>%
            collect(),
         by = "REC_ID"
      )
}