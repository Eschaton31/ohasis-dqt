##  Form PMTCT -----------------------------------------------------------------

continue <- 0
id_col   <- c("REC_ID", "REC_ID_GRP")
# px identifiers (demographics, address, etc.)
object   <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
   filter(
      DISEASE == "HIV",
      substr(MODULE, 1, 1) == "6",
      SNAPSHOT >= snapshot_old,
      SNAPSHOT <= snapshot_new,
      is.na(DELETED_BY)
   )

records <- tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
   filter(
      DISEASE == "101000",
      MODULE == 6,
      (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
         (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new),
      is.na(DELETED_BY)
   ) %>%
   select(REC_ID)

for_delete_1 <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
   filter(
      DISEASE == "HIV",
      substr(MODULE, 1, 1) == "6",
      SNAPSHOT >= snapshot_old,
      SNAPSHOT <= snapshot_new
   ) %>%
   select(REC_ID) %>%
   collect()

for_delete_2 <- data.frame()
if (dbExistsTable(lw_conn, Id(schema = "ohasis_warehouse", table = "form_pmtct"))) {
   for_delete_2 <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
      filter(
         !is.na(DELETED_BY)
      ) %>%
      inner_join(
         y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_pmtct")),
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
         -starts_with("CURR_"),
         -starts_with("PERM_"),
         -starts_with("BIRTH_"),
         -starts_with("SERVICE_"),
         -starts_with("DEATH_")
      ) %>%
      # facility data in form
      inner_join(
         y  = tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_faci_info")) %>%
            filter(
               substr(MODALITY, 1, 6) == "101303"
            ) %>%
            select(
               REC_ID,
               TX_STATUS,
               VISIT_TYPE,
               CLIENT_TYPE,
               SERVICE_FACI,
               SERVICE_SUB_FACI,
               SERVICE_BY,
               REFER_FACI,
               CLINIC_NOTES,
               COUNSEL_NOTES
            ),
         by = "REC_ID"
      ) %>%
      collect() %>%
      # parents
      left_join(
         y  = records %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_parents")),
               by = "REC_ID"
            ) %>%
            mutate(
               PARENT_TYPE   = case_when(
                  PARENT_TYPE == "2" ~ "MOM",
                  TRUE ~ PARENT_TYPE
               ),
               PARENT_RESULT = case_when(
                  PARENT_RESULT == "1" ~ "1_Positive",
                  PARENT_RESULT == "2" ~ "2_Negative",
                  PARENT_RESULT == "3" ~ "3_Indeterminate",
                  PARENT_RESULT == "4" ~ "4_Was not able to get result",
                  TRUE ~ PARENT_RESULT
               ),
               PARENT_STATUS = case_when(
                  PARENT_STATUS == "1" ~ "1_Alive",
                  PARENT_STATUS == "2" ~ "2_Dead",
                  TRUE ~ PARENT_STATUS
               ),
               PARENT_ON_ARV = case_when(
                  PARENT_ON_ARV == "9999" ~ "9999_Don\"t Know",
                  PARENT_ON_ARV == "1" ~ "1_Yes",
                  PARENT_ON_ARV == "0" ~ "0_No",
                  TRUE ~ PARENT_ON_ARV
               ),
               PARENT_FEED   = case_when(
                  PARENT_FEED == "1" ~ "1_Yes",
                  PARENT_FEED == "0" ~ "0_No",
                  TRUE ~ PARENT_FEED
               ),
            ) %>%
            rename_at(
               .vars = vars(starts_with('PARENT_')),
               ~stri_replace_all_fixed(., 'PARENT_', '')
            ) %>%
            pivot_wider(
               id_cols     = REC_ID,
               names_from  = TYPE,
               values_from = c(ID, REC),
               names_glue  = '{TYPE}_{.value}'
            ) %>%
            left_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
                  select(
                     MOM_REC  = REC_ID,
                     MOM_PXID = PATIENT_ID
                  ),
               by = "MOM_REC"
            ) %>%
            left_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_info")) %>%
                  select(
                     MOM_REC               = REC_ID,
                     MOM_BIRTHDATE         = BIRTHDATE,
                     MOM_SEX               = SEX,
                     MOM_UIC               = UIC,
                     MOM_PATIENT_CODE      = PATIENT_CODE,
                     MOM_CONFIRMATORY_CODE = CONFIRMATORY_CODE
                  ),
               by = "MOM_REC"
            ) %>%
            left_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_name")) %>%
                  select(
                     MOM_REC    = REC_ID,
                     MOM_FIRST  = FIRST,
                     MOM_MIDDLE = MIDDLE,
                     MOM_LAST   = LAST,
                     MOM_SUFFIX = SUFFIX,
                  ),
               by = "MOM_REC"
            ) %>%
            left_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_ob")) %>%
                  select(
                     MOM_REC      = REC_ID,
                     MOM_OB_SCORE = OB_SCORE
                  ),
               by = "MOM_REC"
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # pmtct data
      left_join(
         y  = records %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_pmtct")),
               by = "REC_ID"
            ) %>%
            mutate(
               IS_PROPH  = case_when(
                  IS_PROPH == "1" ~ "1_Yes",
                  IS_PROPH == "0" ~ "0_No",
                  TRUE ~ IS_PROPH
               ),
               PCR_DONE  = case_when(
                  PCR_DONE == "1" ~ "1_Yes",
                  PCR_DONE == "0" ~ "0_No",
                  TRUE ~ PCR_DONE
               ),
               FEED_TYPE = case_when(
                  FEED_TYPE == "1" ~ "1_Breasfeeding",
                  FEED_TYPE == "2" ~ "2_Formula feeding",
                  FEED_TYPE == "3" ~ "3_Mixed feeding",
                  TRUE ~ FEED_TYPE
               )
            ) %>%
            select(
               REC_ID,
               PROPH_ARV_STARTED = IS_PROPH,
               PCR_DONE,
               PCR_DATE,
               FEED_TYPE
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # prophylaxis
      left_join(
         y  = records %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_prophylaxis")),
               by = "REC_ID"
            ) %>%
            mutate(
               PROPHYLAXIS = case_when(
                  PROPHYLAXIS == "1" ~ "BCG",
                  PROPHYLAXIS == "2" ~ "CPT",
                  TRUE ~ PROPHYLAXIS
               ),
               IS_PROPH    = case_when(
                  IS_PROPH == "1" ~ "1_Yes",
                  IS_PROPH == "0" ~ "0_No",
                  TRUE ~ IS_PROPH
               ),
            ) %>%
            rename(
               DONE = IS_PROPH,
               DATE = PROPH_DATE
            ) %>%
            pivot_wider(
               id_cols     = REC_ID,
               names_from  = PROPHYLAXIS,
               values_from = c(DONE, DATE),
               names_glue  = "PROPH_{PROPHYLAXIS}_{.value}"
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # arv disp data
      left_join(
         y  = tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
            filter(
               SNAPSHOT >= snapshot_old & SNAPSHOT <= snapshot_new,
               is.na(DELETED_AT)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "disp_meds")),
               by = "REC_ID"
            ) %>%
            collect() %>%
            arrange(REC_ID, DISP_NUM) %>%
            group_by(REC_ID, REC_ID_GRP) %>%
            left_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "inventory_product")) %>%
                  select(
                     MEDICINE = ITEM,
                     SHORT
                  ) %>%
                  collect(),
               by = "MEDICINE"
            ) %>%
            summarise(
               FACI_DISP        = first(FACI_ID, na.rm = TRUE),
               SUB_FACI_DISP    = first(SUB_FACI_ID, na.rm = TRUE),
               MEDICINE_SUMMARY = paste0(unique(SHORT), collapse = "+"),
               DISP_DATE        = suppress_warnings(max(DISP_DATE, na.rm = TRUE), "returning [\\-]*Inf"),
               LATEST_NEXT_DATE = suppress_warnings(max(NEXT_DATE, na.rm = TRUE), "returning [\\-]*Inf"),
            ),
         by = "REC_ID"
      ) %>%
      # tag ART records
      mutate(
         NUM_OF_DRUGS = stri_count_fixed(MEDICINE_SUMMARY, '+') + 1,
         NUM_OF_DRUGS = if_else(is.na(NUM_OF_DRUGS), as.integer(0), as.integer(NUM_OF_DRUGS)),
      )
}