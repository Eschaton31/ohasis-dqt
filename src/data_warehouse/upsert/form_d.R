##  Form D ---------------------------------------------------------------------

continue <- 0
id_col   <- "REC_ID"
# px identifiers (demographics, address, etc.)
object   <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
   filter(
      DISEASE == "*",
      substr(MODULE, 1, 1) == "4",
      SNAPSHOT >= snapshot_old,
      SNAPSHOT <= snapshot_new,
      is.na(DELETED_BY)
   )

for_delete <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
   filter(
      !is.na(DELETED_BY)
   ) %>%
   inner_join(
      y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_d")),
      by = "REC_ID"
   ) %>%
   select(REC_ID) %>%
   collect()

# get number of affected rows
if ((object %>% count() %>% collect())$n > 0) {
   continue <- 1
   object   <- object %>%
      select(
         -starts_with("SERVICE_"),
      ) %>%
      # facility data in form
      left_join(
         y  = tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_faci_info")) %>%
            filter(
               substr(MODALITY, 1, 6) == "*00001"
            ) %>%
            select(
               REC_ID,
               SERVICE_FACI,
               SERVICE_SUB_FACI,
               SERVICE_BY,
            ),
         by = "REC_ID"
      ) %>%
      collect() %>%
      mutate(
         REPORTING_FORM = "Mortality"
      ) %>%
      # key population
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_death")),
               by = "REC_ID"
            ) %>%
            pivot_wider(
               id_cols     = c(REC_ID, INFO_NUM),
               names_from  = DEATH_INFO,
               values_from = c(INFO_TEXT, ICD_VERSION, ICD_CODE)
            ) %>%
            pivot_wider(
               id_cols     = REC_ID,
               names_from  = INFO_NUM,
               values_from = c(
                  starts_with("INFO_TEXT"),
                  starts_with("ICD_VERSION"),
                  starts_with("ICD_CODE")
               )
            ) %>%
            rename_all(
               ~case_when(
                  grepl("_0_", .) ~ stri_replace_all_regex(., "([A-Z_]+)_0_([0-9])+", "MAIN_$1_$2"),
                  grepl("_1_", .) ~ stri_replace_all_regex(., "([A-Z_]+)_1_([0-9])+", "IMMEDIATE_$1_$2"),
                  grepl("_2_", .) ~ stri_replace_all_regex(., "([A-Z_]+)_2_([0-9])+", "ANTECEDENT_$1_$2"),
                  grepl("_3_", .) ~ stri_replace_all_regex(., "([A-Z_]+)_3_([0-9])+", "UNDERLYING_$1_$2"),
                  TRUE ~ .
               )
            ) %>%
            collect() %>%
            mutate(
               IMMEDIATE_INFO_TEXT_DUMMY = NA_character_,
               ANTECEDENT_INFO_TEXT      = NA_character_,
               UNDERLYING_INFO_TEXT      = NA_character_
            ) %>%
            unite(
               starts_with("IMMEDIATE_INFO_TEXT"),
               col   = "IMMEDIATE_CAUSES",
               na.rm = T,
               sep   = "; "
            ) %>%
            unite(
               starts_with("ANTECEDENT_INFO_TEXT"),
               col   = "ANTECEDENT_CAUSES",
               na.rm = T,
               sep   = "; "
            ) %>%
            unite(
               starts_with("UNDERLYING_INFO_TEXT"),
               col   = "UNDERLYING_CAUSES",
               na.rm = T,
               sep   = "; "
            ) %>%
            select(
               REC_ID,
               DEATH_DATE        = MAIN_INFO_TEXT_1,
               REPORTED_BY       = MAIN_INFO_TEXT_3,
               EB_VALIDATED      = MAIN_INFO_TEXT_2,
               DEATH_CERTIFICATE = MAIN_INFO_TEXT_4,
               IMMEDIATE_CAUSES,
               ANTECEDENT_CAUSES,
               UNDERLYING_CAUSES
            ) %>%
            mutate(
               DEATH_DATE        = as.Date(DEATH_DATE),
               REPORTED_BY       = case_when(
                  REPORTED_BY == "1" ~ "1_Family",
                  REPORTED_BY == "2" ~ "2_Friends",
                  REPORTED_BY == "3" ~ "3_Facility",
                  TRUE ~ NA_character_
               ),
               EB_VALIDATED      = case_when(
                  EB_VALIDATED == "0" ~ "0_No",
                  EB_VALIDATED == "1" ~ "1_Yes",
                  EB_VALIDATED == "3" ~ "3_Pending",
                  TRUE ~ NA_character_
               ),
               DEATH_CERTIFICATE = case_when(
                  DEATH_CERTIFICATE == "0" ~ "0_No",
                  DEATH_CERTIFICATE == "1" ~ "1_Yes",
                  TRUE ~ NA_character_
               )
            ),
         by = "REC_ID"
      ) %>%
      # diseases section
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
            filter(
               (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
                  (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
                  (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_oi")),
               by = "REC_ID"
            ) %>%
            mutate(
               IS_OI = case_when(
                  IS_OI == "0" ~ "0_No",
                  IS_OI == "1" ~ "1_Yes",
                  TRUE ~ NA_character_
               ),
               OI    = case_when(
                  OI == "101000" ~ "HIV",
                  OI == "102000" ~ "HEPB",
                  OI == "103000" ~ "HEPC",
                  OI == "104000" ~ "SYPH",
                  OI == "111000" ~ "PCP",
                  OI == "112000" ~ "CMV",
                  OI == "113000" ~ "OROCAND",
                  OI == "117000" ~ "HERPES",
                  OI == "202000" ~ "TB",
                  OI == "111000" ~ "PCP",
                  OI == "115000" ~ "MENINGITIS",
                  OI == "113000" ~ "OROCAND",
                  OI == "116000" ~ "TOXOPLASMOSIS",
                  OI == "201000" ~ "COVID19",
                  OI == "8888" ~ "OTHER",
                  TRUE ~ OI
               )
            ) %>%
            rename(
               PRESENT = IS_OI,
               TEXT    = OI_OTHER
            ) %>%
            pivot_wider(
               id_cols     = REC_ID,
               names_from  = OI,
               values_from = c(PRESENT, TEXT),
               names_glue  = "OI_{OI}_{.value}"
            ) %>%
            rename_all(
               ~case_when(
                  . == "DISEASE_HIV" ~ "OI_HIV_PRESENT",
                  . == "DISEASE_HEPB" ~ "OI_HEPB_PRESENT",
                  . == "DISEASE_HEPC" ~ "OI_HEPC_PRESENT",
                  . == "DISEASE_TB" ~ "OI_TB_PRESENT",
                  . == "DISEASE_PCP" ~ "OI_PCP_PRESENT",
                  . == "DISEASE_MENINGITIS" ~ "OI_MENINGITIS_PRESENT",
                  . == "DISEASE_CMV" ~ "OI_CMV_PRESENT",
                  . == "DISEASE_OROCAND" ~ "OI_OROCAND_PRESENT",
                  . == "DISEASE_TOXOPLASMOSIS" ~ "OI_TOXOPLASMOSIS_PRESENT",
                  . == "DISEASE_COVID19" ~ "OI_COVID19_PRESENT",
                  . == "DISEASE_OTHER" ~ "OI_OTHER_TEXT",
                  TRUE ~ .
               )
            ) %>%
            select(
               REC_ID,
               starts_with("DISEASE")
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      bind_rows(
         tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_art_bc")) %>%
            filter(
               NUM_OF_DISC > 0,
               SNAPSHOT >= snapshot_old & SNAPSHOT <= snapshot_new,
               is.na(DELETED_AT)
            ) %>%
            mutate(
               OI_HIV_PRESENT = "1_Yes",
               REPORTING_FORM = "Form BC"
            ) %>%
            select(
               REC_ID,
               PATIENT_ID,
               RECORD_DATE,
               FACI_ID,
               SUB_FACI_ID,
               PRIME,
               CREATED_AT,
               CREATED_BY,
               SERVICE_FACI,
               SERVICE_SUB_FACI,
               SERVICE_BY,
               CONFIRMATORY_CODE,
               UIC,
               PHILHEALTH_NO,
               SEX,
               BIRTHDATE,
               PATIENT_CODE,
               FIRST,
               MIDDLE,
               LAST,
               SUFFIX,
               AGE,
               SELF_IDENT,
               SELF_IDENT_OTHER,
               CURR_PSGC_REG,
               CURR_PSGC_PROV,
               CURR_PSGC_MUNC,
               CURR_ADDR,
               REPORTING_FORM,
               DISEASE_HIV     = OI_HIV_PRESENT,
               DISEASE_HEPB    = OI_HEPB_PRESENT,
               DISEASE_HEPC    = OI_HEPC_PRESENT,
               DISEASE_PCP     = OI_PCP_PRESENT,
               DISEASE_CMV     = OI_CMV_PRESENT,
               DISEASE_OROCAND = OI_OROCAND_PRESENT,
               DISEASE_OTHER   = OI_OTHER_TEXT,
               CLINIC_NOTES,
               COUNSEL_NOTES,
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
                     y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_medicine_disc")),
                     by = "REC_ID"
                  ) %>%
                  filter(
                     DISC_REASON == "8" |
                        toupper(DISC_REASON_OTHER) %like% "%DEATH%" |
                        toupper(DISC_REASON_OTHER) %like% "%DEAD%"
                  ) %>%
                  select(
                     REC_ID,
                     FORMC_REPORT_DATE = DISC_DATE,
                     REPORT_NOTES      = DISC_REASON_OTHER
                  ) %>%
                  collect(),
               by = "REC_ID"
            )
      )
}