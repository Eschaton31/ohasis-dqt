##  Person Identifiable Information --------------------------------------------

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
      CREATED_AT,
      UPDATED_AT,
      DELETED_AT
   )

# get number of affected rows
if ((object %>% count() %>% collect())$n > 0) {
   continue <- 1
   object   <- object %>%
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_faci")) %>%
            filter(SERVICE_TYPE != "101102") %>%
            select(
               -starts_with("CREATED_"),
               -starts_with("UPDATED_"),
               -starts_with("DELETED_"),
            ),
         by = "REC_ID"
      ) %>%
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_remarks")) %>%
            pivot_wider(
               id_cols      = c("REC_ID", "REMARK_TYPE"),
               names_from   = REMARK_TYPE,
               values_from  = "REMARKS",
               names_prefix = "REMARKS_"
            ) %>%
            select(
               REC_ID,
               CLINIC_NOTES  = REMARKS_1,
               COUNSEL_NOTES = REMARKS_2,
               REPORT_NOTES  = REMARKS_3,
               STI_DIAGNOSIS = REMARKS_10
            ),
         by = "REC_ID"
      ) %>%
      mutate(
         SNAPSHOT = case_when(
            UPDATED_AT > DELETED_AT ~ DELETED_AT,
            UPDATED_AT < DELETED_AT ~ UPDATED_AT,
            !is.na(DELETED_AT) ~ DELETED_AT,
            !is.na(UPDATED_AT) ~ UPDATED_AT,
            TRUE ~ CREATED_AT
         )
      ) %>%
      select(
         REC_ID,
         CREATED_AT,
         UPDATED_AT,
         SNAPSHOT,
         MODALITY         = SERVICE_TYPE,
         SERVICE_FACI     = FACI_ID,
         SERVICE_SUB_FACI = SUB_FACI_ID,
         SERVICE_BY       = PROVIDER_ID,
         REFER_FACI       = REFER_BY_ID,
         CLIENT_TYPE,
         REFER_TYPE,
         PROVIDER_TYPE,
         PROVIDER_TYPE_OTHER,
         TX_STATUS,
         VISIT_TYPE,
         CLINIC_NOTES,
         COUNSEL_NOTES,
         REPORT_NOTES,
         STI_DIAGNOSIS
      ) %>%
      mutate(
         MODALITY      = case_when(
            MODALITY == "*00001" ~ "*00001_Mortality",
            MODALITY == "101101" ~ "101101_Facility-based Testing (FBT)",
            MODALITY == "101103" ~ "101103_Community-based (CBS)",
            MODALITY == "101104" ~ "101104_Non-laboratory FBT (FBS)",
            MODALITY == "101105" ~ "101105_Self-testing",
            MODALITY == "101201" ~ "101201_Anti-Retroviral Treatment (ART)",
            MODALITY == "101301" ~ "101301_Pre-Exposure Prophylaxis (PrEP)",
            MODALITY == "101303" ~ "101303_Prevention of Mother-to-Child Transmission (PMTCT)",
            MODALITY == "101304" ~ "101304_Reach",
            TRUE ~ MODALITY
         ),
         PROVIDER_TYPE = case_when(
            PROVIDER_TYPE == "1" ~ "1_Medical Technologist",
            PROVIDER_TYPE == "2" ~ "2_HIV Counselor",
            PROVIDER_TYPE == "3" ~ "3_CBS Motivator",
            PROVIDER_TYPE == "8888" ~ "8888_Other",
            TRUE ~ PROVIDER_TYPE
         ),
         REFER_TYPE    = case_when(
            REFER_TYPE == "1" ~ "1_TB-DOTS / PMDT Facility",
            REFER_TYPE == "2" ~ "2_Antenatal / Maternity Clinic",
            TRUE ~ REFER_TYPE
         ),
         CLIENT_TYPE   = case_when(
            CLIENT_TYPE == "1" ~ "1_Inpatient",
            CLIENT_TYPE == "2" ~ "2_Walk-in / Outpatient",
            CLIENT_TYPE == "3" ~ "3_Mobile HTS Client",
            CLIENT_TYPE == "5" ~ "5_Satellite Client",
            CLIENT_TYPE == "4" ~ "4_Referral",
            CLIENT_TYPE == "6" ~ "6_Transient",
            CLIENT_TYPE == "7" ~ "7_Persons Deprived of Liberty",
            TRUE ~ CLIENT_TYPE
         ),
         TX_STATUS     = case_when(
            TX_STATUS == "1" ~ "1_Enrollment",
            TX_STATUS == "2" ~ "2_Refill",
            TX_STATUS == "0" ~ "0_Not on ART",
            TRUE ~ NA_character_
         ),
         VISIT_TYPE    = case_when(
            VISIT_TYPE == "1" ~ "1_First consult at this facility",
            VISIT_TYPE == "2" ~ "2_Follow-up",
            VISIT_TYPE == "3" ~ "3_Inpatient",
            TRUE ~ NA_character_
         ),
      ) %>%
      collect()
}