##  Form BC --------------------------------------------------------------------

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

for_delete_2 <- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
   filter(
      !is.na(DELETED_BY)
   ) %>%
   inner_join(
      y  = tbl(lw_conn, dbplyr::in_schema("ohasis_warehouse", "form_prep")),
      by = "REC_ID"
   ) %>%
   select(REC_ID) %>%
   collect()

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
               substr(MODALITY, 1, 6) == "101301"
            ) %>%
            select(
               REC_ID,
               SERVICE_FACI,
               SERVICE_SUB_FACI,
               SERVICE_BY,
               CLINIC_NOTES,
               COUNSEL_NOTES,
               STI_DIAGNOSIS
            ),
         by = "REC_ID"
      ) %>%
      collect() %>%
      # contact info
      left_join(
         y  = records %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_contact")),
               by = "REC_ID"
            ) %>%
            mutate(
               CONTACT_TYPE = case_when(
                  CONTACT_TYPE == "1" ~ "MOBILE",
                  CONTACT_TYPE == "2" ~ "EMAIL",
                  TRUE ~ CONTACT_TYPE
               )
            ) %>%
            pivot_wider(
               id_cols     = c(REC_ID, CONTACT_TYPE),
               names_from  = CONTACT_TYPE,
               values_from = CONTACT,
               names_glue  = 'CLIENT_{CONTACT_TYPE}_{.value}'
            ) %>%
            rename_at(
               .vars = vars(ends_with('_CONTACT')),
               ~stri_replace_all_fixed(., '_CONTACT', '')
            ) %>%
            select(
               REC_ID,
               starts_with("CLIENT_") & !matches("\\d")
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # expose profile
      left_join(
         y  = records %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_expose_profile")),
               by = "REC_ID"
            ) %>%
            mutate(
               WEEK_AVG_SEX = case_when(
                  WEEK_AVG_SEX == "1" ~ "1_<= 1 sex acts a week",
                  WEEK_AVG_SEX == "2" ~ "2_>= 2 sex acts a week",
                  TRUE ~ WEEK_AVG_SEX
               )
            ) %>%
            select(
               REC_ID,
               WEEK_AVG_SEX
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # key population
      left_join(
         y  = full_table(db_conn, records, "px_key_pop") %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # exposure history
      left_join(
         y  = records %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_expose_hist")),
               by = "REC_ID"
            ) %>%
            mutate(
               IS_EXPOSED = case_when(
                  TYPE_LAST_EXPOSE == "4" & IS_EXPOSED == "1" ~ "1",
                  TYPE_LAST_EXPOSE == "0" & IS_EXPOSED == "1" ~ "2",
                  is.na(TYPE_LAST_EXPOSE) & IS_EXPOSED == "1" ~ "2",
                  IS_EXPOSED == "0" ~ "0",
                  TRUE ~ IS_EXPOSED
               ),
               IS_EXPOSED = case_when(
                  IS_EXPOSED == "1" ~ "1_Yes, within the past 12 months",
                  IS_EXPOSED == "2" ~ "2_Yes",
                  IS_EXPOSED == "3" ~ "3_Yes, within the past 6 months",
                  IS_EXPOSED == "4" ~ "4_Yes, within the past 30 days",
                  IS_EXPOSED == "0" ~ "0_No",
                  IS_EXPOSED == "99999" ~ NA_character_,
                  TRUE ~ IS_EXPOSED
               ),
               EXPOSURE   = case_when(
                  EXPOSURE == "261200" ~ "RISK_CONDOMLESS_ANAL",
                  EXPOSURE == "262200" ~ "RISK_CONDOMLESS_VAGINAL",
                  EXPOSURE == "311010" ~ "RISK_DRUG_INJECT",
                  EXPOSURE == "330000" ~ "RISK_DRUG_SEX",
                  EXPOSURE == "200030" ~ "RISK_TRANSACT_SEX",
                  EXPOSURE == "200001" ~ "RISK_HIV_VL_UNKNOWN",
                  EXPOSURE == "230000" ~ "RISK_HIV_UNKNOWN",
                  EXPOSURE == "400000" ~ "RISK_STI",
                  EXPOSURE == "320002" ~ "RISK_PEP",
                  TRUE ~ EXPOSURE
               )
            ) %>%
            pivot_wider(
               id_cols     = REC_ID,
               names_from  = EXPOSURE,
               values_from = IS_EXPOSED
            ) %>%
            rename_at(
               .vars = vars(ends_with("_IS_EXPOSED")),
               ~stri_replace_all_fixed(., "_IS_EXPOSED", "")
            ) %>%
            select(
               REC_ID,
               starts_with("RISK_") & !matches("\\d")
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # labs
      left_join(
         y  = tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "px_pii")) %>%
            filter(
               SNAPSHOT >= snapshot_old & SNAPSHOT <= snapshot_new,
               is.na(DELETED_AT)
            ) %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "lab_wide")),
               by = "REC_ID"
            ) %>%
            select(
               REC_ID,
               LAB_HBSAG_DATE,
               LAB_HBSAG_RESULT,
               LAB_CREA_DATE,
               LAB_CREA_RESULT,
               LAB_CREA_CLEARANCE,
               LAB_SYPH_DATE,
               LAB_SYPH_RESULT,
               LAB_SYPH_TITER,
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # vitals section
      left_join(
         y  = records %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_vitals")),
               by = "REC_ID"
            ) %>%
            pivot_wider(
               id_cols      = REC_ID,
               names_from   = VITAL_SIGN,
               values_from  = VITAL_RESULT,
               names_prefix = 'VITAL_SIGN_'
            ) %>%
            rename_all(
               ~case_when(
                  . == "VITAL_SIGN_2" ~ "WEIGHT",
                  . == "VITAL_SIGN_3" ~ "FEVER",
                  TRUE ~ .
               )
            ) %>%
            select(
               REC_ID,
               any_of(
                  c(
                     "WEIGHT",
                     "FEVER"
                  )
               )
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # ars
      left_join(
         y  = records %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_ars_sx")),
               by = "REC_ID"
            ) %>%
            mutate(
               IS_SYMPTOM = case_when(
                  IS_SYMPTOM == "0" ~ "0_No",
                  IS_SYMPTOM == "1" ~ "1_Yes"
               )
            ) %>%
            pivot_wider(
               id_cols     = REC_ID,
               names_from  = ARS_SYMPTOM,
               values_from = c(IS_SYMPTOM, SYMPTOM_OTHER),
            ) %>%
            rename_all(
               ~case_when(
                  . == "IS_SYMPTOM_1" ~ "ARS_SX_FEVER",
                  . == "IS_SYMPTOM_2" ~ "ARS_SX_SORE_THROAT",
                  . == "IS_SYMPTOM_3" ~ "ARS_SX_DIARRHEA",
                  . == "IS_SYMPTOM_4" ~ "ARS_SX_SWOLLEN_LYMPH",
                  . == "IS_SYMPTOM_5" ~ "ARS_SX_SWOLLEN_TONSILS",
                  . == "IS_SYMPTOM_6" ~ "ARS_SX_RASH",
                  . == "IS_SYMPTOM_7" ~ "ARS_SX_MUSCLE_PAINS",
                  . == "IS_SYMPTOM_8888" ~ "ARS_SX_OTHER",
                  . == "SYMPTOM_OTHER_8888" ~ "ARS_SX_OTHER_TEXT",
                  . == "IS_SYMPTOM_9999" ~ "ARS_SX_NONE",
                  TRUE ~ .
               )
            ) %>%
            select(
               REC_ID,
               starts_with("ARS")
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # sti
      left_join(
         y  = records %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_sti_sx")),
               by = "REC_ID"
            ) %>%
            mutate(
               IS_SYMPTOM = case_when(
                  IS_SYMPTOM == "0" ~ "0_No",
                  IS_SYMPTOM == "1" ~ "1_Yes"
               )
            ) %>%
            pivot_wider(
               id_cols     = REC_ID,
               names_from  = STI_SYMPTOM,
               values_from = c(IS_SYMPTOM, SYMPTOM_OTHER),
            ) %>%
            rename_all(
               ~case_when(
                  . == "IS_SYMPTOM_1" ~ "STI_SX_DISCHARGE_VAGINAL",
                  . == "IS_SYMPTOM_2" ~ "STI_SX_DISCHARGE_ANAL",
                  . == "IS_SYMPTOM_3" ~ "STI_SX_DISCHARGE_URETHRAL",
                  . == "IS_SYMPTOM_4" ~ "STI_SX_SWOLLEN_SCROTUM",
                  . == "IS_SYMPTOM_5" ~ "STI_SX_PAIN_URINE",
                  . == "IS_SYMPTOM_6" ~ "STI_SX_ULCER_GENITAL",
                  . == "IS_SYMPTOM_7" ~ "STI_SX_ULCER_ORAL",
                  . == "IS_SYMPTOM_8" ~ "STI_SX_WARTS_GENITAL",
                  . == "IS_SYMPTOM_9" ~ "STI_SX_PAIN_ABDOMEN",
                  . == "IS_SYMPTOM_8888" ~ "STI_SX_OTHER",
                  . == "SYMPTOM_OTHER_8888" ~ "STI_SX_OTHER_TEXT",
                  . == "IS_SYMPTOM_9999" ~ "STI_SX_NONE",
                  TRUE ~ .
               )
            ) %>%
            select(
               REC_ID,
               starts_with("STI")
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # prep checklist
      left_join(
         y  = records %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_prep_checklist")),
               by = "REC_ID"
            ) %>%
            mutate(
               IS_CHECKED = case_when(
                  IS_CHECKED == "0" ~ "0_No",
                  IS_CHECKED == "1" ~ "1_Yes"
               )
            ) %>%
            pivot_wider(
               id_cols      = REC_ID,
               names_from   = REQUIREMENT,
               values_from  = IS_CHECKED,
               names_prefix = "IS_CHECKED_",
            ) %>%
            rename_all(
               ~case_when(
                  . == "IS_CHECKED_1" ~ "PRE_INIT_HIV_NR",
                  . == "IS_CHECKED_2" ~ "PRE_INIT_WEIGHT",
                  . == "IS_CHECKED_3" ~ "PRE_INIT_NO_ARS",
                  . == "IS_CHECKED_4" ~ "PRE_INIT_CREA_CLEAR",
                  . == "IS_CHECKED_5" ~ "PRE_INIT_NO_ARV_ALLERGY",
                  TRUE ~ .
               )
            ) %>%
            select(
               REC_ID,
               starts_with("PRE_INIT")
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # prep data
      left_join(
         y  = records %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_prep")),
               by = "REC_ID"
            ) %>%
            select(
               REC_ID,
               PREP_HIV_DATE,
               PREP_ACCEPTED,
               PREP_VISIT,
               ELIGIBLE_BEHAVIOR,
               ELIGIBLE_PREP,
               PREP_REQUESTED,
               PREP_PLAN,
               PREP_TYPE,
               FIRST_TIME
            ) %>%
            mutate(
               PREP_CONTINUED    = case_when(
                  PREP_VISIT == '2' & PREP_ACCEPTED == '1' ~ 1,
                  PREP_VISIT == '2' & PREP_ACCEPTED == '0' ~ 0,
                  TRUE ~ NA_integer_
               ),
               PREP_CONTINUED    = case_when(
                  PREP_CONTINUED == '1' ~ '1_Continue PrEP',
                  PREP_CONTINUED == '0' ~ '0_Discontinue PrEP',
                  TRUE ~ as.character(PREP_CONTINUED)
               ),
               PREP_ACCEPTED     = case_when(
                  PREP_VISIT == '1' & PREP_ACCEPTED == '1' ~ 1,
                  PREP_VISIT == '1' & PREP_ACCEPTED == '0' ~ 0,
                  TRUE ~ NA_character_
               ),
               PREP_ACCEPTED     = case_when(
                  PREP_ACCEPTED == '1' ~ '1_Accepted PrEP',
                  PREP_ACCEPTED == '0' ~ '0_Refused PrEP',
                  TRUE ~ as.character(PREP_ACCEPTED)
               ),
               PREP_VISIT        = case_when(
                  PREP_VISIT == '1' ~ '1_Screening',
                  PREP_VISIT == '2' ~ '2_Follow-up',
                  TRUE ~ as.character(PREP_VISIT)
               ),
               ELIGIBLE_BEHAVIOR = case_when(
                  ELIGIBLE_BEHAVIOR == '1' ~ '1_Yes',
                  ELIGIBLE_BEHAVIOR == '0' ~ '0_No',
                  TRUE ~ as.character(ELIGIBLE_BEHAVIOR)
               ),
               ELIGIBLE_PREP     = case_when(
                  ELIGIBLE_PREP == '1' ~ '1_Yes',
                  ELIGIBLE_PREP == '0' ~ '0_No',
                  TRUE ~ as.character(ELIGIBLE_PREP)
               ),
               PREP_REQUESTED    = case_when(
                  PREP_REQUESTED == '1' ~ '1_Yes',
                  PREP_REQUESTED == '0' ~ '0_No',
                  TRUE ~ as.character(PREP_REQUESTED)
               ),
               PREP_PLAN         = case_when(
                  PREP_PLAN == '1' ~ '1_Clinic-supported',
                  PREP_PLAN == '2' ~ '2_Client-supported',
                  PREP_PLAN == '3' ~ '3_Cost-shared',
                  TRUE ~ as.character(PREP_PLAN)
               ),
               PREP_TYPE         = case_when(
                  PREP_TYPE == '1' ~ '1_Daily',
                  PREP_TYPE == '2' ~ '2_Event-driven',
                  TRUE ~ as.character(PREP_TYPE)
               ),
               FIRST_TIME        = case_when(
                  FIRST_TIME == '1' ~ '1_Yes',
                  FIRST_TIME == '0' ~ '0_No',
                  TRUE ~ as.character(FIRST_TIME)
               )
            ) %>%
            rename(PREP_STATUS = PREP_ACCEPTED) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # prep status
      left_join(
         y  = records %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_prep_status")),
               by = "REC_ID"
            ) %>%
            mutate(
               PREP_TYPE_LAST_VISIT = case_when(
                  PREP_TYPE_LAST_VISIT == '1' ~ '1_Daily',
                  PREP_TYPE_LAST_VISIT == '2' ~ '2_Event-driven',
                  TRUE ~ as.character(PREP_TYPE_LAST_VISIT)
               ),
               PREP_SHIFT           = case_when(
                  PREP_SHIFT == '1' ~ '1_Yes',
                  PREP_SHIFT == '0' ~ '0_No',
                  TRUE ~ as.character(PREP_SHIFT)
               ),
               PREP_MISSED          = case_when(
                  PREP_MISSED == '1' ~ '1_Yes',
                  PREP_MISSED == '0' ~ '0_No',
                  TRUE ~ as.character(PREP_MISSED)
               ),
               PREP_SIDE_EFFECTS    = case_when(
                  PREP_SIDE_EFFECTS == '1' ~ '1_Yes',
                  PREP_SIDE_EFFECTS == '0' ~ '0_No',
                  TRUE ~ as.character(PREP_SIDE_EFFECTS)
               )
            ) %>%
            select(
               REC_ID,
               PREP_TYPE_LAST_VISIT,
               PREP_SHIFT,
               PREP_MISSED,
               PREP_SIDE_EFFECTS,
               PREP_SIDE_EFFECTS_SPECIFY = SIDE_EFFECTS
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # occupation section
      left_join(
         y  = records %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_occupation")),
               by = "REC_ID"
            ) %>%
            mutate(
               IS_STUDENT  = case_when(
                  IS_STUDENT == "0" ~ "0_No",
                  IS_STUDENT == "1" ~ "1_Yes",
                  TRUE ~ as.character(IS_STUDENT)
               ),
               IS_EMPLOYED = case_when(
                  IS_EMPLOYED == "0" ~ "0_No",
                  IS_EMPLOYED == "1" ~ "1_Yes",
                  TRUE ~ as.character(IS_EMPLOYED)
               ),
            ) %>%
            select(
               REC_ID,
               IS_EMPLOYED,
               IS_STUDENT
            ) %>%
            collect(),
         by = "REC_ID"
      ) %>%
      # prep finance
      left_join(
         y  = records %>%
            select(REC_ID) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_prep_finance")),
               by = "REC_ID"
            ) %>%
            mutate(
               CAPACITY_TYPE = case_when(
                  CAPACITY == '30000' & CAPACITY_TYPE == '1' ~ '< PHP 30k',
                  CAPACITY == '30000' & CAPACITY_TYPE == '2' ~ '>= PHP 30k',
                  TRUE ~ as.character(CAPACITY_TYPE)
               )
            ) %>%
            select(
               REC_ID,
               FINANCE_CAPACITY = CAPACITY_TYPE
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
      # arv disp data
      mutate(
         VISIT_DATE = case_when(
            RECORD_DATE == as.Date(DISP_DATE) ~ RECORD_DATE,
            RECORD_DATE < as.Date(DISP_DATE) & DISP_DATE >= -25567 ~ as.Date(DISP_DATE),
            RECORD_DATE > as.Date(DISP_DATE) & DISP_DATE >= -25567 ~ as.Date(DISP_DATE),
            is.na(RECORD_DATE) ~ as.Date(DISP_DATE),
            is.na(DISP_DATE) ~ RECORD_DATE,
            TRUE ~ RECORD_DATE
         ),
         .before    = RECORD_DATE
      ) %>%
      # tag ART records
      mutate(
         NUM_OF_DRUGS = stri_count_fixed(MEDICINE_SUMMARY, '+') + 1,
         NUM_OF_DRUGS = if_else(is.na(NUM_OF_DRUGS), as.integer(0), as.integer(NUM_OF_DRUGS)),
         PREP_RECORD  = case_when(
            StrLeft(PREP_STATUS) == 1 ~ "PrEP",
            StrLeft(PREP_CONTINUED) == 1 ~ "PrEP",
            !is.na(MEDICINE_SUMMARY) ~ "PrEP",
            TRUE ~ "Visit"
         )
      )
}