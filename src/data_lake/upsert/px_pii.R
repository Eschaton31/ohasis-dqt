##  Person Identifiable Information --------------------------------------------

continue <- 0
id_col   <- "REC_ID"
object   <- tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_record")) %>%
   filter(
      (CREATED_AT >= snapshot_old & CREATED_AT <= snapshot_new) |
         (UPDATED_AT >= snapshot_old & UPDATED_AT <= snapshot_new) |
         (DELETED_AT >= snapshot_old & DELETED_AT <= snapshot_new)
   )

# get number of affected rows
if ((object %>% count() %>% collect())$n > 0) {
   continue <- 1
   object   <- object %>%
      mutate(
         DISEASE = case_when(
            DISEASE == "101000" ~ "HIV",
            DISEASE == "102000" ~ "Hepatitis B",
            DISEASE == "103000" ~ "Hepatitis C",
            TRUE ~ DISEASE
         ),
         MODULE  = case_when(
            MODULE == "0" ~ "0_Client Add/Update",
            MODULE == "1" ~ "1_General",
            MODULE == "2" ~ "2_Testing",
            MODULE == "3" ~ "3_Treatment",
            MODULE == "4" ~ "4_Modality",
            MODULE == "6" ~ "6_Prevention",
            TRUE ~ MODULE
         )
      ) %>%
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_info")) %>%
            mutate(
               SEX = case_when(
                  SEX == 1 ~ '1_Male',
                  SEX == 2 ~ '2_Female',
                  TRUE ~ as.character(SEX)
               )
            ) %>%
            select(
               REC_ID,
               CONFIRMATORY_CODE,
               UIC,
               PHILHEALTH_NO,
               SEX,
               BIRTHDATE,
               PATIENT_CODE,
               PHILSYS_ID
            ),
         by = "REC_ID"
      ) %>%
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_name")) %>%
            select(
               REC_ID,
               FIRST,
               MIDDLE,
               LAST,
               SUFFIX
            ),
         by = "REC_ID"
      ) %>%
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_faci")) %>%
            mutate(
               SERVICE_TYPE = case_when(
                  SERVICE_TYPE == '*00001' ~ 'Mortality',
                  SERVICE_TYPE == '101101' ~ 'HIV FBT',
                  SERVICE_TYPE == '101102' ~ 'HIV FBT',
                  SERVICE_TYPE == '101103' ~ 'CBS',
                  SERVICE_TYPE == '101104' ~ 'FBS',
                  SERVICE_TYPE == '101105' ~ 'ST',
                  SERVICE_TYPE == '101201' ~ 'ART',
                  SERVICE_TYPE == '101301' ~ 'PrEP',
                  SERVICE_TYPE == '101303' ~ 'PMTCT-N',
                  SERVICE_TYPE == '101304' ~ 'Reach',
                  SERVICE_TYPE == '' ~ NA_character_,
                  TRUE ~ SERVICE_TYPE
               )
            ) %>%
            select(
               REC_ID,
               SERVICE_TYPE
            ) %>%
            group_by(REC_ID) %>%
            summarise(
               SERVICE_TYPE = SERVICE_TYPE
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
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_form")) %>%
            filter(FORM != '') %>%
            mutate(
               FORM_VERSION = if_else(
                  !is.na(FORM) & FORM != '',
                  paste0(FORM, ' (v', VERSION, ')'),
                  NA_character_
               )
            ) %>%
            select(
               REC_ID,
               FORM_VERSION
            ),
         by = "REC_ID"
      ) %>%
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_profile")) %>%
            left_join(
               y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "addr_country")) %>%
                  select(
                     NATIONALITY = COUNTRY_CODE,
                     COUNTRY_NAME
                  ),
               by = 'NATIONALITY'
            ) %>%
            mutate(
               SELF_IDENT   = case_when(
                  SELF_IDENT == 1 ~ '1_Male',
                  SELF_IDENT == 2 ~ '2_Female',
                  SELF_IDENT == 3 ~ '3_Other',
                  TRUE ~ NA_character_
               ),
               EDUC_LEVEL   = case_when(
                  EDUC_LEVEL == 1 ~ '1_None',
                  EDUC_LEVEL == 2 ~ '2_Elementary',
                  EDUC_LEVEL == 3 ~ '3_High School',
                  EDUC_LEVEL == 4 ~ '4_College',
                  EDUC_LEVEL == 5 ~ '5_Vocational',
                  EDUC_LEVEL == 6 ~ '6_Post-Graduate',
                  EDUC_LEVEL == 7 ~ '7_Pre-school',
                  TRUE ~ NA_character_
               ),
               CIVIL_STATUS = case_when(
                  CIVIL_STATUS == 1 ~ '1_Single',
                  CIVIL_STATUS == 2 ~ '2_Married',
                  CIVIL_STATUS == 3 ~ '3_Separated',
                  CIVIL_STATUS == 4 ~ '4_Widowed',
                  CIVIL_STATUS == 5 ~ '5_Divorced',
                  TRUE ~ NA_character_
               )
            ) %>%
            mutate_at(
               .vars = vars(GENDER_AFFIRM_THERAPY, LIVING_WITH_PARTNER),
               ~case_when(
                  . == 1 ~ '1_Yes',
                  . == 0 ~ '0_No',
                  TRUE ~ NA_character_
               )
            ) %>%
            select(
               REC_ID,
               AGE,
               AGE_MO,
               GENDER_AFFIRM_THERAPY,
               SELF_IDENT,
               SELF_IDENT_OTHER,
               NATIONALITY = COUNTRY_NAME,
               NATIONALITY_OTHER,
               EDUC_LEVEL,
               CIVIL_STATUS,
               LIVING_WITH_PARTNER,
               CHILDREN
            ),
         by = "REC_ID"
      ) %>%
      left_join(
         y  = tbl(db_conn, dbplyr::in_schema("ohasis_interim", "px_addr")) %>%
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
               id_cols     = c(REC_ID, ADDR_TYPE),
               names_from  = ADDR_TYPE,
               values_from = c(PSGC_REG, PSGC_PROV, PSGC_MUNC, ADDR),
               names_glue  = "{ADDR_TYPE}_{.value}"
            ) %>%
            select(
               REC_ID,
               starts_with("CURR_"),
               starts_with("PERM_"),
               starts_with("BIRTH_"),
               starts_with("DEATH_"),
               starts_with("SERVICE_")
            ),
         by = "REC_ID"
      ) %>%
      relocate(
         FORM_VERSION,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
         DELETED_BY,
         DELETED_AT,
         SNAPSHOT,
         .after = PATIENT_ID
      ) %>%
      collect()
}