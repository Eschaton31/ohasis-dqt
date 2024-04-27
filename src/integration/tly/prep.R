##  inputs ---------------------------------------------------------------------

file <- "D:/20240417_tly-prep.rds"
tly  <- list(
   convert = list(
      addr  = read_sheet("1r8CVfX16oDSStwLfIQdExyA-X21QuGnKwZp1AGDNXdc", "addr", range = "A:F", col_types = "c"),
      staff = read_sheet("1r8CVfX16oDSStwLfIQdExyA-X21QuGnKwZp1AGDNXdc", "staff", range = "A:C", col_types = "c")
   )
)

##  processing -----------------------------------------------------------------

tly$visits <- read_rds(file)
min        <- min(tly$visits$RECORD_DATE, na.rm = TRUE)
max        <- max(tly$visits$RECORD_DATE, na.rm = TRUE)

#  uploaded --------------------------------------------------------------------

db              <- "ohasis_warehouse"
lw_conn         <- ohasis$conn("lw")
tly$prev_upload <- dbTable(
   lw_conn,
   db,
   "form_prep",
   raw_where = TRUE,
   where     = glue(r"(
         (VISIT_DATE BETWEEN '{min}' AND '{max}') AND
            (CREATED_BY = '1300000048' OR UPDATED_BY = '1300000048')
   )")
)
tly$prev_upload %<>%
   mutate_if(
      .predicate = is.POSIXct,
      ~as.Date(.)
   )
dbDisconnect(lw_conn)

##  new data -------------------------------------------------------------------

tly$records <- tly$visits %>%
   select(-PATIENT_ID) %>%
   rename(
      PROVIDER      = DISP_BY,
      PATIENT_ID    = CENTRAL_ID,
      PATIENT_CODE  = CLIENT_CODE,
      COUNSEL_NOTES = KEY_POPULATION,
   ) %>%
   mutate(
      SERVICE_FACI     = FACI_ID,
      SERVICE_SUB_FACI = SUB_FACI_ID,

      MEDICINE_SUMMARY = if_else(!is.na(DISP_BOTTLES), "TDF/FTC", NA_character_),
      TOTAL_DISP       = as.integer(parse_number(DISP_BOTTLES)),
      LATEST_NEXT_DATE = DISP_DATE %m+% days(TOTAL_DISP * 30),
      UNIT_BASIS       = "1",

      BIRTHDATE        = as.character(BIRTHDATE),
      BIRTHDATE        = if_else(
         is.na(BIRTHDATE) & nchar(UIC) == 14,
         stri_c(sep = "-", StrRight(UIC, 4), substr(UIC, 7, 8), substr(UIC, 9, 10)),
         BIRTHDATE,
         BIRTHDATE
      ),
      BIRTHDATE        = as.Date(BIRTHDATE),

      AGE              = as.integer(AGE),

      DISP_DATE        = RECORD_DATE,
      FORM_VERSION     = case_when(
         str_detect(SHEET, "INITIAL") ~ "PrEP Screening (v2020)",
         str_detect(SHEET, "REFILL") ~ "PrEP Follow-up (v2020)",
      ),
      FORM             = case_when(
         str_detect(SHEET, "INITIAL") ~ "PrEP Screening",
         str_detect(SHEET, "REFILL") ~ "PrEP Follow-up",
      ),
      VERSION          = case_when(
         str_detect(SHEET, "INITIAL") ~ "2020",
         str_detect(SHEET, "REFILL") ~ "2020",
      ),

      KP_PDL           = case_when(
         str_detect(COUNSEL_NOTES, "PDL") ~ "1_Yes",
      ),
      KP_SW            = case_when(
         str_detect(COUNSEL_NOTES, "SEX WORKER") ~ "1_Yes",
      ),
      KP_TG            = case_when(
         str_detect(COUNSEL_NOTES, "TGP") ~ "1_Yes",
         str_detect(COUNSEL_NOTES, "TGW") ~ "1_Yes",
         str_detect(COUNSEL_NOTES, "TP") ~ "1_Yes",
         str_detect(COUNSEL_NOTES, "TWG") ~ "1_Yes",
         str_detect(COUNSEL_NOTES, "TRANSGENDER") ~ "1_Yes",
      ),
      KP_PWID          = case_when(
         str_detect(COUNSEL_NOTES, "PWID") ~ "1_Yes",
         str_detect(COUNSEL_NOTES, "PEOPLE WHO INJECTED DRUG") ~ "1_Yes",
      ),
      KP_MSM           = case_when(
         str_detect(COUNSEL_NOTES, "MSM") ~ "1_Yes",
      ),
      KP_OFW           = case_when(
         str_detect(COUNSEL_NOTES, "OFW") ~ "1_Yes",
      ),
      KP_PARTNER       = case_when(
         str_detect(COUNSEL_NOTES, "PARTNER") ~ "1_Yes",
      ),
      KP_OTHER         = case_when(
         str_detect(COUNSEL_NOTES, "OTHERS") ~ "1_Yes",
         if_all(c(KP_PDL, KP_SW, KP_TG, KP_PWID, KP_MSM, KP_OFW, KP_PARTNER), ~is.na(.)) & !is.na(COUNSEL_NOTES) ~ "Y1_es",
      ),
   ) %>%
   mutate_at(
      .vars = vars(
         starts_with("ARS_SX") & !contains("DATE"),
         starts_with("STI_SX") & !contains("DATE"),
      ),
      ~coalesce(., "0_No")
   ) %>%
   left_join(
      y  = tly$convert$staff %>%
         filter(USER_ID != "0000000000") %>%
         select(PROVIDER, BRANCH, USER_ID) %>%
         distinct_all(),
      by = join_by(PROVIDER, BRANCH)
   ) %>%
   mutate(row_id = row_number()) %>%
   # get records id if existing
   left_join(
      y  = tly$prev_upload %>%
         select(
            REC_ID,
            CREATED_BY,
            CREATED_AT,
            PATIENT_CODE,
            CONFIRMATORY_CODE,
            LAST,
            FIRST,
            MIDDLE,
            SUFFIX,
            UIC,
            BIRTHDATE,
            PHILHEALTH_NO,
            SEX,
            CLIENT_EMAIL,
            CLIENT_MOBILE,
            RECORD_DATE
         ),
      by = join_by(
         PATIENT_CODE,
         CONFIRMATORY_CODE,
         LAST,
         FIRST,
         MIDDLE,
         SUFFIX,
         UIC,
         BIRTHDATE,
         PHILHEALTH_NO,
         SEX,
         CLIENT_EMAIL,
         CLIENT_MOBILE,
         RECORD_DATE
      )
   ) %>%
   distinct(row_id, .keep_all = TRUE) %>%
   mutate(
      SERVICE_BY = USER_ID,
      DISP_BY    = USER_ID,
      VISIT_DATE = RECORD_DATE,
      CREATED_BY = coalesce(CREATED_BY, USER_ID, "1300000048"),
      CREATED_AT = coalesce(CREATED_AT, RECORD_DATE),
      DISEASE    = "101000_HIV",
      MODULE     = "6_Prevention",
   ) %>%
   add_missing_columns(tly$prev_upload) %>%
   select(
      -any_of(c(
         "NUM_OF_DRUGS",
         "PREP_RECORD",
         "FACI_DISP",
         "SUB_FACI_DISP",
         "REC_ID_GRP",
         "SNAPSHOT",
         "PREP_STATUS",
         "DISEASE",
         'PREP_SIDE_EFFECTS', 'PREP_ACCEPTED', 'ELIGIBLE_PREP', 'PREP_VISIT', 'PREP_ACCEPTED', 'PREP_CONTINUED', 'PREP_REQUESTED'
      ))
   ) %>%
   distinct_all()


TIMESTAMP <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
tly$records %<>%
   # retain only not uploaded and those with changes
   filter(!is.na(PATIENT_ID)) %>%
   mutate(
      UPDATED_AT = NA_Date_,
      UPDATED_BY = NA_character_,
   ) %>%
   anti_join(
      y  = tly$prev_upload %>%
         mutate(
            UPDATED_AT = NA_Date_,
            UPDATED_BY = NA_character_,
         ) %>%
         mutate_if(
            .predicate = is.Date,
            ~as.Date(.)
         ),
      by = join_by(!!!intersect(names(tly$records), names(tly$prev_upload)))
   ) %>%
   mutate(
      old_rec    = if_else(!is.na(REC_ID), 1, 0, 0),
      CREATED_BY = coalesce(CREATED_BY, "1300000048"),
      CREATED_AT = coalesce(as.character(CREATED_AT), TIMESTAMP),
      UPDATED_BY = if_else(old_rec == 1, "1300000048", NA_character_),
      UPDATED_AT = if_else(old_rec == 1, TIMESTAMP, NA_character_)
   ) %>%
   relocate(any_of(names(tly$prev_upload)), .before = 1) %>%
   select(-old_rec) %>%
   mutate(
      row_id = row_number()
   )

tly$import <- tly$records %>%
   filter(!is.na(REC_ID)) %>%
   bind_rows(
      batch_rec_ids(tly$records %>% filter(is.na(REC_ID)), REC_ID, CREATED_BY, "row_id")
   )


tly$import %<>%
   mutate(
      UPDATED_BY = "1300000048",
      UPDATED_AT = TIMESTAMP
   )
##  table formats
tables           <- list()
tables$px_record <- list(
   name = "px_record",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = tly$import %>%
      mutate(
         DISEASE = "101000",
         MODULE  = "6",
      ) %>%
      mutate_at(
         .vars = vars(MODULE, DISEASE),
         ~keep_code(.)
      ) %>%
      select(
         REC_ID,
         PATIENT_ID,
         FACI_ID,
         SUB_FACI_ID,
         RECORD_DATE,
         DISEASE,
         MODULE,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      )
)

tables$px_info <- list(
   name = "px_info",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = tly$import %>%
      mutate_at(
         .vars = vars(SEX),
         ~keep_code(.)
      ) %>%
      select(
         REC_ID,
         PATIENT_ID,
         CONFIRMATORY_CODE,
         UIC,
         PATIENT_CODE,
         SEX,
         BIRTHDATE,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      ) %>%
      mutate_at(
         .vars = vars(SEX),
         ~keep_code(.)
      )
)

tables$px_name <- list(
   name = "px_name",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = tly$import %>%
      select(
         REC_ID,
         PATIENT_ID,
         FIRST,
         MIDDLE,
         LAST,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      )
)

tables$px_contact <- list(
   name = "px_contact",
   pk   = c("REC_ID", "CONTACT_TYPE"),
   data = tly$import %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
         CLIENT_MOBILE,
         CLIENT_EMAIL
      ) %>%
      pivot_longer(
         cols      = c(CLIENT_MOBILE, CLIENT_EMAIL),
         names_to  = "CONTACT_TYPE",
         values_to = "CONTACT"
      ) %>%
      mutate(
         CONTACT_TYPE = case_when(
            CONTACT_TYPE == "CLIENT_MOBILE" ~ "1",
            CONTACT_TYPE == "CLIENT_EMAIL" ~ "2",
            TRUE ~ CONTACT_TYPE
         )
      )
)

tables$px_faci <- list(
   name = "px_faci",
   pk   = c("REC_ID", "SERVICE_TYPE"),
   data = tly$import %>%
      mutate(
         SERVICE_TYPE = "101301"
      ) %>%
      select(
         REC_ID,
         FACI_ID,
         SUB_FACI_ID,
         SERVICE_TYPE,
         CLIENT_TYPE,
         PROVIDER_ID = SERVICE_BY,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      )
)

tables$px_remarks <- list(
   name = "px_remarks",
   pk   = c("REC_ID", "REMARK_TYPE"),
   data = tly$import %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
         COUNSEL_NOTES,
      ) %>%
      pivot_longer(
         cols      = c(
            COUNSEL_NOTES,
         ),
         names_to  = "REMARK_TYPE",
         values_to = "REMARKS"
      ) %>%
      mutate(
         REMARK_TYPE = case_when(
            REMARK_TYPE == "CLINIC_NOTES" ~ "1",
            REMARK_TYPE == "COUNSEL_NOTES" ~ "2",
            REMARK_TYPE == "SYMPTOMS" ~ "10",
            TRUE ~ REMARK_TYPE
         ),
      )
)

tables$px_form <- list(
   name = "px_form",
   pk   = c("REC_ID", "FORM"),
   data = tly$import %>%
      select(
         REC_ID,
         FORM,
         VERSION,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      )
)

tables$px_profile <- list(
   name = "px_profile",
   pk   = "REC_ID",
   data = tly$import %>%
      mutate(
         AGE = coalesce(AGE, calc_age(BIRTHDATE, RECORD_DATE))
      ) %>%
      select(
         REC_ID,
         AGE,
         SELF_IDENT,
         SELF_IDENT_OTHER,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      ) %>%
      mutate_at(
         .vars = vars(SELF_IDENT),
         ~keep_code(.)
      )
)

tables$px_labs <- list(
   name = "px_labs",
   pk   = c("REC_ID", "LAB_TEST"),
   data = tly$import %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
         starts_with("LAB"),
      ) %>%
      mutate_at(
         .vars = vars(contains("_DATE")),
         ~as.character(.)
      ) %>%
      pivot_longer(
         cols      = starts_with("LAB"),
         names_to  = "LAB_DATA",
         values_to = "LAB_VALUE"
      ) %>%
      mutate(
         LAB_TEST = substr(LAB_DATA, 5, stri_locate_last_fixed(LAB_DATA, "_") - 1),
         PIECE    = substr(LAB_DATA, stri_locate_last_fixed(LAB_DATA, "_") + 1, 1000),
         PIECE    = if_else(PIECE %in% c("CLEARANCE", "TITER"), "RESULT_OTHER", PIECE, PIECE)
      ) %>%
      mutate(
         LAB_TEST = case_when(
            LAB_TEST == "HBSAG" ~ "1",
            LAB_TEST == "CREA" ~ "2",
            LAB_TEST == "SYPH" ~ "3",
            LAB_TEST == "VL" ~ "4",
            LAB_TEST == "CD4" ~ "5",
            LAB_TEST == "XRAY" ~ "6",
            LAB_TEST == "XPERT" ~ "7",
            LAB_TEST == "DSSM" ~ "8",
            LAB_TEST == "HIVDR" ~ "9",
            LAB_TEST == "HEMO" ~ "10",
            LAB_TEST == "HEMOG" ~ "10",
            TRUE ~ LAB_TEST
         )
      ) %>%
      distinct(REC_ID, CREATED_AT, CREATED_BY, LAB_TEST, PIECE, .keep_all = TRUE) %>%
      pivot_wider(
         id_cols      = c(REC_ID, CREATED_AT, CREATED_BY, LAB_TEST),
         names_from   = PIECE,
         values_from  = LAB_VALUE,
         names_prefix = "LAB_"
      ) %>%
      filter(!is.na(LAB_DATE) | !is.na(LAB_RESULT)) %>%
      arrange(REC_ID, LAB_TEST)
)

tables$px_key_pop <- list(
   name = "px_key_pop",
   pk   = c("REC_ID", "KP"),
   data = tly$import %>%
      select(
         REC_ID,
         CREATED_AT,
         CREATED_BY,
         UPDATED_BY,
         UPDATED_AT,
         contains("KP"),
         COUNSEL_NOTES
      ) %>%
      rename_all(
         ~case_when(
            . == "KP_PDL" ~ "IS_KP_1",
            . == "KP_TG" ~ "IS_KP_2",
            . == "KP_PWID" ~ "IS_KP_3",
            . == "KP_MSM" ~ "IS_KP_5",
            . == "KP_SW" ~ "IS_KP_6",
            . == "KP_OFW" ~ "IS_KP_7",
            . == "KP_PARTNER" ~ "IS_KP_8",
            . == "OTHER_KP" ~ "IS_KP_8888",
            . == "KP_OTHER" ~ "IS_KP_8888",
            TRUE ~ .
         )
      ) %>%
      pivot_longer(
         cols      = contains("KP"),
         names_to  = "KP",
         values_to = "IS_KP"
      ) %>%
      mutate(
         KP       = stri_replace_all_fixed(KP, "IS_KP_", ""),
         KP_OTHER = if_else(
            condition = KP == "8888" & !is.na(IS_KP),
            true      = COUNSEL_NOTES,
            false     = NA_character_,
            missing   = NA_character_
         ),
         IS_KP    = keep_code(IS_KP),
      ) %>%
      filter(IS_KP == 1) %>%
      select(-COUNSEL_NOTES)
)


tables$px_expose_profile <- list(
   name = "px_expose_profile",
   pk   = "REC_ID",
   data = tly$import %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
         WEEK_AVG_SEX
      ) %>%
      mutate_at(
         .vars = vars(WEEK_AVG_SEX),
         ~keep_code(.)
      )
)

tables$px_expose_hist <- list(
   name = "px_expose_hist",
   pk   = c("REC_ID", "EXPOSURE"),
   data = tly$import %>%
      select(
         REC_ID,
         CREATED_AT,
         CREATED_BY,
         UPDATED_BY,
         UPDATED_AT,
         contains("RISK"),
      ) %>%
      pivot_longer(
         cols      = starts_with("RISK_"),
         names_to  = "EXPOSURE",
         values_to = "EXPOSE_VALUE"
      ) %>%
      mutate(
         EXPOSURE         = str_replace(EXPOSURE, "^RISK_", ""),
         EXPOSURE         = case_when(
            EXPOSURE == "CONDOMLESS_ANAL" ~ "261200",
            EXPOSURE == "CONDOMLESS_VAGINAL" ~ "262200",
            EXPOSURE == "DRUG_INJECT" ~ "311010",
            EXPOSURE == "DRUG_SEX" ~ "330000",
            EXPOSURE == "TRANSACT_SEX" ~ "200030",
            EXPOSURE == "HIV_VL_UNKNOWN" ~ "200001",
            EXPOSURE == "HIV_UNKNOWN" ~ "230000",
            EXPOSURE == "STI" ~ "400000",
            EXPOSURE == "PEP" ~ "320002",
            TRUE ~ EXPOSURE
         ),
         IS_EXPOSED       = case_when(
            EXPOSE_VALUE == "4_Yes, within the past 30 days" ~ "1",
            EXPOSE_VALUE == "3_Yes, within the past 6 months" ~ "1",
            EXPOSE_VALUE == "2_Yes" ~ "1",
            EXPOSE_VALUE == "0_No" ~ "0",
         ),
         TYPE_LAST_EXPOSE = case_when(
            EXPOSE_VALUE == "4_Yes, within the past 30 days" ~ "1",
            EXPOSE_VALUE == "3_Yes, within the past 6 months" ~ "3",
            EXPOSE_VALUE == "2_Yes" ~ "0",
         )
      ) %>%
      select(-EXPOSE_VALUE)
)

tables$px_vitals <- list(
   name = "px_vitals",
   pk   = c("REC_ID", "VITAL_SIGN"),
   data = tly$import %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
         WEIGHT,
         BODY_TEMP
      ) %>%
      pivot_longer(
         names_to  = "VITAL_SIGN",
         cols      = c(WEIGHT, BODY_TEMP),
         values_to = "VITAL_RESULT"
      ) %>%
      mutate(
         VITAL_SIGN = case_when(
            VITAL_SIGN == "WEIGHT" ~ "2",
            VITAL_SIGN == "BODY_TEMP" ~ "3",
            TRUE ~ VITAL_SIGN
         )
      )
)

tables$px_ars_sx <- list(
   name = "px_ars_sx",
   pk   = c("REC_ID", "ARS_SYMPTOM"),
   data = tly$import %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
         starts_with("ARS_SX_")
      ) %>%
      distinct() %>%
      pivot_longer(
         cols      = starts_with("ARS_SX_"),
         names_to  = "ARS_SYMPTOM",
         values_to = "SYMPTOM_VALUE"
      ) %>%
      mutate(
         SYMPTOM_DATA = if_else(str_detect(ARS_SYMPTOM, "_TEXT"), "SYMPTOM_OTHER", "IS_SYMPTOM"),
         ARS_SYMPTOM  = str_replace(ARS_SYMPTOM, "^EXPOSE_", ""),
         ARS_SYMPTOM  = str_replace(ARS_SYMPTOM, "_TEXT$", ""),

         ARS_SYMPTOM  = case_when(
            ARS_SYMPTOM == "ARS_SX_FEVER" ~ "1",
            ARS_SYMPTOM == "ARS_SX_SORE_THROAT" ~ "2",
            ARS_SYMPTOM == "ARS_SX_DIARRHEA" ~ "3",
            ARS_SYMPTOM == "ARS_SX_SWOLLEN_LYMPH" ~ "4",
            ARS_SYMPTOM == "ARS_SX_SWOLLEN_TONSILS" ~ "5",
            ARS_SYMPTOM == "ARS_SX_RASH" ~ "6",
            ARS_SYMPTOM == "ARS_SX_MUSCLE_PAINS" ~ "7",
            ARS_SYMPTOM == "ARS_SX_OTHER" ~ "8888",
            ARS_SYMPTOM == "ARS_SX_OTHER_TEXT" ~ "8888",
            ARS_SYMPTOM == "ARS_SX_NONE" ~ "9999",
            TRUE ~ ARS_SYMPTOM
         ),
      ) %>%
      pivot_wider(
         names_from  = SYMPTOM_DATA,
         values_from = SYMPTOM_VALUE,
      ) %>%
      mutate(
         IS_SYMPTOM = keep_code(IS_SYMPTOM),
         IS_SYMPTOM = coalesce(IS_SYMPTOM, "0"),
      )
)
tables$px_sti_sx <- list(
   name = "px_sti_sx",
   pk   = c("REC_ID", "STI_SYMPTOM"),
   data = tly$import %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
         starts_with("STI_SX_")
      ) %>%
      distinct() %>%
      pivot_longer(
         cols      = starts_with("STI_SX_"),
         names_to  = "STI_SYMPTOM",
         values_to = "SYMPTOM_VALUE"
      ) %>%
      mutate(
         SYMPTOM_DATA = if_else(str_detect(STI_SYMPTOM, "_TEXT"), "SYMPTOM_OTHER", "IS_SYMPTOM"),
         STI_SYMPTOM  = str_replace(STI_SYMPTOM, "^EXPOSE_", ""),
         STI_SYMPTOM  = str_replace(STI_SYMPTOM, "_TEXT$", ""),

         STI_SYMPTOM  = case_when(
            STI_SYMPTOM == "STI_SX_DISCHARGE_VAGINAL" ~ "1",
            STI_SYMPTOM == "STI_SX_DISCHARGE_ANAL" ~ "2",
            STI_SYMPTOM == "STI_SX_DISCHARGE_URETHRAL" ~ "3",
            STI_SYMPTOM == "STI_SX_SWOLLEN_SCROTUM" ~ "4",
            STI_SYMPTOM == "STI_SX_PAIN_URINE" ~ "5",
            STI_SYMPTOM == "STI_SX_ULCER_GENITAL" ~ "6",
            STI_SYMPTOM == "STI_SX_ULCER_ORAL" ~ "7",
            STI_SYMPTOM == "STI_SX_WARTS_GENITAL" ~ "8",
            STI_SYMPTOM == "STI_SX_PAIN_ABDOMEN" ~ "9",
            STI_SYMPTOM == "STI_SX_OTHER" ~ "8888",
            STI_SYMPTOM == "STI_SX_OTHER_TEXT" ~ "8888",
            STI_SYMPTOM == "STI_SX_NONE" ~ "9999",
            TRUE ~ STI_SYMPTOM
         ),
      ) %>%
      pivot_wider(
         names_from  = SYMPTOM_DATA,
         values_from = SYMPTOM_VALUE,
      ) %>%
      mutate(
         IS_SYMPTOM = keep_code(IS_SYMPTOM),
         IS_SYMPTOM = coalesce(IS_SYMPTOM, "0"),
      )
)

tables$px_prep <- list(
   name = "px_prep",
   pk   = "REC_ID",
   data = tly$import %>%
      mutate(
         PREP_ACCEPTED  = if_else(!is.na(MEDICINE_SUMMARY), "1", NA_character_),
         ELIGIBLE_PREP  = if_else(!is.na(MEDICINE_SUMMARY), "1", NA_character_),
         PREP_VISIT     = case_when(
            str_detect(SHEET, "INITIAL") ~ "1",
            str_detect(SHEET, "REFILL") ~ "2",
         ),
         PREP_REQUESTED = NA_character_
      ) %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
         PREP_HIV_DATE,
         PREP_ACCEPTED,
         PREP_VISIT,
         ELIGIBLE_BEHAVIOR,
         PREP_REQUESTED,
         ELIGIBLE_PREP,
         PREP_PLAN,
         PREP_TYPE,
         FIRST_TIME
      ) %>%
      mutate_at(
         .vars = vars(PREP_PLAN, PREP_TYPE, ELIGIBLE_BEHAVIOR, FIRST_TIME),
         ~keep_code(.)
      )
)

tables$px_prep_status <- list(
   name = "px_prep_status",
   pk   = "REC_ID",
   data = tly$import %>%
      mutate(
         PREP_SIDE_EFFECTS = if_else(PREP_SIDE_EFFECTS_SPECIFY != "NONE", "1", NA_character_),
      ) %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
         PREP_TYPE_LAST_VISIT,
         PREP_SHIFT,
         PREP_MISSED,
         PREP_SIDE_EFFECTS,
         SIDE_EFFECTS = PREP_SIDE_EFFECTS_SPECIFY
      ) %>%
      mutate_at(
         .vars = vars(PREP_TYPE_LAST_VISIT, PREP_SHIFT, PREP_MISSED),
         ~keep_code(.)
      )
)


tables$px_prep_checklist <- list(
   name = "px_prep_checklist",
   pk   = c("REC_ID", "REQUIREMENT"),
   data = tly$import %>%
      select(
         REC_ID,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
         starts_with("PRE_INIT_")
      ) %>%
      rename_all(
         ~case_when(
            . == "PRE_INIT_HIV_NR" ~ "IS_CHECKED_1",
            . == "PRE_INIT_WEIGHT" ~ "IS_CHECKED_2",
            . == "PRE_INIT_NO_ARS" ~ "IS_CHECKED_3",
            . == "PRE_INIT_CREA_CLEAR" ~ "IS_CHECKED_4",
            . == "PRE_INIT_NO_ARV_ALLERGY" ~ "IS_CHECKED_5",
            TRUE ~ .
         )
      ) %>%
      pivot_longer(
         cols      = contains("CHECKED"),
         names_to  = "REQUIREMENT",
         values_to = "IS_CHECKED"
      ) %>%
      mutate(
         REQUIREMENT = stri_replace_all_fixed(REQUIREMENT, "IS_CHECKED_", ""),
         IS_CHECKED  = keep_code(IS_CHECKED),
      )
)

tables$px_medicine <- list(
   name = "px_medicine",
   pk   = c("REC_ID", "MEDICINE", "DISP_NUM"),
   data = tly$import %>%
      mutate(
         PER_DAY         = 1,
         DISP_NUM        = 1,
         MEDICINE        = if_else(!is.na(MEDICINE_SUMMARY), "2028", NA_character_),
         MEDICINE_MISSED = NA_character_
      ) %>%
      filter(!is.na(MEDICINE_SUMMARY)) %>%
      select(
         REC_ID,
         FACI_ID,
         SUB_FACI_ID,
         MEDICINE,
         DISP_NUM,
         UNIT_BASIS,
         PER_DAY,
         DISP_TOTAL    = TOTAL_DISP,
         MEDICINE_LEFT = PILLS_LEFT,
         MEDICINE_MISSED,
         DISP_DATE,
         NEXT_DATE     = LATEST_NEXT_DATE,
      )
)

db_conn <- ohasis$conn("db")
dbxDelete(
   db_conn,
   Id(schema = "ohasis_interim", table = "px_medicine"),
   tly$import %>% select(REC_ID),
   batch_size = 1000
)
lapply(tables, function(ref, db_conn) {
   log_info("Uploading {green(ref$name)}.")
   table_space <- Id(schema = "ohasis_interim", table = ref$name)
   dbxUpsert(db_conn, table_space, ref$data, ref$pk)
   # dbExecute(db_conn, glue("DELETE FROM ohasis_interim.{ref$name} WHERE REC_ID IN (?)"), params = list(unique(ref$data$REC_ID)))
}, db_conn)
dbDisconnect(db_conn)
