##  inputs ---------------------------------------------------------------------

tly <- list(
   convert = list(
      addr  = read_sheet("1r8CVfX16oDSStwLfIQdExyA-X21QuGnKwZp1AGDNXdc", "addr", range = "A:F", col_types = "c"),
      staff = read_sheet("1r8CVfX16oDSStwLfIQdExyA-X21QuGnKwZp1AGDNXdc", "staff", range = "A:C", col_types = "c"),
      sites = read_sheet("1r8CVfX16oDSStwLfIQdExyA-X21QuGnKwZp1AGDNXdc", "sites", range = "A:C", col_types = "c")
   )
)

#  uploaded --------------------------------------------------------------------
min <- min(tly$hts$RECORD_DATE, na.rm = TRUE)
max <- max(tly$hts$RECORD_DATE, na.rm = TRUE)
tly$sites <- ohasis$ref_faci %>%
   filter(str_detect(FACI_NAME, "LoveYourself"))

# min             <- "2024-01-01"
# max             <- "2024-05-21"
db              <- "ohasis_warehouse"
lw_conn         <- ohasis$conn("lw")
tly$prev_upload <- QB$new(lw_conn)$
   from("ohasis_warehouse.form_hts")$
   whereBetween("RECORD_DATE", c(as.character(min), as.character(max)))$
   whereIn("FACI_ID", unique(tly$convert$sites$FACI_ID))$
   get()
# tly$prev_upload <- dbTable(
#    lw_conn,
#    db,
#    "form_hts",
#    raw_where = TRUE,
#    where     = glue(r"(
#          (RECORD_DATE BETWEEN '{min(as.character(tly$hts$RECORD_DATE))}' AND '{max(as.character(tly$hts$RECORD_DATE))}') AND
#             FACI_ID IN ('{stri_c(collapse = "','", tly$convert$sites$FACI_ID)}')
#    )")
# )
tly$prev_upload %<>%
   mutate_if(
      .predicate = is.POSIXct,
      ~as.Date(.)
   ) %>%
   mutate_all(~as.character(.))
TIMESTAMP <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
dbDisconnect(lw_conn)

##  new data -------------------------------------------------------------------

tly$hts <- read_rds("H:/20240917_tly-hts.rds") %>%
   rename(
      PATIENT_CODE  = CLIENT_CODE,
      PROVIDER      = PROVIDER_ID,
      CLINIC_NOTES  = REMARKS,
      COUNSEL_NOTES = KEY_POPULATION
   ) %>%
   mutate(
      RECORD_DATE = case_when(
         RECORD_DATE == "2004-05-30" ~ as.Date("2024-05-30"),
         RECORD_DATE == "2022-07-08" & SHEET == "JULY 2023" ~ as.Date("2023-07-08"),
         TRUE ~ RECORD_DATE
      )
   ) %>%
   left_join(
      y  = tly$convert$sites,
      by = join_by(BRANCH)
   ) %>%
   left_join(
      y  = tly$convert$staff %>%
         filter(USER_ID != "0000000000") %>%
         rename(SERVICE_BY = USER_ID),
      by = join_by(PROVIDER, BRANCH)
   ) %>%
   left_join(
      y  = tly$convert$addr %>%
         # filter(REG != "000000000") %>%
         select(
            BRANCH,
            CURR_MUNC      = MUNCITY,
            CURR_PSGC_REG  = REG,
            CURR_PSGC_PROV = PROV,
            CURR_PSGC_MUNC = MUNC,
         ) %>%
         distinct_all(),
      by = join_by(CURR_MUNC, BRANCH)
   ) %>%
   left_join(
      y  = tly$convert$addr %>%
         # filter(REG != "000000000") %>%
         select(
            BRANCH,
            PERM_MUNC      = MUNCITY,
            PERM_PSGC_REG  = REG,
            PERM_PSGC_PROV = PROV,
            PERM_PSGC_MUNC = MUNC,
         ) %>%
         distinct_all(),
      by = join_by(PERM_MUNC, BRANCH)
   ) %>%
   mutate_at(
      .vars = vars(contains("PSGC")),
      ~na_if(., "000000000")
   ) %>%
   mutate(
      SERVICE_FACI     = FACI_ID,
      SERVICE_SUB_FACI = SUB_FACI_ID,

      SEX              = case_when(
         SEX == "MALE" ~ "1_Male",
         SEX == "FEMALE" ~ "2_Female",
         TRUE ~ SELF_IDENT
      ),

      SELF_IDENT_OTHER = case_when(
         SELF_IDENT == "TGM" ~ SELF_IDENT,
         SELF_IDENT == "TGW" ~ SELF_IDENT,
         SELF_IDENT == "TGF" ~ SELF_IDENT,
         SELF_IDENT == "QUEER" ~ SELF_IDENT,
         SELF_IDENT == "OTHER" ~ SELF_IDENT,
         SELF_IDENT == "NON-BINARY" ~ SELF_IDENT,
         TRUE ~ NA_character_
      ),
      SELF_IDENT       = case_when(
         SELF_IDENT == "MALE" ~ "1_Man",
         SELF_IDENT == "FEMALE" ~ "2_Woman",
         !is.na(SELF_IDENT) ~ "3_Other",
         TRUE ~ SELF_IDENT
      ),

      T0_RESULT        = case_when(
         T0_RESULT == "REACTIVE" ~ "1_Reactive",
         T0_RESULT == "NON REACTIVE" ~ "2_Non-reactive",
         T0_RESULT == "NON-REACTIVE" ~ "2_Non-reactive",
         TRUE ~ T0_RESULT
      ),
      PREV_TEST_RESULT = case_when(
         PREV_TEST_RESULT == "POSITIVE" ~ "1_Positive",
         PREV_TEST_RESULT == "REACTIVE" ~ "1_Positive",
         PREV_TEST_RESULT == "NEGATIVE" ~ "2_Negative",
         PREV_TEST_RESULT == "N" ~ "2_Negative",
         PREV_TEST_RESULT == "INDETERMINATE" ~ "3_Indeterminate",
         PREV_TEST_RESULT == "DON'T KNOW" ~ "4_Was not able to get result",
         PREV_TEST_RESULT == "DID NOT GET RESULT" ~ "4_Was not able to get result",
         TRUE ~ PREV_TEST_RESULT
      ),

      MODALITY         = "101101_Facility-based Testing (FBT)",
      row_id           = row_number(),
   ) %>%
   # fix risk
   mutate_at(
      .vars = vars(
         starts_with("EXPOSE") & !contains("DATE"),
      ),
      ~case_when(
         . == "0" ~ "0_No",
         . == "No" ~ "0_No",
         . == "NO" ~ "0_No",
         . == "no" ~ "0_No",
         . == "1" ~ "1_Yes",
         . == "Yes" ~ "1_Yes",
         . == "YES" ~ "1_Yes",
         . == "yes" ~ "1_Yes",
         TRUE ~ coalesce(., '0_No')
      )
   ) %>%
   mutate(
      EXPOSE_SEX_M             = if_else(if_any(c(EXPOSE_SEX_M_AV_DATE, EXPOSE_SEX_M_AV_NOCONDOM_DATE), ~!is.na(.)), "1_Yes", "0_No"),
      EXPOSE_SEX_M_AV          = EXPOSE_SEX_M,
      EXPOSE_SEX_M_AV_NOCONDOM = if_else(!is.na(EXPOSE_SEX_M_AV_NOCONDOM_DATE), "1_Yes", "0_No"),

      EXPOSE_SEX_F             = if_else(if_any(c(EXPOSE_SEX_F_AV_DATE, EXPOSE_SEX_F_AV_NOCONDOM_DATE), ~!is.na(.)), "1_Yes", "0_No"),
      EXPOSE_SEX_F_AV          = EXPOSE_SEX_F,
      EXPOSE_SEX_F_AV_NOCONDOM = if_else(!is.na(EXPOSE_SEX_F_AV_NOCONDOM_DATE), "1_Yes", "0_No"),
   ) %>%
   mutate(
      TEST_REASON_HIV_EXPOSE = case_when(
         str_detect(TEST_REASON_OTHER_TEXT, "POSSIBLE EXPOS") ~ "1_Yes",
         TRUE ~ NA_character_
      ),
      TEST_REASON_OTHER      = if_else(!is.na(TEST_REASON_OTHER_TEXT), "1_Yes", NA_character_)
   ) %>%
   mutate(
      PREV_TESTED = if_else(!is.na(PREV_TEST_DATE), "1_Yes", NA_character_)
   ) %>%
   add_missing_columns(tly$prev_upload)

#  uploaded --------------------------------------------------------------------

tly$hts %<>%
   select(-any_of(c("PATIENT_ID", "REC_ID"))) %>%
   mutate(
      UIC = StrLeft(UIC, 14)
   ) %>%
   # get records id if existing
   left_join(
      y  = tly$prev_upload %>%
         mutate(BIRTHDATE = as.Date(BIRTHDATE)) %>%
         select(
            PATIENT_ID,
            UIC,
            FIRST,
            MIDDLE,
            LAST,
            SUFFIX,
            SEX,
            BIRTHDATE,
            CLIENT_MOBILE,
            CLIENT_EMAIL
         ) %>%
         distinct(
            UIC,
            FIRST,
            MIDDLE,
            LAST,
            SUFFIX,
            SEX,
            BIRTHDATE,
            CLIENT_MOBILE,
            CLIENT_EMAIL,
            .keep_all = TRUE
         ),
      by = join_by(
         UIC,
         FIRST,
         MIDDLE,
         LAST,
         SUFFIX,
         SEX,
         BIRTHDATE,
         CLIENT_MOBILE,
         CLIENT_EMAIL
      )
   ) %>%
   # get records id if existing
   select(-CREATED_BY, -CREATED_AT) %>%
   left_join(
      y  = tly$prev_upload %>%
         mutate(
            RECORD_DATE = if_else(RECORD_DATE < -25567, T0_DATE, RECORD_DATE, RECORD_DATE),
            RECORD_DATE = as.Date(RECORD_DATE),
            CREATED_AT  = as.Date(CREATED_AT),
         ) %>%
         select(
            REC_ID,
            PATIENT_ID,
            CREATED_BY,
            CREATED_AT,
            RECORD_DATE
         ),
      by = join_by(PATIENT_ID, RECORD_DATE)
   ) %>%
   mutate(
      CREATED_BY = coalesce(CREATED_BY, SERVICE_BY, "1300000048"),
      CREATED_AT = coalesce(CREATED_AT, RECORD_DATE),
      # CREATED_AT   = format(CREATED_AT, "%Y-%m-%d 00:00:00"),
   ) %>%
   distinct(row_id, .keep_all = TRUE)

tly$check <- list(
   addr  = bind_rows(
      tly$hts %>%
         filter(
            is.na(PERM_PSGC_REG),
            !is.na(PERM_MUNC)
         ) %>%
         distinct(
            ENCODED_MUNC = PERM_MUNC
         ),
      tly$hts %>%
         filter(
            is.na(CURR_PSGC_REG),
            !is.na(CURR_MUNC)
         ) %>%
         distinct(
            ENCODED_MUNC = CURR_MUNC
         ),
   ) %>%
      distinct_all(),
   staff = tly$hts %>%
      filter(
         is.na(SERVICE_BY),
         !is.na(PROVIDER)
      ) %>%
      distinct(PROVIDER)
)

tly$import <- tly$hts %>%
   # filter(RECORD_DATE >= "2024-01-01") %>%
   mutate(
      MODULE       = "2_Testing",
      DISEASE      = "101000_HIV",
      old_rec      = if_else(!is.na(REC_ID), 1, 0, 0),
      # UPDATED_BY   = if_else(old_rec == 1, "1300000048", NA_character_),
      # UPDATED_AT   = if_else(old_rec == 1, TIMESTAMP, NA_character_),
      SERVICE_TYPE = MODALITY,
      TEST_TYPE    = "10",
      TEST_NUM     = 1,
      RESULT       = T0_RESULT,
      DATE_PERFORM = T0_DATE,
      PERFORM_BY   = CREATED_BY,
      FORM         = "HTS Form",
      VERSION      = "2021",
   )

tly$import %<>%
   filter(!is.na(PATIENT_ID)) %>%
   bind_rows(
      batch_px_ids(tly$import %>% filter(is.na(PATIENT_ID)), PATIENT_ID, FACI_ID, "row_id")
   )

tly$import %<>%
   filter(!is.na(REC_ID)) %>%
   bind_rows(
      batch_rec_ids(tly$import %>% filter(is.na(REC_ID)), REC_ID, CREATED_BY, "row_id")
   )

match_vars <- intersect(names(tly$import), names(tly$prev_upload))
match_vars <- match_vars[!(match_vars %in% c("PRIME", "UPDATED_BY", "UPDATED_AT", "DELETED_BY", "DELETED_AT", "FORM_VERSION", "DISEASE", "HTS_MSM", "HTS_TGW", "SNAPSHOT", "CONFIRMATORY_CODE", "PHILHEALTH_NO", "PHILSYS_ID"))]
match_vars <- match_vars[!(match_vars %in% c("EXPOSE_HIV_MOTHER", "EXPOSE_SEX_M_AV_NOCONDOM", "EXPOSE_SEX_M_AV_NOCONDOM_DATE", "EXPOSE_SEX_F_AV_NOCONDOM", "EXPOSE_SEX_F_AV_NOCONDOM_DATE", "SERVICE_BY", "TEST_REASON_OTHER_TEXT"))]
match_vars <- match_vars[!(match_vars %in% c("AGE_MO", "GENDER_AFFIRM_THERAPY", "IS_PREGNANT"))]
# tly$import %<>%
#    anti_join(tly$prev_upload, join_by(PATIENT_ID, RECORD_DATE))
tly$import %<>%
   # select(-HTS_FACI) %>%
   mutate_if(
      .predicate = is.Date,
      ~as.character(.)
   ) %>%
   mutate_if(
      .predicate = is.logical,
      ~as.character(.)
   ) %>%
   # fix risk
   mutate_at(
      .vars = vars(
         starts_with("EXPOSE") & !contains("DATE"),
      ),
      ~coalesce(., '0_No')
   ) %>%
   mutate(
      EXPOSE_OCCUPATION  = if_else(EXPOSE_OCCUPATION == '0_No' & !is.na(EXPOSE_OCCUPATION_DATE), '1_Yes', EXPOSE_OCCUPATION, EXPOSE_OCCUPATION),
      EXPOSE_SEX_PAYMENT = if_else(EXPOSE_SEX_PAYMENT == '0_No' & !is.na(EXPOSE_SEX_PAYMENT_DATE), '1_Yes', EXPOSE_SEX_PAYMENT, EXPOSE_SEX_PAYMENT),
      EXPOSE_SEX_PAYING  = if_else(EXPOSE_SEX_PAYING == '0_No' & !is.na(EXPOSE_SEX_PAYING_DATE), '1_Yes', EXPOSE_SEX_PAYING, EXPOSE_SEX_PAYING),
   ) %>%
   anti_join(
      y  = tly$prev_upload %>%
         left_join(
            y  = ohasis$ref_country %>%
               select(
                  NATIONALITY = COUNTRY_NAME,
                  COUNTRY_CODE
               ),
            by = join_by(NATIONALITY)
         ) %>%
         select(-NATIONALITY) %>%
         rename(NATIONALITY = COUNTRY_CODE) %>%
         left_join(
            y  = ohasis$ref_country %>%
               select(
                  OFW_COUNTRY = COUNTRY_NAME,
                  COUNTRY_CODE
               ),
            by = join_by(OFW_COUNTRY)
         ) %>%
         select(-OFW_COUNTRY) %>%
         rename(OFW_COUNTRY = COUNTRY_CODE) %>%
         # fix risk
         mutate_at(
            .vars = vars(
               starts_with("REFER_"),
               starts_with("TEST_REASON") & !contains("OTHER"),
               starts_with("SERVICE") &
                  !contains("TYPE") &
                  !contains("LUBE") &
                  !contains("CONDOM"),
               PREV_TESTED,
               LIVING_WITH_PARTNER,
               REFER_RETEST,
               REFER_CONFIRM,
               REFER_ART,
               IS_EMPLOYED,
               IS_STUDENT,
               IS_PREGNANT,
            ),
            ~na_if(., "0_No")
         ),
      by = match_vars
   )

rec_id <- "2024041517F98J41300010106"
tly$import %>%
   filter(REC_ID == rec_id) %>%
   select(any_of(match_vars)) %>%
   bind_rows(tly$prev_upload %>%
                filter(REC_ID == rec_id) %>%
                select(any_of(match_vars))) %>%
   View('same rec')

tly$prev_upload %>%
   left_join(
      y  = ohasis$ref_country %>%
         select(
            NATIONALITY = COUNTRY_NAME,
            COUNTRY_CODE
         ),
      by = join_by(NATIONALITY)
   ) %>%
   select(-NATIONALITY) %>%
   rename(NATIONALITY = COUNTRY_CODE) %>%
   left_join(
      y  = ohasis$ref_country %>%
         select(
            OFW_COUNTRY = COUNTRY_NAME,
            COUNTRY_CODE
         ),
      by = join_by(OFW_COUNTRY)
   ) %>%
   select(-OFW_COUNTRY) %>%
   rename(OFW_COUNTRY = COUNTRY_CODE) %>%
   # fix risk
   mutate_at(
      .vars = vars(
         starts_with("REFER_"),
         starts_with("TEST_REASON") & !contains("OTHER"),
         starts_with("SERVICE") &
            !contains("TYPE") &
            !contains("LUBE") &
            !contains("CONDOM"),
         PREV_TESTED,
         LIVING_WITH_PARTNER,
         REFER_RETEST,
         REFER_CONFIRM,
         REFER_ART,
         IS_EMPLOYED,
         IS_STUDENT,
         IS_PREGNANT,
      ),
      ~na_if(., "0_No")
   ) %>%
   select(any_of(match_vars)) %>%
   filter(PATIENT_ID == "20231201990005003E") %>%
   write_clip()

tly$import %>%
   select(any_of(match_vars)) %>%
   filter(PATIENT_ID == "20231201990005003E") %>%
   write_clip()

tly$import %<>%
   mutate_at(
      .vars = vars(
         DISEASE,
         MODULE,
         SEX,
         SELF_IDENT,
         CIVIL_STATUS,
         EDUC_LEVEL,
         LIVING_WITH_PARTNER,
         CLIENT_TYPE,
         PROVIDER_TYPE,
         MODALITY,
         T0_RESULT,
         PREV_TEST_RESULT,
         IS_PREGNANT,
         IS_STUDENT,
         IS_EMPLOYED,
         IS_OFW,
         SCREEN_AGREED,
         CLINICAL_PIC,
         WHO_CLASS,
         REFER_ART,
         REFER_CONFIRM,
         REFER_RETEST,
         OFW_STATION,
         PREV_TESTED
      ),
      ~keep_code(.)
   ) %>%
   mutate(
      CREATED_AT   = if_else(str_length(CREATED_AT) == 10, stri_c(CREATED_AT, " 00:00:00"), CREATED_AT, CREATED_AT),
      UPDATED_BY   = "1300000048",
      UPDATED_AT   = TIMESTAMP,
      SERVICE_TYPE = keep_code(SERVICE_TYPE),
      PRIME        = 0
   )

tly$import %<>%
   distinct(REC_ID, PATIENT_ID, .keep_all = TRUE)

tly$import %<>%
   left_join(
      y=  tly$prev_upload %>%
         select(REC_ID, CORR_PID = PATIENT_ID),
      by = join_by(REC_ID)
   ) %>%
   mutate(
      PATIENT_ID = coalesce(CORR_PID, PATIENT_ID),
   ) %>%
   select(-CORR_PID)

tly$tables <- deconstruct_hts(tly$import %>% select(-CURR_MUNC, -PERM_MUNC, -SERVICE))
wide       <- c("px_test_refuse", "px_other_service", "px_reach", "px_med_profile", "px_test_reason")
delete     <- tly$tables$px_record$data %>% select(REC_ID)

db_conn <- ohasis$conn("db")
lapply(wide, function(table) dbxDelete(db_conn, Id(schema = "ohasis_interim", table = table), delete))
# lapply(wide, function(table) dbExecute(conn = db_conn, statement = glue("DELETE FROM ohasis_interim.{table} WHERE REC_ID IN (?)"), params = list(tly$import$REC_ID)))

lapply(tly$tables, function(ref, db_conn) {
   log_info("Uploading {green(ref$name)}.")
   table_space <- Id(schema = "ohasis_interim", table = ref$name)
   dbxUpsert(db_conn, table_space, ref$data, ref$pk)
   # dbExecute(db_conn, glue("DELETE FROM ohasis_interim.{ref$name} WHERE REC_ID IN (?)"), params = list(unique(ref$data$REC_ID)))
}, db_conn)
dbDisconnect(db_conn)
