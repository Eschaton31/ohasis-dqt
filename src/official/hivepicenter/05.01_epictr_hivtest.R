# Form A + HTS Forms
.log_info("Getting Form A data.")
form_a <- epictr$forms$form_a %>%
   mutate(
      FORM        = "Form A",
      MODALITY    = "101101_FBT",
      TEST_AGREED = "1_Yes"
   ) %>%
   select(
      REC_ID,
      FORM,
      PATIENT_ID,
      UIC,
      BIRTHDATE,
      SEX,
      FACI_ID,
      SELF_IDENT,
      SELF_IDENT_OTHER,
      TEST_FACI     = SERVICE_FACI,
      TEST_SUB_FACI = SERVICE_SUB_FACI,
      RECORD_DATE,
      MODALITY,
      TEST_AGREED,
      TEST_DATE     = T0_DATE,
      TEST_RESULT   = T0_RESULT,
      T1_DATE,
      T1_RESULT,
      T2_DATE,
      T2_RESULT,
      T3_DATE,
      T3_RESULT,
      DATE_CONFIRM,
      CONFIRM_RESULT,
      AGE
   )

.log_info("Getting HTS Form data.")
form_hts <- epictr$forms$form_hts %>%
   mutate(FORM = "HTS Form") %>%
   select(
      REC_ID,
      FORM,
      PATIENT_ID,
      BIRTHDATE,
      UIC,
      SEX,
      FACI_ID,
      SELF_IDENT,
      SELF_IDENT_OTHER,
      TEST_FACI     = SERVICE_FACI,
      TEST_SUB_FACI = SERVICE_SUB_FACI,
      RECORD_DATE,
      HIV_SERVICE_PSGC_REG,
      HIV_SERVICE_PSGC_PROV,
      HIV_SERVICE_PSGC_MUNC,
      TEST_AGREED   = SCREEN_AGREED,
      MODALITY,
      TEST_DATE     = T0_DATE,
      TEST_RESULT   = T0_RESULT,
      T1_DATE,
      T1_RESULT,
      T2_DATE,
      T2_RESULT,
      T3_DATE,
      T3_RESULT,
      DATE_CONFIRM,
      CONFIRM_RESULT,
      REACH_CLINICAL,
      REACH_ONLINE,
      REACH_INDEX_TESTING,
      REACH_SSNT,
      REACH_VENUE,
      AGE
   )

.log_info("Getting CFBS Form data.")
form_cfbs <- epictr$forms$form_cfbs %>%
   mutate(
      FORM = "CFBS Form"
   ) %>%
   select(
      REC_ID,
      FORM,
      PATIENT_ID,
      UIC,
      BIRTHDATE,
      SEX,
      FACI_ID,
      SELF_IDENT,
      SELF_IDENT_OTHER,
      TEST_FACI     = SERVICE_FACI,
      TEST_SUB_FACI = SERVICE_SUB_FACI,
      RECORD_DATE,
      HIV_SERVICE_PSGC_REG,
      HIV_SERVICE_PSGC_PROV,
      HIV_SERVICE_PSGC_MUNC,
      TEST_AGREED   = SCREEN_AGREED,
      MODALITY,
      TEST_DATE,
      TEST_RESULT,
      AGE
   )

##  Combine testing data  ------------------------------------------------------

.log_info("Creating unified reach dataset.")
tests <- form_a %>%
   bind_rows(form_hts) %>%
   bind_rows(form_cfbs) %>%
   # get cid for merging
   left_join(
      y  = epictr$forms$id_registry,
      by = "PATIENT_ID"
   ) %>%
   mutate(
      CENTRAL_ID      = if_else(
         condition = is.na(CENTRAL_ID),
         true      = PATIENT_ID,
         false     = CENTRAL_ID
      ),

      # tag those without form faci
      use_record_faci = if_else(
         condition = is.na(TEST_FACI) | TEST_FACI == "",
         true      = 1,
         false     = 0
      ),
      TEST_FACI       = if_else(
         condition = use_record_faci == 1,
         true      = FACI_ID,
         false     = TEST_FACI
      ),
      .before         = 1
   ) %>%
   rename(
      FINAL_FACI     = TEST_FACI,
      FINAL_SUB_FACI = TEST_SUB_FACI
   ) %>%
   mutate_at(
      .var = vars(
         names(select(
            .,
            -REC_ID,
            -TEST_AGREED,
            -MODALITY,
            -starts_with("EXPOSE"),
            -starts_with("RISK"),
            -contains("RESULT")
         ) %>% select_if(is.character))
      ),
      ~remove_code(.)
   ) %>%
   mutate(
      use_test = case_when(
         !is.na(TEST_RESULT) ~ "t0",
         is.na(TEST_RESULT) & !is.na(T1_RESULT) ~ "t1",
         is.na(TEST_RESULT) &
            is.na(T1_RESULT) &
            !is.na(CONFIRM_RESULT) ~ "confirm",
      ),
      drop     = case_when(
         StrLeft(MODALITY, 6) == "101304" ~ 1,
         is.na(use_test) & is.na(MODALITY) ~ 1,
         TRUE ~ 0
      )
   ) %>%
   filter(drop == 0) %>%
   mutate(
      modality    = case_when(
         FINAL_FACI == "130605" & StrLeft(MODALITY, 6) %in% c("101101", 101104) ~ "CBS",
         StrLeft(MODALITY, 6) == "101101" ~ "FBT",
         StrLeft(MODALITY, 6) == "101103" ~ "CBS",
         StrLeft(MODALITY, 6) == "101104" ~ "FBS",
         StrLeft(MODALITY, 6) == "101105" ~ "ST",
         TRUE ~ "FBT"
      ),
      test_date   = case_when(
         use_test == "t0" & TEST_DATE >= -25567 ~ as.Date(TEST_DATE),
         use_test == "t0" & TEST_DATE < -25567 ~ as.Date(RECORD_DATE),
         use_test == "t1" & T1_DATE >= -25567 ~ as.Date(T1_DATE),
         use_test == "t1" & T1_DATE < -25567 ~ as.Date(T1_DATE),
         use_test == "confirm" ~ as.Date(RECORD_DATE),
      ),
      test_result = case_when(
         use_test == "t0" ~ TEST_RESULT,
         use_test == "t1" ~ T1_RESULT,
         use_test == "confirm" ~ CONFIRM_RESULT,
      ),
      test_result = case_when(
         StrLeft(test_result, 1) == "1" ~ "Reactive",
         StrLeft(test_result, 1) == "2" ~ "Non-reactive",
         test_result == "Duplicate" ~ "Reactive"
      )
   ) %>%
   filter(
      test_date <= "2022-09-30",
      test_date >= "2020-01-01"
   ) %>%
   mutate(
      FINAL_SUB_FACI = case_when(
         FINAL_FACI == "130001" & StrLeft(FINAL_SUB_FACI, 6) == "130001" ~ FINAL_SUB_FACI,
         FINAL_FACI == "130605" & StrLeft(FINAL_SUB_FACI, 6) == "130605" ~ FINAL_SUB_FACI,
         TRUE ~ ""
      )
   ) %>%
   distinct(CENTRAL_ID, FINAL_FACI, FINAL_SUB_FACI, test_date, .keep_all = TRUE) %>%
   left_join(
      y = ohasis$ref_faci %>%
         select(
            FINAL_FACI_NAME = FACI_NAME,
            FINAL_FACI      = FACI_ID,
            FINAL_SUB_FACI  = SUB_FACI_ID,
            TEST_PSGC_REG   = FACI_PSGC_REG,
            TEST_PSGC_PROV  = FACI_PSGC_PROV,
            TEST_PSGC_MUNC  = FACI_PSGC_MUNC,
         )
   ) %>%
   mutate(
      TEST_PSGC_REG  = case_when(
         modality %in% c("CBS", "ST") & !is.na(HIV_SERVICE_PSGC_REG) ~ HIV_SERVICE_PSGC_REG,
         TRUE ~ TEST_PSGC_REG
      ),
      TEST_PSGC_PROV = case_when(
         modality %in% c("CBS", "ST") & !is.na(HIV_SERVICE_PSGC_PROV) ~ HIV_SERVICE_PSGC_PROV,
         TRUE ~ TEST_PSGC_PROV
      ),
      TEST_PSGC_MUNC = case_when(
         modality %in% c("CBS", "ST") & !is.na(HIV_SERVICE_PSGC_MUNC) ~ HIV_SERVICE_PSGC_MUNC,
         TRUE ~ TEST_PSGC_MUNC
      ),
      TEST_PSGC_MUNC = case_when(
         TEST_PSGC_PROV == "133900000" ~ "133900000",
         TRUE ~ TEST_PSGC_MUNC
      ),
      TEST_PSGC_PROV = stri_pad_right(StrLeft(TEST_PSGC_MUNC, 4), 9, "0"),
      TEST_PSGC_REG  = stri_pad_right(StrLeft(TEST_PSGC_MUNC, 2), 9, "0"),
   ) %>%
   distinct_all() %>%
   left_join(
      y          = epictr$estimates$class %>%
         select(
            aem_class_test = aem_class,
            TEST_PSGC_REG  = PSGC_REG,
            TEST_PSGC_PROV = PSGC_PROV,
            TEST_PSGC_MUNC = PSGC_MUNC
         ),
      na_matches = "never"
   ) %>%
   mutate(
      TEST_PSGC_AEM = if_else(aem_class_test %in% c("a", "ncr", "cebu city", "cebu province"), TEST_PSGC_MUNC, TEST_PSGC_PROV, TEST_PSGC_PROV),
      # TEST_PSGC_PROV = case_when(
      #    TEST_PSGC_PROV == TEST_PSGC_MUNC &StrLeft(TEST_PSGC_PROV, 2) != "13" ~ "",
      # ),
   ) %>%
   left_join(
      y          = epictr$ref_addr %>%
         select(
            TEST_PSGC_REG  = PSGC_REG,
            TEST_PSGC_PROV = PSGC_PROV,
            TEST_PSGC_MUNC = PSGC_MUNC,
            TEST_NAME_REG  = NAME_REG,
            TEST_NAME_PROV = NAME_PROV,
            TEST_NAME_MUNC = NAME_MUNC
         ) %>%
         mutate_at(
            .vars = vars(contains("PSGC")),
            ~if_else(. != "" & nchar(.) == 9, paste0("PH", .), ., "")
         ),
      na_matches = "never"
   ) %>%
   distinct_all() %>%
   mutate(
      TEST_NAME_AEM = if_else(aem_class_test %in% c("a", "ncr", "cebu city", "cebu province"), TEST_NAME_MUNC, TEST_NAME_PROV, TEST_NAME_PROV),
   ) %>%
   filter(!is.na(TEST_PSGC_REG)) %>%
   select(-TEST_DATE, -TEST_RESULT, -MODALITY)

lw_conn     <- ohasis$conn("lw")
table_space <- Id(schema = "harp", table = "epictr_testlist_hiv")
if (dbExistsTable(lw_conn, table_space))
   dbRemoveTable(lw_conn, table_space)

dbCreateTable(lw_conn, table_space, tests)
# dbExecute(
#    lw_conn,
#    r"(
#    alter table harp.epictr_testlist_hiv modify row_id varchar(355) not null;
# )"
# )

ohasis$upsert(lw_conn, "harp", "epictr_testlist_hiv", tests, c("CENTRAL_ID", "REC_ID"))

dbDisconnect(lw_conn)



