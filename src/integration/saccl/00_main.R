##  inputs ---------------------------------------------------------------------

file <- "R:/File requests/SACCL Submissions/logsheet/logsheet_hiv/CENSUS 2022 Jay Chinjen.xls"
file <- "R:/File requests/SACCL Submissions/logsheet/logsheet_hiv/2020 Jay Chinjen/NOV TO DEC 2020.xls"
file <- "R:/File requests/SACCL Submissions/logsheet/logsheet_hiv/saccl logsheet 2024.01.01 to 2024.01.31 SACCLHIV.xlsx"

##  processing -----------------------------------------------------------------

parse_saccl_date <- function(data, column) {
   data %<>%
      mutate(
         .after = {{column}},
         DATE_1 = case_when(
            str_detect({{column}}, "^0222-") ~ str_replace({{column}}, "0222-", "2022-"),
            str_detect({{column}}, "^0227-") ~ str_replace({{column}}, "0227-", "2022-"),
            str_detect({{column}}, "^0200-") ~ str_replace({{column}}, "0200-", "2022-"),
            str_detect({{column}}, "^0228-") ~ str_replace({{column}}, "0228-", "2022-"),
            str_detect({{column}}, "^0215-") ~ str_replace({{column}}, "0215-", "2021-"),
            str_detect({{column}}, "^0201-") ~ str_replace({{column}}, "0201-", "2021-"),
            str_detect({{column}}, "^0202-") ~ str_replace({{column}}, "0202-", "2021-"),
            str_detect({{column}}, "^0221-") ~ str_replace({{column}}, "0221-", "2021-"),
         ),
         DATE_1 = as.Date(parse_date_time(DATE_1, c("YmdHMS", "dmY", "Ymd"))),
         DATE_2 = if_else(is.na(DATE_1) & !is.na({{column}}), {{column}}, NA_character_),
         DATE_2 = na_if(DATE_2, "11/30/-1")
      )

   review <- data %>% filter(!is.na(DATE_2), !StrIsNumeric(DATE_2))
   if (nrow(review) > 0)
      review %>% tab(DATE_2) %>% print()

   data %<>%
      mutate(
         .after = {{column}},
         DATE_2 = excel_numeric_to_date(as.numeric(DATE_2)),

         DATE_1 = na_if(DATE_1, as.Date("1970-01-01")),
         DATE_2 = na_if(DATE_2, as.Date("1970-01-01")),

         DATE   = coalesce(DATE_1, DATE_2),
      ) %>%
      select(-{{column}}, -DATE_1, -DATE_2) %>%
      rename(
         {{column}} := DATE
      )

   return(data)
}

saccl       <- list()
saccl$facis <- read_sheet("1LLsUNwRfYycXaUWxQjD87YbniZszefy1My_t0_dAHEk", "SOURCE", col_types = "c", range = "A:C") %>%
   filter(!is.na(SOURCE_FACI)) %>%
   distinct(SOURCE, .keep_all = TRUE) %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(.)
   )

saccl$raw <- read_excel(file, 1, col_types = "text", .name_repair = "unique_quiet") %>%
   slice(-1) %>%
   select(
      DATE_COLLECT   = 1,
      DATE_RECEIVE   = 2,
      CONFIRM_CODE   = 3,
      FIRST          = 4,
      MIDDLE         = 5,
      LAST           = 6,
      # PATIENT_CODE   = 7,
      BIRTHDATE      = 8,
      AGE            = 9,
      SEX            = 10,
      SOURCE         = 11,
      CONFIRM_RESULT = 12,
      REMARKS        = 13,
      DATE_CONFIRM   = 14,
      T0_COV_1       = 15,
      T0_ABS_1       = 16,
      T0_RESULT_1    = 17,
      T0_DATE_1      = 18,
      T0_COV_2       = 19,
      T0_ABS_2       = 20,
      T0_RESULT_2    = 21,
      T0_DATE_2      = 22,
   ) %>%
   parse_saccl_date(DATE_COLLECT) %>%
   parse_saccl_date(DATE_RECEIVE) %>%
   parse_saccl_date(DATE_CONFIRM) %>%
   parse_saccl_date(T0_DATE_1) %>%
   parse_saccl_date(T0_DATE_2) %>%
   parse_saccl_date(BIRTHDATE) %>%
   mutate(
      T0_DATE   = case_when(
         T0_DATE_1 == T0_DATE_2 ~ T0_DATE_1,
         T0_DATE_1 < T0_DATE_2 ~ T0_DATE_1,
         T0_DATE_1 > T0_DATE_2 ~ T0_DATE_2,
         TRUE ~ coalesce(T0_DATE_1, T0_DATE_2)
      ),
      T0_RESULT = coalesce(T0_RESULT_1, T0_RESULT_2),
      T0_RESULT = if_else(is.na(T0_RESULT) & !is.na(T0_DATE), "REACTIVE", T0_RESULT, T0_RESULT)
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~str_squish(.)
   ) %>%
   mutate_at(
      .vars = vars(
         CONFIRM_CODE,
         FIRST,
         MIDDLE,
         LAST,
         CONFIRM_RESULT,
         SOURCE,
         T0_RESULT
      ),
      ~toupper(.)
   ) %>%
   # standardize
   mutate(
      SEX            = case_when(
         SEX == "M" ~ "1",
         SEX == "MALE" ~ "1",
         SEX == "F" ~ "2",
         SEX == "FEMALE" ~ "2",
         TRUE ~ SEX
      ),
      SEX            = as.integer(SEX),

      CONFIRM_RESULT = case_when(
         CONFIRM_CODE == "D22-05-05552" ~ "Positive",
         is.na(CONFIRM_RESULT) & str_detect(REMARKS, "^SAME AS") ~ "DUPLICATE",
         is.na(CONFIRM_RESULT) & str_detect(REMARKS, "^SAMESAME AS") ~ "DUPLICATE",
         is.na(CONFIRM_RESULT) & str_detect(REMARKS, "^Submit plasma") ~ "INDETERMINATE",
         is.na(CONFIRM_RESULT) & str_detect(REMARKS, "Patient is advised for a repeat") ~ "INDETERMINATE",
         is.na(CONFIRM_RESULT) & str_detect(REMARKS, "RECOMMEND REPEAT EXTRACTION") ~ "INDETERMINATE",
         is.na(CONFIRM_RESULT) & str_detect(REMARKS, "Client is advised to proceed to the nearest") ~ "POSITIVE FOR HIV ANTIBODIES",
         is.na(CONFIRM_RESULT) & str_detect(REMARKS, "Fill out HIV care report") ~ "POSITIVE FOR HIV ANTIBODIES",
         is.na(CONFIRM_RESULT) & is.na(REMARKS) ~ "NEGATIVE",
         TRUE ~ CONFIRM_RESULT
      )
   ) %>%
   mutate(
      T1_KIT         = "",
      T2_KIT         = "",
      T3_KIT         = "",
      CONFIRM_RESULT = case_when(
         str_detect(CONFIRM_RESULT, "POSITIVE") ~ "Positive",
         str_detect(CONFIRM_RESULT, "NEGATIVE") ~ "Negative",
         str_detect(CONFIRM_RESULT, "INDETERMINATE") ~ "Indeterminate",
         str_detect(CONFIRM_RESULT, "DUPLICATE") ~ "Duplicate",
         TRUE ~ CONFIRM_RESULT
      ),
   ) %>%
   filter(SOURCE != "JAY DUMMY LAB") %>%
   left_join(saccl$facis, join_by(SOURCE))

#  unconverted data ------------------------------------------------------------

issues <- list(
   source = saccl$raw %>%
      filter(is.na(SOURCE_FACI)) %>%
      distinct(SOURCE),
   result = saccl$raw %>%
      filter(is.na(CONFIRM_RESULT))
)

#  uploaded --------------------------------------------------------------------

conn              <- ohasis$conn("lw")
saccl$prev_upload <- QB$new(conn)$
   from("ohasis_lake.px_hiv_testing AS test")$
   leftJoin("ohasis_lake.px_pii AS pii", "test.REC_ID", "=", "pii.REC_ID")$
   whereIn("CONFIRM_CODE", saccl$raw$CONFIRM_CODE)$
   select("pii.PATIENT_ID", "pii.CREATED_BY", "pii.UPDATED_BY", "pii.DELETED_BY", "test.*")$
   get()

saccl$prev_upload %<>%
   mutate(CONFIRM_RESULT = remove_code(CONFIRM_RESULT)) %>%
   mutate_if(
      .predicate = is.POSIXct,
      ~as.Date(.)
   )
dbDisconnect(conn)

#  match ids -------------------------------------------------------------------

TIMESTAMP     <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
saccl$records <- saccl$raw %>%
   # get records id if existing
   left_join(
      y  = saccl$prev_upload %>%
         select(
            REC_ID,
            PATIENT_ID,
            CREATED_BY,
            CREATED_AT,
            CONFIRM_CODE,
            OH_CONFIRM_DATE   = DATE_CONFIRM,
            OH_CONFIRM_RESULT = CONFIRM_RESULT,
            OH_SOURCE         = SPECIMEN_SOURCE,
            OH_SUB_SOURCE     = SPECIMEN_SUB_SOURCE
         ),
      by = join_by(CONFIRM_CODE)
   ) %>%
   mutate(
      old_rec          = if_else(!is.na(REC_ID), 1, 0, 0),
      CONFIRM_FACI     = "130023",
      CONFIRM_SUB_FACI = "130023_001"
   ) %>%
   mutate(
      CREATED_BY = coalesce(CREATED_BY, "1300000048"),
      CREATED_AT = coalesce(as.character(CREATED_AT), TIMESTAMP),
      UPDATED_BY = "1300000048",
      UPDATED_AT = TIMESTAMP
   ) %>%
   mutate(
      update_source       = if_else(coalesce(SOURCE_FACI, "") != coalesce(OH_SOURCE, ""), 1, 0, 0),
      update_sub_source   = if_else(coalesce(SOURCE_FACI, "") != coalesce(OH_SOURCE, ""), 1, 0, 0),
      update_result       = if_else(coalesce(CONFIRM_RESULT, "") != coalesce(OH_CONFIRM_RESULT, ""), 1, 0, 0),
      update_confirm_date = if_else(coalesce(DATE_CONFIRM, as.Date("1970-01-01")) != coalesce(OH_CONFIRM_DATE, as.Date("1970-01-01")), 1, 0, 0),
   ) %>%
   filter()

# patient ids
saccl$records %<>%
   filter(!is.na(PATIENT_ID)) %>%
   bind_rows(
      batch_px_ids(saccl$records %>% filter(is.na(PATIENT_ID)), PATIENT_ID, CONFIRM_FACI, "CONFIRM_CODE")
   )

# rec ids
saccl$records %<>%
   filter(!is.na(REC_ID)) %>%
   bind_rows(
      batch_rec_ids(saccl$records %>% filter(is.na(REC_ID)), REC_ID, CREATED_BY, "CONFIRM_CODE")
   )


##  new data -------------------------------------------------------------------

saccl$import_confirm <- saccl$records %>%
   filter(if_any(c(update_source, update_sub_source, update_confirm_date, update_result), ~. == 1))

saccl$import_test <- saccl$records %>%
   anti_join(
      y  = saccl$prev_upload %>%
         select(
            CONFIRM_CODE,
            DATE_COLLECT,
            DATE_RECEIVE,
            T0_DATE,
            T0_RESULT,
         ),
      by = join_by(CONFIRM_CODE, DATE_COLLECT, DATE_RECEIVE, T0_DATE, T0_RESULT)
   )

##  table formats
tables           <- list()
tables$px_record <- list(
   name = "px_record",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = saccl$import_confirm %>%
      select(REC_ID, PATIENT_ID, RECORD_DATE = DATE_RECEIVE, CREATED_BY, CREATED_AT, UPDATED_BY, UPDATED_AT) %>%
      bind_rows(
         saccl$import_test %>%
            select(REC_ID, PATIENT_ID, RECORD_DATE = DATE_RECEIVE, CREATED_BY, CREATED_AT, UPDATED_BY, UPDATED_AT)
      ) %>%
      distinct_all() %>%
      mutate(
         FACI_ID     = "130000",
         SUB_FACI_ID = NA_character_,
         DISEASE     = "101000",
         MODULE      = 2
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
   data = saccl$records %>%
      filter(old_rec == 0) %>%
      select(
         REC_ID,
         PATIENT_ID,
         CONFIRMATORY_CODE = CONFIRM_CODE,
         SEX,
         BIRTHDATE,
         CREATED_BY,
         CREATED_AT,
         UPDATED_BY,
         UPDATED_AT,
      )
)

tables$px_name <- list(
   name = "px_name",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = saccl$records %>%
      filter(old_rec == 0) %>%
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

tables$px_confirm <- list(
   name = "px_confirm",
   pk   = "REC_ID",
   data = saccl$import_confirm %>%
      mutate(
         CLIENT_TYPE  = 4,
         CONFIRM_TYPE = 1,
         DATE_RELEASE = DATE_CONFIRM
      ) %>%
      select(
         REC_ID,
         FACI_ID      = CONFIRM_FACI,
         SUB_FACI_ID  = CONFIRM_SUB_FACI,
         CONFIRM_TYPE,
         CONFIRM_CODE,
         CLIENT_TYPE,
         SOURCE       = SOURCE_FACI,
         SUB_SOURCE   = SOURCE_SUB_FACI,
         FINAL_RESULT = CONFIRM_RESULT,
         REMARKS,
         DATE_CONFIRM,
         DATE_RELEASE,
         CREATED_AT,
         CREATED_BY
      )
)


tables$px_test <- list(
   name = "px_test",
   pk   = c("REC_ID", "TEST_TYPE", "TEST_NUM"),
   data = saccl$import_test %>%
      select(-ends_with("_1"), ends_with("_2")) %>%
      mutate(
         DATE_PERFORM = coalesce(T0_DATE, DATE_CONFIRM),
         T0_FACI      = SOURCE_FACI,
         T0_SUBFACI   = SOURCE_SUB_FACI,
         T0_DATE      = as.character(T0_DATE)
      ) %>%
      select(
         REC_ID,
         FACI_ID      = CONFIRM_FACI,
         SUB_FACI_ID  = CONFIRM_SUB_FACI,
         DATE_PERFORM = DATE_CONFIRM,
         CREATED_BY,
         CREATED_AT,
         T0_DATE,
         T0_RESULT,
         T0_FACI,
         T0_SUBFACI,
         starts_with("T1"),
         starts_with("T2"),
         starts_with("T3"),
      ) %>%
      pivot_longer(
         cols = c(starts_with("T0"), starts_with("T1"), starts_with("T2"), , starts_with("T3"))
      ) %>%
      separate_wider_delim(name, "_", names = c("TEST_TYPE", "VAR")) %>%
      filter(TEST_TYPE != "CONFIRM") %>%
      mutate(
         TEST_TYPE = case_when(
            TEST_TYPE == "T0" ~ "10",
            TEST_TYPE == "T1" ~ "31",
            TEST_TYPE == "T2" ~ "32",
            TEST_TYPE == "T3" ~ "33",
         ),
         value     = case_when(
            value == "REACTIVE" ~ "10",
            value == "NONREACTIVE" ~ "20",
            TRUE ~ value
         ),
         TEST_NUM  = 1
      ) %>%
      distinct(
         REC_ID,
         FACI_ID,
         SUB_FACI_ID,
         CREATED_BY,
         CREATED_AT,
         TEST_TYPE,
         TEST_NUM,
         VAR,
         .keep_all = TRUE
      ) %>%
      pivot_wider(
         id_cols     = c(
            REC_ID,
            FACI_ID,
            SUB_FACI_ID,
            CREATED_BY,
            CREATED_AT,
            TEST_TYPE,
            TEST_NUM,
            DATE_PERFORM
         ),
         names_from  = VAR,
         values_from = value
      ) %>%
      select(
         REC_ID,
         FACI_ID     = FACI,
         SUB_FACI_ID = SUB_FACI_ID,
         TEST_TYPE,
         TEST_NUM,
         DATE_PERFORM,
         RESULT,
         CREATED_AT,
         CREATED_BY
      ) %>%
      filter(!is.na(RESULT))
)

tables$px_test_hiv <- list(
   name = "px_test_hiv",
   pk   = c("REC_ID", "TEST_TYPE", "TEST_NUM"),
   data = saccl$import_test %>%
      select(
         REC_ID,
         FACI_ID     = CONFIRM_FACI,
         SUB_FACI_ID = CONFIRM_SUB_FACI,
         DATE_RECEIVE,
         DATE_COLLECT,
         CREATED_BY,
         CREATED_AT,
         ends_with("KIT"),
         ends_with("RESULT"),
      ) %>%
      pivot_longer(
         cols = starts_with("T1")
      ) %>%
      separate_wider_delim(name, "_", names = c("TEST_TYPE", "VAR")) %>%
      filter(TEST_TYPE != "FINAL") %>%
      mutate(
         TEST_TYPE = case_when(
            TEST_TYPE == "T0" ~ "10",
            TEST_TYPE == "T1" ~ "31",
            TEST_TYPE == "T2" ~ "32",
            TEST_TYPE == "T3" ~ "33",
         ),
         value     = case_when(
            value == "REACTIVE" ~ "10",
            value == "NONREACTIVE" ~ "20",
            TRUE ~ value
         ),
         TEST_NUM  = 1
      ) %>%
      distinct(
         REC_ID,
         FACI_ID,
         SUB_FACI_ID,
         CREATED_BY,
         CREATED_AT,
         TEST_TYPE,
         TEST_NUM,
         DATE_RECEIVE,
         DATE_COLLECT,
         .keep_all = TRUE
      ) %>%
      pivot_wider(
         id_cols     = c(
            REC_ID,
            FACI_ID,
            SUB_FACI_ID,
            CREATED_BY,
            CREATED_AT,
            TEST_TYPE,
            TEST_NUM,
            DATE_RECEIVE,
            DATE_COLLECT,
         ),
         names_from  = VAR,
         values_from = value
      ) %>%
      select(
         REC_ID,
         FACI_ID,
         SUB_FACI_ID,
         TEST_TYPE,
         TEST_NUM,
         DATE_RECEIVE,
         DATE_COLLECT,
         CREATED_AT,
         CREATED_BY
      )
)


db_conn <- ohasis$conn("db")
lapply(tables, function(ref, db_conn) {
   log_info("Uploading {green(ref$name)}.")
   table_space <- Id(schema = "ohasis_interim", table = ref$name)
   dbxUpsert(db_conn, table_space, ref$data, ref$pk)
}, db_conn)
dbDisconnect(db_conn)
