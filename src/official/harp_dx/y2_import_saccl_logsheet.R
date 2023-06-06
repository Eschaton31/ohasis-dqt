##  Download encoding documentation --------------------------------------------

get_pdf_data <- function(file = NULL) {
   if (is.null(file))
      file <- input("Kindly provide the UNIX path to the SACCL PDF Logsheet.")

   corr_sheet       <- "1LLsUNwRfYycXaUWxQjD87YbniZszefy1My_t0_dAHEk"
   corr_names       <- sheet_names(corr_sheet)
   corr_data        <- lapply(corr_names, function(sheet) read_sheet(corr_sheet, sheet, col_types = "c"))
   names(corr_data) <- corr_names

   lst        <- tabulizer::extract_tables(file = file, method = "lattice")
   confirm_df <- lst %>%
      lapply(function(data) {
         data %<>%
            as_tibble() %>%
            slice(-1, -2) %>%
            select(
               DATE_RECEIVE      = V2,
               CONFIRMATORY_CODE = V3,
               FULLNAME          = V4,
               BIRTHDATE         = V5,
               AGE               = V6,
               SEX               = V7,
               SOURCE            = V8,
               RAPID             = V10,
               SYSMEX            = V14,
               VIDAS             = V17,
               GEENIUS           = V18,
               REMARKS           = V19,
               DATE_CONFIRM      = V20
            )

         return(data)
      }) %>%
      bind_rows() %>%
      mutate_if(
         .predicate = is.character,
         ~str_squish(.)
      ) %>%
      mutate_at(
         .vars = vars(
            CONFIRMATORY_CODE,
            FULLNAME,
            SOURCE,
            RAPID,
            SYSMEX,
            VIDAS,
            GEENIUS
         ),
         ~toupper(.)
      ) %>%
      fullname_to_components(FULLNAME) %>%
      rename(
         FIRST  = FirstName,
         MIDDLE = MiddleName,
         LAST   = LastName,
      ) %>%
      # standardize
      mutate(
         SEX = case_when(
            SEX == "M" ~ "1",
            SEX == "MALE" ~ "1",
            SEX == "F" ~ "2",
            SEX == "FEMALE" ~ "2",
            TRUE ~ SEX
         )
      ) %>%
      mutate(
         T1_KIT       = "SYSMEX HISCL HIV Ag + Ab Assay",
         T1_RESULT    = as.numeric(SYSMEX),
         T1_RESULT    = case_when(
            SYSMEX == ">100.000" ~ "10",
            T1_RESULT >= 1 ~ "10",
            T1_RESULT < 1 ~ "20",
            TRUE ~ "  "
         ),

         T2_KIT       = "VIDAS HIV DUO Ultra",
         T2_RESULT    = case_when(
            VIDAS == "REACTIVE" ~ "10",
            VIDAS == "NONREACTIVE" ~ "20",
            TRUE ~ "  "
         ),

         T3_KIT       = case_when(
            RAPID != "" ~ "HIV 1/2 STAT-PAK Assay",
            GEENIUS != "" ~ "Geenius HIV 1/2 Confirmatory Assay",
         ),
         T3_RESULT    = case_when(
            RAPID == "REACTIVE" ~ "10",
            RAPID == "NONREACTIVE" ~ "20",
            GEENIUS == "POSITIVE" ~ "10",
            GEENIUS == "NEGATIVE" ~ "20",
            GEENIUS == "INDETERMINATE" ~ "30",
            TRUE ~ "  "
         ),
         FINAL_RESULT = stri_c(T1_RESULT, T2_RESULT, T3_RESULT),
         FINAL_RESULT = case_when(
            FINAL_RESULT == "101010" ~ "Positive",
            FINAL_RESULT == "202020" ~ "Negative",
            FINAL_RESULT == "2020  " ~ "Negative",
            FINAL_RESULT == "20    " ~ "Negative",
            grepl("30", FINAL_RESULT) ~ "Indeterminate",
            grepl("20", FINAL_RESULT) ~ "Indeterminate",
            grepl("^SAME AS", REMARKS) ~ "Duplicate",
         ),

         DATE_RECEIVE = as.Date(DATE_RECEIVE, "%m/%d/%y"),
         DATE_CONFIRM = as.Date(DATE_CONFIRM, "%m/%d/%y"),
         BIRTHDATE    = as.Date(BIRTHDATE, "%m/%d/%Y"),
      ) %>%
      filter(SOURCE != "JAY DUMMY LAB") %>%
      left_join(corr_data$SOURCE %>% distinct(SOURCE, SOURCE_FACI, SOURCE_SUB_FACI))

   return(confirm_df)
}

match_ohasis <- function(pdf_data) {
   db_conn <- ohasis$conn("db")

   # get list of labcodes
   labcodes <- unique(pdf_data$CONFIRMATORY_CODE)
   query    <- r"(
SELECT px_info.*,
       1                                             AS EXIST_INFO,
       IF(px_confirm.CONFIRM_CODE IS NOT NULL, 1, 0) AS EXIST_CONFIRM
FROM ohasis_interim.px_info
         JOIN ohasis_interim.px_record ON px_info.REC_ID = px_record.REC_ID
         LEFT JOIN ohasis_interim.px_confirm ON px_info.REC_ID = px_confirm.REC_ID
WHERE px_record.MODULE = 2
  AND px_record.DELETED_AT IS NULL
  AND px_info.CONFIRMATORY_CODE IN (?)
      )"
   oh_data  <- dbxSelect(db_conn, query, params = list(labcodes))
   dbDisconnect(db_conn)

   # match with pdf
   data <- pdf_data %>%
      left_join(oh_data %>% select(-SEX, -BIRTHDATE), join_by(CONFIRMATORY_CODE)) %>%
      mutate_at(
         .vars = vars(EXIST_INFO, EXIST_CONFIRM),
         ~coalesce(., 0)
      )

   return(data)
}

prepare_import <- function(data, match) {
   TIMESTAMP <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

   import <- data %>%
      mutate_all(~str_squish(as.character(.))) %>%
      select(-FINAL_RESULT, -FINAL_RESULT) %>%
      inner_join(
         y          = match %>%
            filter(!is.na(REC_ID), is.na(EXIST_OH)),
         by         = "CONFIRMATORY_CODE",
         na_matches = "never"
      ) %>%
      distinct(CONFIRMATORY_CODE, .keep_all = TRUE) %>%
      filter(!is.na(REC_ID), nchar(REC_ID) == 25) %>%
      mutate(
         UPDATED_AT = TIMESTAMP,
         UPDATED_BY = Sys.getenv("OH_USER_ID"),
      )

   return(import)
}

generate_tables <- function(import) {
   tables            <- list()
   tables$px_confirm <- import %>%
      mutate(
         FACI_ID      = "130023",
         SUB_FACI_ID  = "130023_001",
         CONFIRM_TYPE = "1",
         CREATED_AT   = UPDATED_AT,
         CREATED_BY   = Sys.getenv("OH_USER_ID"),
         # REMARKS      = case_when(
         #    REMARKS == "Duplicate" ~ "Client already has a previous confirmatory record.",
         #    FINAL_RESULT == "Negative" ~ "Laboratory evidence suggests no presence of HIV Antibodies at the time of testing.",
         #    TRUE ~ REMARKS
         # ),
         DATE_CONFIRM = as.character(DATE_CONFIRM),
         DATE_CONFIRM = if_else(!is.na(DATE_CONFIRM), glue("{DATE_CONFIRM} 00:00:00"), NA_character_),
         DATE_RELEASE = as.character(DATE_CONFIRM),
      ) %>%
      select(
         REC_ID,
         FACI_ID,
         SUB_FACI_ID,
         CONFIRM_TYPE,
         CONFIRM_CODE = CONFIRMATORY_CODE,
         SOURCE       = SOURCE_FACI,
         SUB_SOURCE   = SOURCE_SUB_FACI,
         FINAL_RESULT,
         REMARKS,
         DATE_CONFIRM,
         DATE_RELEASE,
         CREATED_AT,
         CREATED_BY
      ) %>%
      distinct(REC_ID, .keep_all = TRUE)

   tables$px_test <- import %>%
      mutate(
         TEST_TYPE   = "31",
         TEST_NUM    = "1",
         FACI_ID     = "130023",
         SUB_FACI_ID = "130023_001",
         CREATED_AT  = UPDATED_AT,
         CREATED_BY  = Sys.getenv("OH_USER_ID"),
         RESULT      = substr(FINAL_RESULT_31, 1, 1)
      ) %>%
      select(
         REC_ID,
         FACI_ID,
         SUB_FACI_ID,
         TEST_TYPE,
         TEST_NUM,
         DATE_PERFORM = DATE_CONFIRM,
         RESULT,
         CREATED_AT,
         CREATED_BY
      ) %>%
      distinct(REC_ID, .keep_all = TRUE) %>%
      bind_rows(
         import %>%
            mutate(
               TEST_TYPE   = "32",
               TEST_NUM    = "1",
               FACI_ID     = "130023",
               SUB_FACI_ID = "130023_001",
               CREATED_AT  = UPDATED_AT,
               CREATED_BY  = Sys.getenv("OH_USER_ID"),
               RESULT      = substr(FINAL_RESULT_32, 1, 1)
            ) %>%
            select(
               REC_ID,
               FACI_ID,
               SUB_FACI_ID,
               TEST_TYPE,
               TEST_NUM,
               DATE_PERFORM = DATE_CONFIRM,
               RESULT,
               CREATED_AT,
               CREATED_BY
            ) %>%
            distinct(REC_ID, .keep_all = TRUE)
      ) %>%
      bind_rows(
         import %>%
            mutate(
               TEST_TYPE   = "33",
               TEST_NUM    = "1",
               FACI_ID     = "130023",
               SUB_FACI_ID = "130023_001",
               CREATED_AT  = UPDATED_AT,
               CREATED_BY  = Sys.getenv("OH_USER_ID"),
               RESULT      = substr(FINAL_RESULT_33, 1, 1)
            ) %>%
            select(
               REC_ID,
               FACI_ID,
               SUB_FACI_ID,
               TEST_TYPE,
               TEST_NUM,
               DATE_PERFORM = DATE_CONFIRM,
               RESULT,
               CREATED_AT,
               CREATED_BY
            ) %>%
            distinct(REC_ID, .keep_all = TRUE)
      )

   tables$px_test_hiv <- import %>%
      mutate(
         TEST_TYPE   = "31",
         TEST_NUM    = "1",
         FACI_ID     = "130023",
         SUB_FACI_ID = "130023_001",
         CREATED_AT  = UPDATED_AT,
         CREATED_BY  = Sys.getenv("OH_USER_ID"),
      ) %>%
      select(
         REC_ID,
         FACI_ID,
         SUB_FACI_ID,
         TEST_TYPE,
         TEST_NUM,
         DATE_RECEIVE = SPECIMEN_RECEIPT_DATE,
         KIT_NAME     = KIT_31,
         FINAL_RESULT = FINAL_RESULT_31,
         CREATED_AT,
         CREATED_BY
      ) %>%
      bind_rows(
         import %>%
            mutate(
               TEST_TYPE   = "32",
               TEST_NUM    = "1",
               FACI_ID     = "130023",
               SUB_FACI_ID = "130023_001",
               CREATED_AT  = UPDATED_AT,
               CREATED_BY  = Sys.getenv("OH_USER_ID"),
            ) %>%
            select(
               REC_ID,
               FACI_ID,
               SUB_FACI_ID,
               TEST_TYPE,
               TEST_NUM,
               KIT_NAME     = KIT_32,
               FINAL_RESULT = FINAL_RESULT_32,
               CREATED_AT,
               CREATED_BY
            )
      ) %>%
      bind_rows(
         import %>%
            mutate(
               TEST_TYPE   = "33",
               TEST_NUM    = "1",
               FACI_ID     = "130023",
               SUB_FACI_ID = "130023_001",
               CREATED_AT  = UPDATED_AT,
               CREATED_BY  = Sys.getenv("OH_USER_ID"),
            ) %>%
            select(
               REC_ID,
               FACI_ID,
               SUB_FACI_ID,
               TEST_TYPE,
               TEST_NUM,
               KIT_NAME     = KIT_33,
               FINAL_RESULT = FINAL_RESULT_33,
               CREATED_AT,
               CREATED_BY
            )
      )

   return(tables)
}

import_data <- function(tables) {

   db_conn     <- ohasis$conn("db")
   table_space <- Id(schema = "ohasis_interim", table = "px_confirm")
   dbxUpsert(
      db_conn,
      table_space,
      tables[["px_confirm"]],
      "REC_ID"
   )
   table_space <- Id(schema = "ohasis_interim", table = "px_test_hiv")
   dbxUpsert(
      db_conn,
      table_space,
      tables[["px_test_hiv"]],
      c("REC_ID", "TEST_TYPE", "TEST_NUM")
   )
   table_space <- Id(schema = "ohasis_interim", table = "px_test")
   dbxUpsert(
      db_conn,
      table_space,
      tables[["px_test"]],
      c("REC_ID", "TEST_TYPE", "TEST_NUM")
   )
   dbDisconnect(db_conn)

   update_credentials(tables$px_confirm$REC_ID)
}

.init <- function() {
   p <- parent.env(environment())
   local(envir = p, {
      results <- get_pdf_data() %>%
         match_ohasis()

      for_import <- prepare_import(.GlobalEnv$nhsss$harp_dx$pdf_saccl$data, match)
      tables     <- generate_tables(for_import)
   })
}

