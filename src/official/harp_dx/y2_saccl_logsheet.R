##  Download encoding documentation --------------------------------------------

get_pdf_data <- function(file = NULL, format = "old") {
   local_drive_quiet()
   local_gs4_quiet()

   if (is.null(file))
      file <- input("Kindly provide the UNIX path to the SACCL PDF Logsheet.")

   log_info("Getting corrections.")
   corr_sheet       <- "1LLsUNwRfYycXaUWxQjD87YbniZszefy1My_t0_dAHEk"
   corr_names       <- sheet_names(corr_sheet)
   corr_data        <- lapply(corr_names, function(sheet) read_sheet(corr_sheet, sheet, col_types = "c"))
   names(corr_data) <- corr_names

   if (format == "old") {
      if (tools::file_ext(file) == "pdf") {
         log_info("Extractinng tables from PDF.")
         lst        <- tabulizer::extract_tables(file = file, method = "lattice")
         confirm_df <- lst %>%
            # lapply(function(data) {
            #    data %<>%
            #       as_tibble() %>%
            #       slice(-1, -2) %>%
            #       select(
            #          DATE_RECEIVE      = V2,
            #          CONFIRMATORY_CODE = V3,
            #          FULLNAME          = V4,
            #          BIRTHDATE         = V5,
            #          AGE               = V6,
            #          SEX               = V7,
            #          SOURCE            = V8,
            #          RAPID             = V10,
            #          SYSMEX            = V14,
            #          VIDAS             = V17,
            #          GEENIUS           = V18,
            #          REMARKS           = V19,
            #          DATE_CONFIRM      = V20
            #       )
            #
            #    return(data)
            # }) %>%
            lapply(function(data) {
               data %<>%
                  as_tibble() %>%
                  slice(-1, -2) %>%
                  select(
                     DATE_RECEIVE      = V3,
                     CONFIRMATORY_CODE = V4,
                     FULLNAME          = V5,
                     BIRTHDATE         = V6,
                     AGE               = V7,
                     SEX               = V8,
                     SOURCE            = V9,
                     RAPID             = V10,
                     SYSMEX            = V11,
                     VIDAS             = V14,
                     GEENIUS           = V15,
                     REMARKS           = V16,
                     DATE_CONFIRM      = V17
                  ) %>%
                  mutate(
                     DATE_RECEIVE = as.Date(DATE_RECEIVE, "%m/%d/%y"),
                     DATE_CONFIRM = as.Date(DATE_CONFIRM, "%m/%d/%y"),
                     BIRTHDATE    = as.Date(BIRTHDATE, "%m/%d/%Y"),
                  )

               return(data)
            }) %>%
            bind_rows()
      } else if (tools::file_ext(file) == "xlsx") {
         log_info("Extractinng tables from XLSX.")
         # read workbook using password
         wb         <- XLConnect::loadWorkbook(file)
         confirm_df <- XLConnect::readWorksheet(wb, 1, colTypes = XLC$DATA_TYPE.STRING, header = FALSE) %>%
            as_tibble()

         if (ncol(confirm_df) > 17) {
            confirm_df %<>%
               select(
                  DATE_RECEIVE      = 2,
                  CONFIRMATORY_CODE = 3,
                  FULLNAME          = 4,
                  BIRTHDATE         = 5,
                  AGE               = 6,
                  SEX               = 7,
                  SOURCE            = 8,
                  RAPID             = 13,
                  SYSMEX            = 14,
                  VIDAS             = 17,
                  GEENIUS           = 18,
                  REMARKS           = 19,
                  DATE_CONFIRM      = 20
               )
         } else {
            confirm_df %<>%
               select(
                  DATE_RECEIVE      = 3,
                  CONFIRMATORY_CODE = 4,
                  FULLNAME          = 5,
                  BIRTHDATE         = 6,
                  AGE               = 7,
                  SEX               = 8,
                  SOURCE            = 9,
                  RAPID             = 10,
                  SYSMEX            = 11,
                  VIDAS             = 14,
                  GEENIUS           = 15,
                  REMARKS           = 16,
                  DATE_CONFIRM      = 17
               ) %>%
               filter(!is.na(CONFIRMATORY_CODE))
         }

         if (confirm_df[1,]$DATE_RECEIVE == "DATE RECEIVED")
            confirm_df %<>%
               slice(-1)

         confirm_df %<>%
            mutate_at(
               .vars = vars(contains("DATE")),
               ~as.Date(.)
            )

         rm(wb)
         XLConnect::xlcFreeMemory()
      }

      confirm_df %<>%
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
            PATIENT_CODE = str_extract(FULLNAME, "[^\\(]*(?=\\))"),
            SEX          = case_when(
               SEX == "M" ~ "1",
               SEX == "MALE" ~ "1",
               SEX == "F" ~ "2",
               SEX == "FEMALE" ~ "2",
               TRUE ~ SEX
            ),
            SEX          = as.integer(SEX)
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
         ) %>%
         filter(SOURCE != "JAY DUMMY LAB") %>%
         left_join(corr_data$SOURCE %>% distinct(SOURCE, SOURCE_FACI, SOURCE_SUB_FACI))
   } else {
      confirm_df <- read_excel(file, 1, col_types = "text", .name_repair = "unique_quiet") %>%
         slice(-1) %>%
         rename(
            DATE_COLLECT      = 1,
            DATE_RECEIVE      = 2,
            CONFIRMATORY_CODE = 3,
            FIRST             = 4,
            MIDDLE            = 5,
            LAST              = 6,
            PATIENT_CODE      = 7,
            BIRTHDATE         = 8,
            AGE               = 9,
            SEX               = 10,
            SOURCE            = 11,
            FINAL_RESULT      = 12,
            REMARKS           = 13,
            DATE_CONFIRM      = 14,
            T0_COV_1          = 15,
            T0_ABS_1          = 16,
            T0_RESULT_1       = 17,
            T0_DATE_1         = 18,
            T0_COV_2          = 19,
            T0_ABS_2          = 20,
            T0_RESULT_2       = 21,
            T0_DATE_2         = 22,
            ) %>%
         mutate_at(
            .vars = vars(contains("DATE")),
            ~excel_numeric_to_date(as.numeric(.))
         ) %>%
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
               CONFIRMATORY_CODE,
               FIRST,
               MIDDLE,
               LAST,
               FINAL_RESULT,
               SOURCE,
               T0_RESULT
            ),
            ~toupper(.)
         ) %>%
         # standardize
         mutate(
            SEX          = case_when(
               SEX == "M" ~ "1",
               SEX == "MALE" ~ "1",
               SEX == "F" ~ "2",
               SEX == "FEMALE" ~ "2",
               TRUE ~ SEX
            ),
            SEX          = as.integer(SEX),

            FINAL_RESULT = case_when(
               is.na(FINAL_RESULT) & str_detect(REMARKS, "^SAME AS") ~ "DUPLICATE",
               is.na(FINAL_RESULT) & str_detect(REMARKS, "^Submit plasma") ~ "INDETERMINATE",
               is.na(FINAL_RESULT) & str_detect(REMARKS, "Client is advised to proceed to the nearest") ~ "POSITIVE FOR HIV ANTIBODIES",
               is.na(FINAL_RESULT) & str_detect(REMARKS, "Fill out HIV care report") ~ "POSITIVE FOR HIV ANTIBODIES",
               is.na(FINAL_RESULT) & is.na(REMARKS) ~ "NEGATIVE",
               TRUE ~ FINAL_RESULT
            )
         ) %>%
         mutate(
            T1_KIT       = "",

            T2_KIT       = "",

            T3_KIT       = "",
            FINAL_RESULT = case_when(
               str_detect(FINAL_RESULT, "POSITIVE") ~ "Positive",
               str_detect(FINAL_RESULT, "NEGATIVE") ~ "Negative",
               str_detect(FINAL_RESULT, "INDETERMINATE") ~ "Indeterminate",
               str_detect(FINAL_RESULT, "DUPLICATE") ~ "Duplicate",
            ),
         ) %>%
         filter(SOURCE != "JAY DUMMY LAB") %>%
         left_join(corr_data$SOURCE %>% distinct(SOURCE, SOURCE_FACI, SOURCE_SUB_FACI))
   }

   return(confirm_df)
}

##  Match pdf tables with OHASIS -----------------------------------------------

match_ohasis <- function(pdf_data) {
   log_info("Downloading data already in OHASIS.")
   db_conn <- ohasis$conn("db")

   # get list of labcodes
   labcodes <- unique(pdf_data$CONFIRMATORY_CODE)
   query    <- r"(
SELECT px_info.REC_ID,
       px_info.PATIENT_ID,
       COALESCE(px_confirm.CONFIRM_CODE, px_info.CONFIRMATORY_CODE)                                  AS CONFIRMATORY_CODE,
       px_info.UIC,
       px_info.PHILHEALTH_NO,
       px_info.SEX,
       px_info.BIRTHDATE,
       px_info.PATIENT_CODE,
       px_info.PHILSYS_ID,
       px_info.PRIME,
       px_info.CREATED_BY,
       px_info.CREATED_AT,
       px_info.UPDATED_BY,
       px_info.UPDATED_AT,
       px_info.DELETED_BY,
       1                                                                                             AS EXIST_INFO,
       IF(px_confirm.CONFIRM_CODE IS NOT NULL AND COALESCE(px_confirm.FINAL_RESULT, '') <> '', 1, 0) AS EXIST_CONFIRM,
       IF(px_test_hiv.DATE_COLLECT IS NOT NULL, 1, 0)                                                AS EXIST_TEST
FROM ohasis_interim.px_info
         JOIN ohasis_interim.px_record ON px_info.REC_ID = px_record.REC_ID
         LEFT JOIN ohasis_interim.px_confirm ON px_info.REC_ID = px_confirm.REC_ID
         LEFT JOIN ohasis_interim.px_test_hiv ON px_info.REC_ID = px_test_hiv.REC_ID AND px_test_hiv.TEST_TYPE = 31
WHERE px_record.MODULE = 2
  AND px_record.DELETED_AT IS NULL
  AND COALESCE(px_confirm.CONFIRM_CODE, px_info.CONFIRMATORY_CODE) IN (?)
      )"
   oh_data  <- dbxSelect(db_conn, query, params = list(labcodes))
   dbDisconnect(db_conn)

   log_info("Matchinng against PDF data.")
   # match with pdf
   data <- pdf_data %>%
      left_join(oh_data, join_by(CONFIRMATORY_CODE)) %>%
      mutate_at(
         .vars = vars(EXIST_INFO, EXIST_CONFIRM),
         ~coalesce(., 0)
      ) %>%
      mutate(
         priority     = case_when(
            str_left(CREATED_AT, 6) == "130000" ~ 1,
            TRUE ~ 2
         ),

         SEX          = coalesce(SEX.y, SEX.x),
         BIRTHDATE    = coalesce(BIRTHDATE.y, BIRTHDATE.x),
         PATIENT_CODE = coalesce(PATIENT_CODE.y, PATIENT_CODE.x),
      ) %>%
      arrange(priority) %>%
      distinct(CONFIRMATORY_CODE, .keep_all = TRUE)

   return(data)
}

##  Flag data for validation ---------------------------------------------------

get_checks <- function(pdf_data) {
   update <- input(
      prompt  = "Run `saccl_hiv_logsheet` validations?",
      options = c("1" = "yes", "2" = "no"),
      default = "1"
   )

   check <- list()
   if (update == "1") {
      check$SOURCE <- pdf_data %>%
         filter(is.na(SOURCE_FACI)) %>%
         distinct(SOURCE)
   }

   return(check)
}

##  Generating final data for import -------------------------------------------

prepare_import <- function(data) {
   TIMESTAMP <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

   col_dates <- select_if(data, .predicate = is.Date) %>% names()
   col_posix <- select_if(data, .predicate = is.POSIXct) %>% names()

   import <- data %>%
      mutate_at(
         .vars = vars(col_dates, col_posix),
         ~as.character(.)
      ) %>%
      mutate(
         MODULE       = 2,
         CLIENT_TYPE  = 4,

         # credentials
         CREATED_BY   = Sys.getenv("OH_USER_ID"),
         CREATED_AT   = coalesce(CREATED_AT, TIMESTAMP),
         UPDATED_BY   = Sys.getenv("OH_USER_ID"),
         UPDATED_AT   = coalesce(UPDATED_AT, TIMESTAMP),

         # confirmatory data
         FACI_ID      = "130023",
         SUB_FACI_ID  = "130023_001",
         CONFIRM_TYPE = 1,
         DATE_RELEASE = DATE_CONFIRM
      )

   log_info("Generating OHASIS IDs.")
   # generate ohasis data
   db_conn <- ohasis$conn("db")
   n_rows  <- nrow(import)
   pb      <- progress_bar$new(format = ":current of :total rows | [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed", total = n_rows, width = 100, clear = FALSE)
   pb$tick(0)
   for (i in seq_len(n_rows)) {
      # patient id
      pid_list <- unique(import$PATIENT_ID)
      pid_list <- pid_list[!is.na(pid_list)]
      pid_now  <- import[i,]$PATIENT_ID
      pid_new  <- pid_now
      if (is.na(pid_now)) {
         pid_new <- oh_px_id(db_conn, "130023")
         while (pid_new %in% pid_list)
            pid_new <- oh_px_id(db_conn, "130023", import[i,]$DATE_RECEIVE)
      }

      # record id
      rid_list <- unique(import$REC_ID)
      rid_list <- rid_list[!is.na(rid_list)]
      rid_now  <- import[i,]$REC_ID
      rid_new  <- rid_now
      if (is.na(rid_now)) {
         rid_new <- oh_rec_id(db_conn, Sys.getenv("OH_USER_ID"))
         while (rid_new %in% rid_list)
            rid_new <- oh_rec_id(db_conn, Sys.getenv("OH_USER_ID"))
      }

      import[i, "PATIENT_ID"] <- pid_new
      import[i, "REC_ID"]     <- rid_new
      pb$tick(1)
   }
   dbDisconnect(db_conn)

   return(import)
}

generate_tables <- function(import) {
   tables           <- list()
   tables$px_record <- list(
      name = "px_record",
      pk   = c("REC_ID", "PATIENT_ID"),
      data = import %>%
         filter(EXIST_INFO == 0 |
                   EXIST_CONFIRM == 0 |
                   coalesce(EXIST_TEST, 0) == 0) %>%
         mutate(
            FACI_ID     = "130000",
            SUB_FACI_ID = NA_character_,
            DISEASE     = "101000"
         ) %>%
         select(
            REC_ID,
            PATIENT_ID,
            FACI_ID,
            SUB_FACI_ID,
            RECORD_DATE = DATE_RECEIVE,
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
      data = import %>%
         filter(EXIST_INFO == 0) %>%
         select(
            REC_ID,
            PATIENT_ID,
            CONFIRMATORY_CODE,
            # PATIENT_CODE,
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
      data = import %>%
         filter(EXIST_INFO == 0) %>%
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
      data = import %>%
         filter(EXIST_CONFIRM == 0) %>%
         select(
            REC_ID,
            FACI_ID,
            SUB_FACI_ID,
            CONFIRM_TYPE,
            CONFIRM_CODE = CONFIRMATORY_CODE,
            CLIENT_TYPE,
            SOURCE       = SOURCE_FACI,
            SUB_SOURCE   = SOURCE_SUB_FACI,
            FINAL_RESULT,
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
      data = import %>%
         filter(coalesce(EXIST_TEST, 0) == 0) %>%
         select(-ends_with("_1"), ends_with("_2")) %>%
         select(
            REC_ID,
            FACI_ID,
            SUB_FACI_ID,
            DATE_PERFORM = DATE_CONFIRM,
            CREATED_BY,
            CREATED_AT,
            ends_with("KIT"),
            ends_with("RESULT"),
         ) %>%
         pivot_longer(
            cols = c(ends_with("KIT"), ends_with("RESULT"))
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
            FACI_ID,
            SUB_FACI_ID,
            TEST_TYPE,
            TEST_NUM,
            DATE_PERFORM,
            RESULT,
            CREATED_AT,
            CREATED_BY
         )
   )

   tables$px_test_hiv <- list(
      name = "px_test_hiv",
      pk   = c("REC_ID", "TEST_TYPE", "TEST_NUM"),
      data = import %>%
         filter(coalesce(EXIST_TEST, 0) == 0) %>%
         select(-ends_with("_1"), ends_with("_2")) %>%
         select(
            REC_ID,
            FACI_ID,
            SUB_FACI_ID,
            DATE_RECEIVE,
            DATE_COLLECT,
            CREATED_BY,
            CREATED_AT,
            ends_with("KIT"),
            ends_with("RESULT"),
         ) %>%
         pivot_longer(
            cols = c(ends_with("KIT"), ends_with("RESULT"))
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
            KIT_NAME     = KIT,
            FINAL_RESULT = RESULT,
            CREATED_AT,
            CREATED_BY
         )
   )

   return(tables)
}

import_data <- function(tables) {

   db_conn <- ohasis$conn("db")
   lapply(tables, function(ref, db_conn) {
      table_space <- Id(schema = "ohasis_interim", table = ref$name)
      dbxUpsert(db_conn, table_space, ref$data, ref$pk)
   }, db_conn)
   dbDisconnect(db_conn)
}

.init <- function(file = NULL, format = NULL) {
   p         <- parent.env(environment())
   p$results <- get_pdf_data(file, format) %>%
      match_ohasis()
   p$check   <- get_checks(p$results)
   p$import  <- prepare_import(p$results)
   p$tables  <- generate_tables(p$import)

   local(envir = .GlobalEnv, flow_validation(nhsss$harp_dx, "import_saccl_logsheet", ohasis$ym))
}

