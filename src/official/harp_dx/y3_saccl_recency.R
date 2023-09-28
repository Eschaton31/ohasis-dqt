##  Read data from the pdf file ------------------------------------------------

get_pdf_data <- function(file = NULL) {
   if (is.null(file))
      file <- input("Kindly provide the UNIX path to the SACCL PDF Logsheet.")

   if (tools::file_ext(file) == "pdf") {
      log_info("Extractinng tables from PDF.")
      lst        <- tabulizer::extract_tables(file = file, method = "lattice")
      recency_df <- lst %>%
         lapply(function(data) {
            col_need   <- c("LAB#", "RECENCYTESTDATE", "RECENCYTESTKIT", "RECENCYTESTRESULT", "VIRALLOADTESTREQUESTED", "VIRALLOADTESTDATE", "VIRALLOADTESTRESULT")
            col_val    <- str_replace_all(toupper(data[1,]), "\\s", "")
            col_key    <- seq_len(length(col_val))
            col_select <- c()
            for (i in col_key) {
               if (col_val[i] %in% col_need)
                  col_select <- c(col_select, i)
            }

            data %<>%
               as_tibble() %>%
               select(col_select)

            col_final   <- str_replace_all(toupper(data[1,]), "\\s", "")
            names(data) <- col_final

            data %<>%
               slice(-1) %>%
               rename_all(
                  ~case_when(
                     . == "LAB#" ~ "CONFIRM_CODE",
                     . == "RECENCYTESTDATE" ~ "RT_DATE",
                     . == "RECENCYTESTKIT" ~ "RT_KIT",
                     . == "RECENCYTESTRESULT" ~ "RT_RESULT",
                     . == "VIRALLOADTESTREQUESTED" ~ "RT_VL_REQUESTED",
                     . == "VIRALLOADTESTDATE" ~ "RT_VL_DATE",
                     . == "VIRALLOADTESTRESULT" ~ "RT_VL_RESULT",
                     TRUE ~ .
                  )
               )

            return(data)
         }) %>%
         bind_rows() %>%
         mutate_if(
            .predicate = is.character,
            ~toupper(str_squish(.))
         )

   } else if (tools::file_ext(file) == "xlsx") {
      recency_df <- read_xlsx(file, .name_repair = "unique_quiet") %>%
         rename_all(~str_replace_all(toupper(.), "\\s", "")) %>%
         rename_all(
            ~case_when(
               . == "LAB#" ~ "CONFIRM_CODE",
               . == "RECENCYTESTDATE" ~ "RT_DATE",
               . == "RECENCYTESTKIT" ~ "RT_KIT",
               . == "RECENCYTESTRESULT" ~ "RT_RESULT",
               . == "VIRALLOADTESTREQUESTED" ~ "RT_VL_REQUESTED",
               . == "VIRALLOADTESTDATE" ~ "RT_VL_DATE",
               . == "VIRALLOADTESTRESULT" ~ "RT_VL_RESULT",
               TRUE ~ .
            )
         )
   }

   recency_df %<>%
      mutate_if(
         .predicate = is.character,
         ~str_squish(toupper(.))
      ) %>%
      mutate(
         TEST_RESULT     = case_when(
            str_detect(RT_RESULT, "RECENT") ~ "1",
            str_detect(RT_RESULT, "LONG-TERM") ~ "2",
            str_detect(RT_RESULT, "INCONCLUSIVE") ~ "3",
         ),
         RT_AGREED       = 1,
         RT_KIT          = "1014",
         RT_RESULT       = case_when(
            str_detect(RT_RESULT, "RECENT") ~ "Recent Infection",
            str_detect(RT_RESULT, "LONG-TERM") ~ "Long Term Infection",
            str_detect(RT_RESULT, "INCONCLUSIVE") ~ "Inconclusive",
            TRUE ~ RT_RESULT
         ),
         RT_VL_REQUESTED = if_else(RT_VL_REQUESTED == "Yes", 1, 0, 0),
      ) %>%
      mutate_at(
         .vars = vars(RT_DATE, RT_VL_DATE),
         ~case_when(
            str_detect(., "/") ~ as.Date(., "%m/%d/%Y"),
            StrIsNumeric(.) ~ excel_numeric_to_date(as.numeric(.)),
         )
      )

   return(recency_df)
}

##  Match pdf tables with OHASIS -----------------------------------------------

match_ohasis <- function(pdf_data) {
   # get list of labcodes
   log_info("Downloading data already in OHASIS.")
   db_conn  <- ohasis$conn("db")
   labcodes <- unique(pdf_data$CONFIRM_CODE)
   query    <- r"(
SELECT px_confirm.*,
       1                                          AS EXIST_CONFIRM,
       IF(px_rtri.RT_RESULT IS NOT NULL, 1, 0)    AS EXIST_RT,
       IF(px_test_hiv.KIT_NAME IS NOT NULL, 1, 0) AS EXIST_TEST,
       IF(px_labs.LAB_RESULT IS NOT NULL, 1, 0)   AS EXIST_VL
FROM ohasis_interim.px_confirm
         JOIN ohasis_interim.px_record ON px_confirm.REC_ID = px_record.REC_ID
         LEFT JOIN ohasis_interim.px_rtri ON px_confirm.REC_ID = px_rtri.REC_ID
         LEFT JOIN ohasis_interim.px_test_hiv ON px_confirm.REC_ID = px_test_hiv.REC_ID AND px_test_hiv.TEST_TYPE = 60
         LEFT JOIN ohasis_interim.px_labs ON px_confirm.REC_ID = px_labs.REC_ID AND px_labs.LAB_TEST = 4
WHERE px_record.MODULE = 2
  AND px_record.DELETED_AT IS NULL
  AND px_confirm.CONFIRM_CODE IN (?)
      )"
   oh_data  <- dbxSelect(db_conn, query, params = list(labcodes))
   dbDisconnect(db_conn)

   log_info("Matchinng against PDF data.")
   # match with pdf
   data <- pdf_data %>%
      left_join(oh_data, join_by(CONFIRM_CODE)) %>%
      mutate_at(
         .vars = vars(EXIST_CONFIRM, EXIST_RT, EXIST_TEST, EXIST_VL),
         ~coalesce(., 0)
      )

   return(data)
}

##  Flag data for validation ---------------------------------------------------

get_checks <- function(pdf_data) {
   update <- input(
      prompt  = "Run `saccl_recency_logsheet` validations?",
      options = c("1" = "yes", "2" = "no"),
      default = "1"
   )

   check <- list()
   if (update == "1") {
      check$CONFIRM_NOT_OH <- pdf_data %>%
         filter(EXIST_CONFIRM == 0)
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
         .vars = vars(c(col_dates, col_posix)),
         ~as.character(.)
      ) %>%
      mutate(
         # credentials
         CREATED_BY  = Sys.getenv("OH_USER_ID"),
         CREATED_AT  = coalesce(CREATED_AT, TIMESTAMP),
         UPDATED_BY  = Sys.getenv("OH_USER_ID"),
         UPDATED_AT  = coalesce(UPDATED_AT, TIMESTAMP),

         # confirmatory data
         FACI_ID     = "130023",
         SUB_FACI_ID = "130023_001",
      )

   return(import)
}

generate_tables <- function(import) {
   tables         <- list()
   tables$px_rtri <- list(
      name = "px_rtri",
      pk   = "REC_ID",
      data = import %>%
         filter(EXIST_RT == 0) %>%
         select(
            REC_ID,
            RT_AGREED,
            RT_RESULT,
            VL_REQUESTED = RT_VL_REQUESTED,
            CREATED_BY,
            CREATED_AT,
            UPDATED_BY,
            UPDATED_AT,
         )
   )

   tables$px_test <- list(
      name = "px_test",
      pk   = c("REC_ID", "TEST_TYPE", "TEST_NUM"),
      data = import %>%
         filter(EXIST_TEST == 0) %>%
         mutate(
            TEST_TYPE = 60,
            TEST_NUM  = 1,
         ) %>%
         select(
            REC_ID,
            FACI_ID,
            SUB_FACI_ID,
            TEST_TYPE,
            TEST_NUM,
            DATE_PERFORM = RT_DATE,
            RESULT       = TEST_RESULT,
            CREATED_AT,
            CREATED_BY,
            UPDATED_BY,
            UPDATED_AT,
         )
   )

   tables$px_test_hiv <- list(
      name = "px_test_hiv",
      pk   = c("REC_ID", "TEST_TYPE", "TEST_NUM"),
      data = import %>%
         filter(EXIST_TEST == 0) %>%
         mutate(
            TEST_TYPE   = 60,
            TEST_NUM    = 1,
            TEST_RESULT = str_c("1", TEST_RESULT)
         ) %>%
         select(
            REC_ID,
            FACI_ID,
            SUB_FACI_ID,
            TEST_TYPE,
            TEST_NUM,
            KIT_NAME     = RT_KIT,
            FINAL_RESULT = TEST_RESULT,
            CREATED_AT,
            CREATED_BY,
            UPDATED_BY,
            UPDATED_AT,
         )
   )

   tables$px_labs <- list(
      name = "px_labs",
      pk   = c("REC_ID", "LAB_TEST"),
      data = import %>%
         filter(EXIST_VL == 0, RT_RESULT == "Recent Infection", RT_VL_RESULT != "") %>%
         mutate(
            LAB_TEST = 4,
            TEST_NUM = 1,
         ) %>%
         select(
            REC_ID,
            LAB_TEST,
            LAB_DATE   = RT_VL_DATE,
            LAB_RESULT = RT_VL_RESULT,
            CREATED_AT,
            CREATED_BY,
            UPDATED_BY,
            UPDATED_AT,
         )
   )

   return(tables)
}

import_data <- function(tables) {

   db_conn <- ohasis$conn("db")
   lapply(tables, function(ref, db_conn) {
      table_space <- Id(schema = "ohasis_interim", table = ref$name)
      dbxUpsert(db_conn, table_space, ref$data, ref$pk)
      update_credentials(ref$data$REC_ID)
   }, db_conn)
   dbDisconnect(db_conn)
}

.init <- function() {
   p         <- parent.env(environment())
   p$results <- get_pdf_data() %>%
      match_ohasis()
   p$check   <- get_checks(p$results)
   p$import  <- prepare_import(p$results)
   p$tables  <- generate_tables(p$import)

   local(envir = .GlobalEnv, flow_validation(nhsss$harp_dx, "import_saccl_recency", ohasis$ym))
}