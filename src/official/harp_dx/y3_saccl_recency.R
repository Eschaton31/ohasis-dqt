##  Read data from the pdf file ------------------------------------------------

get_pdf_data <- function(file = NULL) {
   if (is.null(file))
      file <- input("Kindly provide the unix path to the saccl pdf Logsheet.")

   if (tools::file_ext(file) == "pdf") {
      log_info("Extractinng tables from pdf.")
      lst        <- tabulizer::extract_tables(file = file, method = "lattice")
      recency_df <- lst %>%
         lapply(function(data) {
            col_need   <- c("lab#", "recencytestdate", "recencytestkit", "recencytestresult", "viralloadtestrequested", "viralloadtestdate", "viralloadtestresult")
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
                     . == "lab#" ~ "confirm_code",
                     . == "recencytestdate" ~ "rt_date",
                     . == "recencytestkit" ~ "rt_kit",
                     . == "recencytestresult" ~ "rt_result",
                     . == "viralloadtestrequested" ~ "rt_vl_requested",
                     . == "viralloadtestdate" ~ "rt_vl_date",
                     . == "viralloadtestresult" ~ "rt_vl_result",
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
      recency_df <- read_xlsx(file, .name_repair = "unique_quiet", col_types = "text") %>%
         rename_all(~str_replace_all(toupper(.), "\\s", "")) %>%
         rename_with(tolower) %>%
         rename_all(
            ~case_when(
               . == "lab#" ~ "confirm_code",
               . == "recencytestdate" ~ "rt_date",
               . == "recencytestkit" ~ "rt_kit",
               . == "recencytestresult" ~ "rt_result",
               . == "viralloadtestrequested" ~ "rt_vl_requested",
               . == "viralloadtestdate" ~ "rt_vl_date",
               . == "viralloadtestresult" ~ "rt_vl_result",
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
         confirm_code    = str_squish(confirm_code),
         confirm_code    = str_replace_all(confirm_code, '--', '-'),
         test_result     = case_when(
            str_detect(rt_result, "RECENT") ~ "1",
            str_detect(rt_result, "LONG-TERM") ~ "2",
            str_detect(rt_result, "INCONCLUSIVE") ~ "3",
         ),
         rt_agreed       = 1,
         rt_kit          = "1014",
         rt_result       = case_when(
            str_detect(rt_result, "RECENT") ~ "Recent Infection",
            str_detect(rt_result, "LONG-TERM") ~ "Long Term Infection",
            str_detect(rt_result, "RITA-LONG") ~ "Long Term Infection",
            str_detect(rt_result, "RITA LONG") ~ "Long Term Infection",
            str_detect(rt_result, "INCONCLUSIVE") ~ "Inconclusive",
            TRUE ~ rt_result
         ),
         rt_vl_requested = if_else(rt_vl_requested == "Yes", 1, 0, 0),
      ) %>%
      mutate_at(
         .vars = vars(rt_date, rt_vl_date),
         ~case_when(
            stri_detect_fixed(., "-") ~ as.Date(parse_date_time(., 'mdY')),
            stri_detect_fixed(., "-") ~ as.Date(parse_date_time(., 'Ymd')),
            !str_detect(., "[^[:digit:]]") ~ excel_numeric_to_date(as.numeric(.)),
         )
      )

   return(recency_df)
}

##  Match pdf tables with ohasis -----------------------------------------------

match_ohasis <- function(pdf_data) {
   # get list of labcodes
   log_info("Downloading data already in ohasis.")
   db_conn  <- connect('ohasis-live')
   labcodes <- unique(pdf_data$confirm_code)
   query    <- r"(
select px_confirm.*,
       1                                          as exist_confirm,
       if(px_rtri.rt_result is not NULL, 1, 0)    as exist_rt,
       if(px_test.test_kit is not NULL, 1, 0)     as exist_test,
       if(px_labs.lab_result is not NULL, 1, 0)   as exist_vl
from ohasis.px_confirm
         join ohasis.px_record on px_confirm.rec_id = px_record.rec_id
         left join ohasis.px_rtri on px_confirm.rec_id = px_rtri.rec_id
         left join ohasis.px_test on px_confirm.rec_id = px_test.rec_id and px_test.test_type = 60
         left join ohasis.px_labs on px_confirm.rec_id = px_labs.rec_id and px_labs.lab_test = 4
where px_record.module = 2
  and px_record.deleted_at is NULL
  and px_confirm.confirm_code in (?)
      )"
   oh_data  <- dbxSelect(db_conn, query, params = list(labcodes))
   dbDisconnect(db_conn)

   log_info("Matchinng against pdf data.")
   # match with pdf
   data <- pdf_data %>%
      left_join(oh_data, join_by(confirm_code), na_matches = 'never') %>%
      mutate_at(
         .vars = vars(exist_confirm, exist_rt, exist_test, exist_vl),
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
      check$confirm_not_oh <- pdf_data %>%
         filter(exist_confirm == 0)
   }

   return(check)
}

##  Generating final data for import -------------------------------------------

prepare_import <- function(data) {
   timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

   col_dates <- select_if(data, .predicate = is.Date) %>% names()
   col_posix <- select_if(data, .predicate = is.POSIXct) %>% names()

   import <- data %>%
      mutate_at(
         .vars = vars(c(col_dates, col_posix)),
         ~as.character(.)
      ) %>%
      mutate(
         # credentials
         created_by  = Sys.getenv("oh_user_id"),
         created_at  = coalesce(created_at, timestamp),
         updated_by  = Sys.getenv("oh_user_id"),
         updated_at  = coalesce(updated_at, timestamp),

         # confirmatory data
         faci_id     = "130023",
         sub_faci_id = "130023_001",
      ) %>%
      filter(!is.na(rec_id))

   return(import)
}

generate_tables <- function(import) {
   tables         <- list()
   tables$px_rtri <- list(
      name = "px_rtri",
      pk   = "rec_id",
      data = import %>%
         filter(exist_rt == 0) %>%
         select(
            rec_id,
            rt_agreed,
            rt_result,
            vl_requested = rt_vl_requested,
            created_by,
            created_at,
            updated_by,
            updated_at,
         )
   )

   tables$px_test <- list(
      name = "px_test",
      pk   = c("rec_id", "test_type", "test_num"),
      data = import %>%
         filter(exist_test == 0) %>%
         mutate(
            test_type = 60,
            test_num  = 1,
         ) %>%
         select(
            rec_id,
            faci_id,
            sub_faci_id,
            test_type,
            test_num,
            date_perform = rt_date,
            result       = test_result,
            test_kit     = rt_kit,
            created_at,
            created_by,
            updated_by,
            updated_at,
         )
   )

   tables$px_test_hiv <- list(
      name = "px_test_hiv",
      pk   = c("rec_id", "test_type", "test_num"),
      data = import %>%
         filter(exist_test == 0) %>%
         mutate(
            test_type   = 60,
            test_num    = 1,
            test_result = str_c("1", test_result)
         ) %>%
         select(
            rec_id,
            test_type,
            test_num,
            final_result = test_result,
            created_at,
            created_by,
            updated_by,
            updated_at,
         )
   )

   tables$px_labs <- list(
      name = "px_labs",
      pk   = c("rec_id", "lab_test"),
      data = import %>%
         filter(exist_vl == 0, rt_result == "Recent Infection", rt_vl_result != "") %>%
         mutate(
            lab_test = 4,
            test_num = 1,
         ) %>%
         select(
            rec_id,
            lab_test,
            lab_date   = rt_vl_date,
            lab_result = rt_vl_result,
            created_at,
            created_by,
            updated_by,
            updated_at,
         )
   )

   return(tables)
}

import_data <- function(tables) {
   db_conn <- connect('ohasis-live')
   lapply(tables, function(ref, db_conn) {
      table_space <- Id(schema = "ohasis", table = ref$name)
      dbxUpsert(db_conn, table_space, ref$data %>% filter(!is.na(rec_id)), ref$pk)
      update_credentials(ref$data$rec_id)
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