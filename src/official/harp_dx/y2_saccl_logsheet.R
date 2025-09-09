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
            date_collect      = 1,
            date_receive      = 2,
            confirmatory_code = 3,
            first             = 4,
            middle            = 5,
            last              = 6,
            patient_code      = 7,
            birthdate         = 8,
            age               = 9,
            sex               = 10,
            source            = 11,
            final_result      = 12,
            remarks           = 13,
            date_confirm      = 14,
            t0_cov_1          = 15,
            t0_abs_1          = 16,
            t0_result_1       = 17,
            t0_date_1         = 18,
            t0_cov_2          = 19,
            t0_abs_2          = 20,
            t0_result_2       = 21,
            t0_date_2         = 22,
         ) %>%
         mutate_at(
            .vars = vars(contains("date")),
            ~excel_numeric_to_date(as.numeric(.))
         ) %>%
         mutate(
            t0_date   = case_when(
               t0_date_1 == t0_date_2 ~ t0_date_1,
               t0_date_1 < t0_date_2 ~ t0_date_1,
               t0_date_1 > t0_date_2 ~ t0_date_2,
               TRUE ~ coalesce(t0_date_1, t0_date_2)
            ),
            t0_result = coalesce(t0_result_1, t0_result_2),
            t0_result = if_else(is.na(t0_result) & !is.na(t0_date), "REACTIVE", t0_result, t0_result)
         ) %>%
         mutate_if(
            .predicate = is.character,
            ~str_squish(.)
         ) %>%
         mutate_at(
            .vars = vars(
               confirmatory_code,
               first,
               middle,
               last,
               final_result,
               source,
               t0_result
            ),
            ~toupper(.)
         ) %>%
         # standardize
         mutate(
            sex          = case_when(
               sex == "M" ~ "1",
               sex == "MALE" ~ "1",
               sex == "F" ~ "2",
               sex == "FEMALE" ~ "2",
               TRUE ~ sex
            ),
            sex          = as.integer(sex),

            final_result = case_when(
               is.na(final_result) & str_detect(remarks, "^SAME AS") ~ "DUPLICATE",
               is.na(final_result) & str_detect(remarks, "^Submit plasma") ~ "INDETERMINATE",
               is.na(final_result) & str_detect(remarks, "Client is advised to proceed to the nearest") ~ "POSITIVE FOR HIV ANTIBODIES",
               is.na(final_result) & str_detect(remarks, "Fill out HIV care report") ~ "POSITIVE FOR HIV ANTIBODIES",
               is.na(final_result) & is.na(remarks) ~ "NEGATIVE",
               TRUE ~ final_result
            )
         ) %>%
         mutate(
            t1_kit       = "",

            t2_kit       = "",

            t3_kit       = "",
            final_result = case_when(
               str_detect(final_result, "POSITIVE") ~ "Positive",
               str_detect(final_result, "NEGATIVE") ~ "Negative",
               str_detect(final_result, "INDETERMINATE") ~ "Indeterminate",
               str_detect(final_result, "DUPLICATE") ~ "Duplicate",
               str_detect(toupper(remarks), "NONREACTIVE") ~ "Negative",
            ),
         ) %>%
         filter(source != "JAY DUMMY LAB") %>%
         left_join(corr_data$SOURCE %>%
                      distinct(SOURCE, SOURCE_FACI, SOURCE_SUB_FACI) %>%
                      rename_all(tolower))
   }

   return(confirm_df)
}

##  Match pdf tables with OHASIS -----------------------------------------------

match_ohasis <- function(pdf_data) {
   log_info("Downloading data already in OHASIS.")
   db_conn <- ohasis$conn("db")

   # get list of labcodes
   labcodes <- unique(pdf_data$confirmatory_code)
   query    <- r"(
select px_pii.rec_id,
       px_record.patient_id,
       coalesce(px_confirm.confirm_code, px_pii.confirmatory_code)                                  as confirmatory_code,
       px_pii.uic,
       px_pii.philhealth_no,
       px_pii.sex,
       px_pii.birthdate,
       px_pii.patient_code,
       px_pii.philsys_id,
       px_pii.created_by,
       px_pii.created_at,
       px_pii.updated_by,
       px_pii.updated_at,
       px_pii.deleted_by,
       1                                                                                             as exist_info,
       if(px_confirm.confirm_code is not null and coalesce(px_confirm.final_result, '') <> '', 1, 0) as exist_confirm,
       if(px_test.date_collect is not null, 1, 0)                                                as exist_test
from ohasis.px_pii
         join ohasis.px_record on px_pii.rec_id = px_record.rec_id
         left join ohasis.px_confirm on px_pii.rec_id = px_confirm.rec_id
         left join ohasis.px_test on px_pii.rec_id = px_test.rec_id and px_test.test_type = 31
where px_record.module = 2
  and px_record.deleted_at is null
  and coalesce(px_confirm.confirm_code, px_pii.confirmatory_code) in (?)
      )"
   oh_data  <- dbxSelect(db_conn, query, params = list(labcodes))
   dbDisconnect(db_conn)

   log_info("Matchinng against PDF data.")
   # match with pdf
   data <- pdf_data %>%
      left_join(oh_data, join_by(confirmatory_code)) %>%
      mutate_at(
         .vars = vars(exist_info, exist_confirm),
         ~coalesce(., 0)
      ) %>%
      mutate(
         priority     = case_when(
            str_left(created_at, 6) == "130000" ~ 1,
            TRUE ~ 2
         ),

         sex          = coalesce(sex.y, sex.x),
         birthdate    = coalesce(birthdate.y, birthdate.x),
         patient_code = coalesce(patient_code.y, patient_code.x),
      ) %>%
      arrange(priority) %>%
      distinct(confirmatory_code, .keep_all = TRUE)

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
      check$source <- pdf_data %>%
         filter(is.na(source_faci)) %>%
         distinct(source)
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
         .vars = vars(col_dates, col_posix),
         ~as.character(.)
      ) %>%
      mutate(
         module       = 2,
         client_type  = 4,

         # credentials
         created_by   = Sys.getenv("OH_USER_ID"),
         created_at   = coalesce(created_at, timestamp),
         updated_by   = Sys.getenv("OH_USER_ID"),
         updated_at   = coalesce(updated_at, timestamp),

         # confirmatory data
         faci_id      = "130023",
         sub_faci_id  = "130023_001",
         confirm_type = 1,
         date_release = date_confirm,

         row_id       = row_number(),
      )


   import %<>%
      filter(!is.na(patient_id)) %>%
      bind_rows(
         batch_px_ids(import %>% filter(is.na(patient_id)), patient_id, faci_id, "row_id")
      )

   import %<>%
      filter(!is.na(rec_id)) %>%
      bind_rows(
         batch_rec_ids(import %>% filter(is.na(rec_id)), rec_id, created_by, "row_id")
      )

   # log_info("Generating OHASIS IDs.")
   # # generate ohasis data
   # db_conn <- ohasis$conn("db")
   # n_rows  <- nrow(import)
   # pb      <- progress_bar$new(format = ":current of :total rows | [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed", total = n_rows, width = 100, clear = FALSE)
   # pb$tick(0)
   # for (i in seq_len(n_rows)) {
   #    # patient id
   #    pid_list <- unique(import$PATIENT_ID)
   #    pid_list <- pid_list[!is.na(pid_list)]
   #    pid_now  <- import[i,]$PATIENT_ID
   #    pid_new  <- pid_now
   #    if (is.na(pid_now)) {
   #       pid_new <- oh_px_id(db_conn, "130023")
   #       while (pid_new %in% pid_list)
   #          pid_new <- oh_px_id(db_conn, "130023", import[i,]$DATE_RECEIVE)
   #    }
   #
   #    # record id
   #    rid_list <- unique(import$REC_ID)
   #    rid_list <- rid_list[!is.na(rid_list)]
   #    rid_now  <- import[i,]$REC_ID
   #    rid_new  <- rid_now
   #    if (is.na(rid_now)) {
   #       rid_new <- oh_rec_id(db_conn, Sys.getenv("OH_USER_ID"))
   #       while (rid_new %in% rid_list)
   #          rid_new <- oh_rec_id(db_conn, Sys.getenv("OH_USER_ID"))
   #    }
   #
   #    import[i, "PATIENT_ID"] <- pid_new
   #    import[i, "REC_ID"]     <- rid_new
   #    pb$tick(1)
   # }
   # dbDisconnect(db_conn)

   return(import)
}

generate_tables <- function(import) {
   tables           <- list()
   tables$px_record <- list(
      name = "px_record",
      pk   = "rec_id",
      data = import %>%
         filter(exist_info == 0 |
                   exist_confirm == 0 |
                   coalesce(exist_test, 0) == 0) %>%
         mutate(
            faci_id     = "130000",
            sub_faci_id = NA_character_,
            disease     = "101000"
         ) %>%
         select(
            rec_id,
            patient_id,
            faci_id,
            sub_faci_id,
            record_date = date_receive,
            disease,
            module,
            created_by,
            created_at,
            updated_by,
            updated_at,
         )
   )

   tables$patients <- list(
      name = "patients",
      pk   = "patient_id",
      data = import %>%
         filter(exist_info == 0) %>%
         select(
            patient_id,
            faci_id,
            sub_faci_id,
            first,
            middle,
            last,
            confirmatory_code,
            sex,
            birthdate,
            created_by,
            created_at,
            updated_by,
            updated_at,
         ) %>%
         distinct()
   )

   tables$px_pii <- list(
      name = "px_pii",
      pk   = "rec_id",
      data = import %>%
         filter(exist_info == 0) %>%
         select(
            rec_id,
            first,
            middle,
            last,
            confirmatory_code,
            sex,
            birthdate,
            created_by,
            created_at,
            updated_by,
            updated_at,
         )
   )

   tables$px_confirm <- list(
      name = "px_confirm",
      pk   = "rec_id",
      data = import %>%
         filter(exist_confirm == 0) %>%
         select(
            rec_id,
            faci_id,
            sub_faci_id,
            confirm_type,
            confirm_code = confirmatory_code,
            client_type,
            source       = source_faci,
            sub_source   = source_sub_faci,
            final_result,
            remarks,
            date_confirm,
            date_release,
            created_at,
            created_by
         )
   )

   tables$px_test <- list(
      name = "px_test",
      pk   = c("rec_id", "test_type", "test_num"),
      data = import %>%
         filter(coalesce(exist_test, 0) == 0) %>%
         select(-ends_with("_1"), ends_with("_2")) %>%
         select(
            rec_id,
            faci_id,
            sub_faci_id,
            date_perform = date_confirm,
            date_receive,
            date_collect,
            created_by,
            created_at,
            ends_with("kit"),
            ends_with("result"),
         ) %>%
         pivot_longer(
            cols = c(ends_with("kit"), ends_with("result"))
         ) %>%
         separate_wider_delim(name, "_", names = c("test_type", "var")) %>%
         filter(test_type != "final") %>%
         mutate(
            test_type = case_when(
               test_type == "t0" ~ "10",
               test_type == "t1" ~ "31",
               test_type == "t2" ~ "32",
               test_type == "t3" ~ "33",
            ),
            value     = case_when(
               value == "REACTIVE" ~ "10",
               value == "NONREACTIVE" ~ "20",
               TRUE ~ value
            ),
            test_num  = 1
         ) %>%
         distinct(
            rec_id,
            faci_id,
            sub_faci_id,
            created_by,
            created_at,
            date_receive,
            date_collect,
            test_type,
            test_num,
            .keep_all = TRUE
         ) %>%
         pivot_wider(
            id_cols     = c(
               rec_id,
               faci_id,
               sub_faci_id,
               created_by,
               created_at,
               test_type,
               test_num,
               date_perform,
               date_receive,
               date_collect,
            ),
            names_from  = var,
            values_from = value
         ) %>%
         select(
            rec_id,
            faci_id,
            sub_faci_id,
            test_type,
            test_num,
            date_receive,
            date_collect,
            date_perform,
            result,
            created_at,
            created_by
         )
   )

   tables$px_test_hiv <- list(
      name = "px_test_hiv",
      pk   = c("rec_id", "test_type", "test_num"),
      data = import %>%
         filter(coalesce(exist_test, 0) == 0) %>%
         select(-ends_with("_1"), ends_with("_2")) %>%
         select(
            rec_id,
            faci_id,
            sub_faci_id,
            created_by,
            created_at,
            ends_with("result"),
         ) %>%
         pivot_longer(
            cols = c(ends_with("result"))
         ) %>%
         separate_wider_delim(name, "_", names = c("test_type", "var")) %>%
         filter(test_type != "final") %>%
         mutate(
            test_type = case_when(
               test_type == "t0" ~ "10",
               test_type == "t1" ~ "31",
               test_type == "t2" ~ "32",
               test_type == "t3" ~ "33",
            ),
            value     = case_when(
               value == "REACTIVE" ~ "10",
               value == "NONREACTIVE" ~ "20",
               TRUE ~ value
            ),
            test_num  = 1
         ) %>%
         distinct(
            rec_id,
            faci_id,
            sub_faci_id,
            created_by,
            created_at,
            test_type,
            test_num,
            .keep_all = TRUE
         ) %>%
         pivot_wider(
            id_cols     = c(
               rec_id,
               faci_id,
               sub_faci_id,
               created_by,
               created_at,
               test_type,
               test_num,
            ),
            names_from  = var,
            values_from = value
         ) %>%
         select(
            rec_id,
            faci_id,
            sub_faci_id,
            test_type,
            test_num,
            final_result = result,
            created_at,
            created_by
         )
   )

   return(tables)
}

import_data <- function(tables) {

   db_conn <- ohasis$conn("db")
   lapply(tables, function(ref, db_conn) {
      table_space <- Id(schema = "ohasis", table = ref$name)
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

