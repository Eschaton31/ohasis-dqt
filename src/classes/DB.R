##  DBMS Class -----------------------------------------------------------------

# Database accessor
DB <- setRefClass(
   Class    = "DB",
   contains = "Project",
   fields   = list(
      timestamp     = "character",
      output_title  = "character",
      internet      = "data.frame",
      db_checks     = "list",
      ref_addr      = "data.frame",
      ref_country   = "data.frame",
      ref_faci      = "data.frame",
      ref_faci_code = "data.frame",
      ref_staff     = "data.frame"
   ),
   methods  = list(
      initialize        = function(ref_yr = NULL, ref_mo = NULL, title = NULL) {
         "This method is called when you create an instance of this class."

         # get current time
         timestamp    <<- format(Sys.time(), "%Y.%m.%d.%H%M%S")
         output_title <<- paste0(timestamp, "-", Sys.getenv("LW_USER"))

         # set the report
         callSuper()$set_report(ref_yr, ref_mo, title)

         # check internet speed
         # check_speed <- input(
         #    prompt  = "Run the speed test?",
         #    options = c("1" = "yes", "2" = "no"),
         #    default = "1"
         # )
         # check_speed <- substr(toupper(check_speed), 1, 1)
         # if (check_speed == "1") {
         #    .log_info("Checking internet speed.")
         #    internet <<- .self$speedtest()
         #    .log_info("Current download speed: {underline(red(internet$speed_down_megabits_sec))} Mbps")
         #    .log_info("Current upload speed: {underline(red(internet$speed_up_megabits_sec))} Mbps")
         # }

         # check database consistency
         .log_info("Checking database for inconsistencies.")
         db_checks <<- .self$check_consistency()

         # update data lake
         .self$update_lake()

         # download latest references before final initialization
         .log_info("Downloading references.")
         db_conn           <- .self$conn("db")
         lw_conn           <- .self$conn("lw")
         .self$ref_addr    <<- dbTable(lw_conn, "ohasis_lake", "ref_addr", name = "ref_addr")
         .self$ref_country <<- dbTable(lw_conn, "ohasis_interim", "addr_country", name = "addr_country")
         .self$ref_faci    <<- dbTable(lw_conn, "ohasis_lake", "ref_faci", name = "ref_faci")
         .self$ref_staff   <<- dbTable(lw_conn, "ohasis_lake", "ref_staff", name = "ref_staff")

         # subset of ref_faci
         .self$ref_faci_code <<- ref_faci %>%
            filter(!is.na(FACI_CODE)) %>%
            distinct(FACI_CODE, .keep_all = TRUE) %>%
            rename(
               SUB_FACI_CODE = FACI_CODE
            ) %>%
            mutate(
               FACI_CODE     = case_when(
                  stri_detect_regex(SUB_FACI_CODE, "^HASH") ~ "HASH",
                  stri_detect_regex(SUB_FACI_CODE, "^SAIL") ~ "SAIL",
                  stri_detect_regex(SUB_FACI_CODE, "^TLY") ~ "TLY",
                  TRUE ~ SUB_FACI_CODE
               ),
               SUB_FACI_CODE = if_else(
                  condition = nchar(SUB_FACI_CODE) == 3,
                  true      = NA_character_,
                  false     = SUB_FACI_CODE
               ),
               SUB_FACI_CODE = case_when(
                  FACI_CODE == "HASH" & is.na(SUB_FACI_CODE) ~ "HASH-QC",
                  FACI_CODE == "TLY" & is.na(SUB_FACI_CODE) ~ "TLY-ANGLO",
                  FACI_CODE == "SHP" & is.na(SUB_FACI_CODE) ~ "SHIP-MAKATI",
                  TRUE ~ SUB_FACI_CODE
               ),
            ) %>%
            relocate(FACI_CODE, SUB_FACI_CODE, .before = 1)

         dbDisconnect(db_conn)
         dbDisconnect(lw_conn)

         .log_success("OHASIS initialized!")
      },

      # refreshes db connection
      conn              = function(db = NULL) {
         # live database
         if (db == "db")
            db_conn <- dbConnect(
               RMariaDB::MariaDB(),
               user     = Sys.getenv("DB_USER"),
               password = Sys.getenv("DB_PASS"),
               host     = Sys.getenv("DB_HOST"),
               port     = Sys.getenv("DB_PORT"),
               timeout  = -1
            )

         # live data lake / warehouse
         if (db == "lw")
            db_conn <- dbConnect(
               RMariaDB::MariaDB(),
               user     = Sys.getenv("LW_USER"),
               password = Sys.getenv("LW_PASS"),
               host     = Sys.getenv("LW_HOST"),
               port     = Sys.getenv("LW_PORT"),
               timeout  = -1
            )
         return(db_conn)
      },

      # update lake
      update_lake       = function() {
         update <- input(
            prompt  = "Update the data lake?",
            options = c("1" = "yes", "2" = "no", "3" = "all"),
            default = "1"
         )
         if (update %in% c("1", "3")) {
            # get required refresh data & upsert only
            for (update_type in (c("refresh", "upsert"))) {
               # if all should be updated
               if (update == "3")
                  default_yes <- TRUE
               else
                  default_yes <- FALSE

               lake_table <- function(table_name) {
                  table_name <- stri_replace_last_fixed(table_name, ".R", "")
                  .self$data_factory("lake", table_name, update_type, default_yes)
               }

               lake_dir <- file.path(getwd(), "src", "data_lake", update_type)
               lapply(list.files(lake_dir, pattern = "ref_*.*\\.R"), lake_table)
               lapply(list.files(lake_dir, pattern = "px_*.*\\.R"), lake_table)
               lapply(list.files(lake_dir, pattern = "lab_*.*\\.R"), lake_table)
               lapply(list.files(lake_dir, pattern = "disp_*.*\\.R"), lake_table)
            }
            .log_info("Data lake updated!")
         }
      },

      # upsert data
      upsert            = function(db_conn = NULL, db_type = NULL, table_name = NULL, data = NULL, id_col = NULL) {
         if (db_type %in% c("warehouse", "lake", "api"))
            db_name <- paste0("ohasis_", db_type)
         else
            db_name <- db_type

         table_space <- Id(schema = db_name, table = table_name)
         table_sql   <- DBI::SQL(paste0('`', db_name, '`.`', table_name, '`'))

         # check if table exists, if not create
         if (!dbExistsTable(db_conn, table_space)) {
            sql <- .self$create(db_name, table_name, data, id_col)
            dbExecute(db_conn, sql)
         }

         # compare columns match, if not re-create table
         names_data  <- sort(names(data))
         names_table <- sort(dbListFields(db_conn, table_space))
         if (!identical(names_data, names_table) & length(setdiff(names_data, names_table)) > 0) {
            # get current records (assuming change is only added columns)
            df <- dbReadTable(db_conn, table_space)

            # recreate table using new columns
            sql <- .self$create(db_name, table_name, data, id_col)
            dbExecute(db_conn, sql)

            # re-insert data
            dbAppendTable(db_conn, table_sql, df)
         }

         # upsert data
         chunk_size <- 1000
         if (nrow(data) >= chunk_size) {
            # upload in chunks to monitor progress
            n_rows     <- nrow(data)
            n_chunks   <- rep(1:ceiling(n_rows / chunk_size), each = chunk_size)[1:n_rows]
            data       <- split(data, n_chunks)
            data_bytes <- as.numeric(object.size(data))

            # get progress
            pb <- progress_bar$new(format = ":bytes uploaded | :rate [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed", total = data_bytes, width = 100, clear = FALSE)
            pb$tick(0)
            for (i in seq_len(length(data))) {
               chunk_bytes <- as.numeric(object.size(data[[i]]))
               dbxUpsert(db_conn, table_space, data[[i]], id_col)
               pb$tick(chunk_bytes)
            }
            cat("\n")
         } else {
            dbxUpsert(db_conn, table_space, data, id_col)
         }
      },

      # create table
      create            = function(db_name = NULL, table_name = NULL, data = NULL, id_col = NULL) {
         # attached columns
         user_cols <- c("CREATED_BY", "UPDATED_BY", "DELETED_BY", "PROVIDER_ID", "SIGNATORY_1", "SIGNATORY_2", "SIGNATORY_2", "USER_ID", "STAFF_ID")
         text_faci <- c("PREV_TEST_FACI", "DELIVER_FACI", "FACI_LABEL", "FACI_NAME", "FACI_NAME_CLEAN", "FACI_NAME_REG", "FACI_NAME_PROV", "FACI_NAME_MUNC",
                        "FACI_ADDR", "FACI_NHSSS_REG", "FACI_NHSSS_PROV", "FACI_NHSSS_MUNC")

         # construct create based on data types
         df_str <- data %>%
            summary.default %>%
            as.data.frame %>%
            group_by(Var1) %>%
            spread(key = Var2, value = Freq) %>%
            ungroup %>%
            mutate(
               Type = case_when(
                  Var1 %in% text_faci ~ "TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci'",
                  Var1 %in% user_cols & !(Var1 %in% id_col) ~ "CHAR(10) NULL DEFAULT NULL COLLATE 'utf8_general_ci'",
                  Var1 %in% user_cols & Var1 %in% id_col ~ "CHAR(10) NULL COLLATE 'utf8_general_ci'",
                  Var1 == "FACI_CODE" ~ "VARCHAR(255) NULL COLLATE 'utf8_general_ci'",
                  Var1 == "LONG" ~ "DECIMAL(10,7) NULL COLLATE 'utf8_general_ci'",
                  Var1 == "LAT" ~ "DECIMAL(9,7) NULL COLLATE 'utf8_general_ci'",
                  Var1 == "REC_ID" ~ "CHAR(25) NULL COLLATE 'utf8_general_ci'",
                  Var1 == "SOURCE_REC" ~ "CHAR(25) NULL COLLATE 'utf8_general_ci'",
                  Var1 == "DESTINATION_REC" ~ "CHAR(25) NULL COLLATE 'utf8_general_ci'",
                  Var1 == "REC_ID_GRP" ~ "VARCHAR(100) NULL COLLATE 'utf8_general_ci'",
                  Var1 == "CENTRAL_ID" ~ "CHAR(18) NULL COLLATE 'utf8_general_ci'",
                  Var1 == "PATIENT_ID" ~ "CHAR(18) NULL COLLATE 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "PSGC") ~ "CHAR(9) NULL COLLATE 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "ADDR") ~ "TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "SUB_FACI") ~ "CHAR(10) NULL COLLATE 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "SUB_SOURCE") ~ "CHAR(10) NULL COLLATE 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "FACI") ~ "CHAR(6) NULL COLLATE 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "SOURCE") ~ "CHAR(6) NULL COLLATE 'utf8_general_ci'",
                  Mode == "numeric" & Class == "Date" ~ "DATE NULL DEFAULT NULL",
                  Mode == "numeric" & Class == "POSIXct" ~ "DATETIME NULL DEFAULT NULL",
                  Mode == "numeric" ~ "INT(11) NULL DEFAULT NULL",
                  Mode == "character" ~ "TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci'",
                  TRUE ~ NA_character_
               ),
               SQL  = paste0("`", Var1, "` ", Type),
            )

         # add indices if not in pk
         index <- ""
         if (!("CENTRAL_ID" %in% id_col) & "CENTRAL_ID" %in% names(data))
            index <- ", INDEX `CENTRAL_ID` (`CENTRAL_ID`)"
         if (!("PATIENT_ID" %in% id_col) & "PATIENT_ID" %in% names(data))
            index <- ", INDEX `PATIENT_ID` (`PATIENT_ID`)"

         # implode into query
         pk_sql     <- paste(collapse = "`,`", id_col)
         create_sql <- paste(collapse = ",", df_str$SQL)
         create_sql <- glue("CREATE TABLE `{db_name}`.`{table_name}` (\n",
                            "{create_sql},\n",
                            "\nPRIMARY KEY(`{pk_sql}`) {index}\n)\n",
                            "COLLATE='utf8_general_ci'\nENGINE=InnoDB;")
         return(create_sql)
      },

      # update lake
      data_factory      = function(db_type = NULL, table_name = NULL, update_type = NULL, default_yes = FALSE, from = NULL, to = NULL) {
         # append "ohasis_" as db name
         db_name     <- ifelse(db_type %in% c("lake", "warehouse"), paste0("ohasis_", db_type), db_type)
         table_space <- Id(schema = db_name, table = table_name)

         # get input
         if (default_yes == TRUE) {
            update <- "1"
         } else {
            update <- input(
               prompt  = paste0("Update ", red(table_name), "?"),
               options = c("1" = "yes", "2" = "no"),
               default = "1"
            )
         }

         # update
         if (update == "1") {
            .log_info("Updating {red(table_name)} @ the {red(db_type)}.")

            # get files
            factory_dir   <- file.path(getwd(), "src", paste0("data_", db_type))
            factory_files <- dir_info(factory_dir, recurse = TRUE, regexp = table_name)
            factory_file  <- filter(factory_files, basename(path) == paste0(table_name, '.R'))$path
            factory_sql   <- filter(factory_files, basename(path) == paste0(table_name, '.sql'))$path

            # open connections
            .log_info("Opening connections.")
            db_conn      <- .self$conn("db")
            lw_conn      <- .self$conn("lw")
            table_exists <- dbExistsTable(lw_conn, table_space)

            # data for deletion (warehouse)
            for_delete <- data.frame()

            # read sql first, then parse for necessary snapshots
            query_snapshot <- paste0("SELECT MAX(SNAPSHOT) AS snapshot FROM ", db_name, ".", table_name)
            if (length(factory_sql) != 0) {
               sql_query   <- read_file(factory_sql)
               sql_tables  <- substr(sql_query,
                                     stri_locate_first_fixed(sql_query, "FROM "),
                                     stri_locate_first_fixed(sql_query, "-- ID_COLS") - 1)
               query_table <- substr(sql_query,
                                     1,
                                     stri_locate_first_fixed(sql_query, "-- ID_COLS") - 1)
               query_nrow  <- paste0("SELECT COUNT(*) AS nrow ", sql_tables)
               id_col      <- substr(sql_query,
                                     stri_locate_first_fixed(sql_query, "-- ID_COLS: ") + 12,
                                     nchar(sql_query))
               id_col      <- str_split(id_col, ", ")[[1]]
            }

            # snapshots are the reference for scoping
            # get start date of records  to be fetched
            if (!is.null(from)) {
               snapshot_old <- from
            } else if (update_type == "refresh") {
               snapshot_old <- "1970-01-01 00:00:00"

               if (table_exists)
                  dbRemoveTable(lw_conn, table_space)
            } else if (table_exists) {
               snapshot_old <- as.character(dbGetQuery(lw_conn, query_snapshot)$snapshot)
            }

            # get end date of records  to be fetched
            if (!is.null(to)) {
               snapshot_new <- to
            } else {
               snapshot_new <- str_split(.self$timestamp, "\\.")[[1]]
               snapshot_new <- paste0(snapshot_new[1], "-",
                                      snapshot_new[2], "-",
                                      snapshot_new[3], " ",
                                      substr(snapshot_new[4], 1, 2), ":",
                                      substr(snapshot_new[4], 3, 4), ":",
                                      substr(snapshot_new[4], 5, 6))
            }

            # # rollback 1 month to get other changes
            # snapshot_old <- as.POSIXct(snapshot_old) %m-%
            #    days(3) %>%
            #    format("%Y-%m-%d %H:%M:%S")

            # run data lake script for object
            .log_info("Getting new/updated data.")
            if (length(factory_sql) != 0) {
               # get number of affected rows
               object <- tibble()
               params <- list(snapshot_old, snapshot_new, snapshot_old, snapshot_new, snapshot_old, snapshot_new)
               n_rows <- dbGetQuery(db_conn, query_nrow, params = params)$nrow
               n_rows <- ifelse(length(n_rows) > 1, length(n_rows), as.numeric(n_rows))
               n_rows <- as.integer(n_rows)

               # get actual result set
               .log_info("Number of records to fetch = {green(formatC(n_rows, big.mark = ','))}.")
               rs <- dbSendQuery(db_conn, query_table, params = params)

               chunk_size <- 1000
               if (!is.na(n_rows) && n_rows >= chunk_size) {
                  # upload in chunks to monitor progress
                  n_chunks <- ceiling(n_rows / chunk_size)

                  # get progress
                  pb_name <- paste0(table_name, ": :current of :total chunks [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed")

                  pb <- progress_bar$new(format = pb_name, total = n_chunks, width = 100, clear = FALSE)
                  pb$tick(0)

                  # fetch in chunks
                  for (i in seq_len(n_chunks)) {
                     chunk  <- dbFetch(rs, chunk_size)
                     object <- bind_rows(object, chunk)
                     pb$tick(1)
                  }
               } else {
                  object <- dbFetch(rs)
               }
               dbClearResult(rs)

               for_delete <- object %>% select({ { id_col } }) %>% distinct_all()
               continue   <- ifelse(nrow(object) == 0, 0, 1)
            } else {
               source(factory_file, local = TRUE)
            }

            # keep connection alive
            dbDisconnect(lw_conn)
            lw_conn <- .self$conn("lw")

            if (continue > 0) {
               # check if there is data for deletion
               if (nrow(for_delete) > 0 && table_exists) {
                  .log_info("Number of invalidated records = {red(formatC(nrow(for_delete), big.mark = ','))}.")
                  dbxDelete(
                     lw_conn,
                     table_space,
                     for_delete,
                     batch_size = 1000
                  )
                  .log_success("Invalidated records removed.")
               }

               .log_info("Payload = {red(formatC(nrow(object), big.mark = ','))} rows.")
               .self$upsert(lw_conn, db_type, table_name, object, id_col)
               # update reference
               df <- data.frame(
                  user      = Sys.getenv("LW_USER"),
                  report_yr = .self$yr,
                  report_mo = .self$mo,
                  run_title = paste0(.self$timestamp, " (", .self$run_title, ")"),
                  table     = table_name,
                  rows      = nrow(object),
                  snapshot  = snapshot_new
               )

               # log if successful
               dbAppendTable(
                  lw_conn,
                  DBI::SQL(paste0('`', db_name, '`.`logs`')),
                  df
               )
               .log_success("Done.")
            } else {
               .log_info("No new/updated data found.")
            }

            # close connections
            dbDisconnect(db_conn)
            dbDisconnect(lw_conn)
         }
      },

      # get snapshot
      get_snapshots     = function(db_type = NULL, table_name = NULL) {
         snapshot    <- list()
         db_conn     <- .self$conn("lw")
         db_name     <- paste0("ohasis_", db_type)
         table_space <- Id(schema = db_name, table = table_name)

         # check current snapshot
         snapshot$old <- tbl(db_conn, dbplyr::in_schema(db_name, "logs")) %>%
            filter(table == table_name) %>%
            summarise(id = max(id, na.rm = TRUE)) %>%
            inner_join(
               y  = tbl(db_conn, dbplyr::in_schema(db_name, "logs")),
               by = "id"
            )

         # check if already available
         if ((snapshot$old %>% tally() %>% collect())$n > 0) {
            snapshot$old <- (snapshot$old %>% collect())$snapshot
            .log_info("Latest snapshot = {red(format(snapshot$old, \"%a %b %d, %Y %X\"))}.")
         } else {
            snapshot$old <- as.POSIXct("1970-01-01 00:00:00", tz = "UTC")
            .log_info("No version found in data lake.")
         }

         # check if already exists
         if (dbExistsTable(db_conn, table_space) &&
            "SNAPSHOT" %in% dbListFields(db_conn, table_space)) {
            sql           <- dbSendQuery(db_conn, paste0("SELECT MAX(SNAPSHOT) AS SNAPSHOT FROM `", db_name, "`.`", table_name, "`;"))
            snapshot$data <- dbFetch(sql)$SNAPSHOT %>% as.POSIXct(tz = "UTC")
            dbClearResult(sql)
         } else {
            snapshot$data <- as.POSIXct("1970-01-01 00:00:00", tz = "UTC")
         }

         snapshot$new <- as.POSIXct(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), tz = "UTC")

         dbDisconnect(db_conn)
         return(snapshot)
      },

      # consistency checks for live database
      check_consistency = function() {
         db_conn   <- .self$conn("db")
         checklist <- list()

         # get list of queries
         checks <- list.files(file.path(getwd(), "src", "diagnostics"))
         for (query in checks) {
            issue <- stri_replace_last_fixed(query, ".sql", "")

            if (stri_detect_regex(query, "^delete")) {
               dbExecute(db_conn, read_file(file.path(getwd(), "src", "diagnostics", query)))
            } else {
               rs <- dbSendQuery(db_conn, read_file(file.path(getwd(), "src", "diagnostics", query)))

               data <- dbFetch(rs)

               # if there are issues, break
               if (dbGetRowCount(rs) > 0)
                  checklist[[issue]] <- data

               # clear results
               dbClearResult(rs)
            }
         }
         dbDisconnect(db_conn)

         if (length(checklist) > 0) {
            .log_warn("DB inconsistencies found! See {underline(red('db_checks'))} for more info.")
            if ("duped_rec_id" %in% names(checklist)) {
               .log_error("Duplicated Record IDs found.")
            }
            if ("flipped_cid" %in% names(checklist))
               .log_error("Flipped Central IDs found in OHASIS registry.")
         } else {
            .log_info("DB is clean.")
         }
         return(checklist)
      },

      # speedtest
      speedtest         = function() {
         data <- shell(paste0(file.path(getwd(), getwd(), "src", "speedtest.exe"), " -f tsv --output-header -u Mbps"), intern = TRUE)
         data <- read.table(text = data, sep = "\t", header = TRUE) %>%
            mutate(
               speed_down_megabits_sec = download / 125000,
               speed_up_megabits_sec   = upload / 125000
            ) %>%
            select(
               server      = server.name,
               latency_ms  = latency,
               packet_loss = packet.loss,
               speed_down_megabits_sec,
               speed_up_megabits_sec
            )

         return(data)
      },

      # method to decode address data
      get_addr          = function(data, addr_set = NULL, type = "nhsss") {
         # addr_set format:
         # reg, prov, munc

         type <- tolower(type)
         if (type %in% c("nhsss", "code")) {
            get_reg  <- as.symbol("NHSSS_REG")
            get_prov <- as.symbol("NHSSS_PROV")
            get_munc <- as.symbol("NHSSS_MUNC")
         } else if (type == "label") {
            get_reg  <- as.symbol("LABEL_REG")
            get_prov <- as.symbol("LABEL_PROV")
            get_munc <- as.symbol("LABEL_MUNC")
         } else if (type == "name") {
            get_reg  <- as.symbol("NAME_REG")
            get_prov <- as.symbol("NAME_PROV")
            get_munc <- as.symbol("NAME_MUNC")
         }

         coded_reg  <- addr_set[1] %>% as.symbol()
         coded_prov <- addr_set[2] %>% as.symbol()
         coded_munc <- addr_set[3] %>% as.symbol()

         named_reg  <- names(addr_set)[1]
         named_prov <- names(addr_set)[2]
         named_munc <- names(addr_set)[3]

         # rename columns
         data %<>%
            select(
               -any_of(
                  c(
                     "PSGC_REG",
                     "PSGC_PROV",
                     "PSGC_MUNC"
                  )
               )
            ) %>%
            mutate_at(
               .vars = vars(!!coded_reg, !!coded_prov, !!coded_munc),
               ~if_else(is.na(.), "", .)
            ) %>%
            rename_all(
               ~case_when(
                  . == coded_reg ~ "PSGC_REG",
                  . == coded_prov ~ "PSGC_PROV",
                  . == coded_munc ~ "PSGC_MUNC",
                  TRUE ~ .
               )
            ) %>%
            left_join(
               y  = .self$ref_addr %>%
                  select(
                     PSGC_REG,
                     PSGC_PROV,
                     PSGC_MUNC,
                     !!named_reg  := !!get_reg,
                     !!named_prov := !!get_prov,
                     !!named_munc := !!get_munc,
                  ),
               by = c("PSGC_REG", "PSGC_PROV", "PSGC_MUNC")
            ) %>%
            relocate(!!named_reg, !!named_prov, !!named_munc, .before = PSGC_REG) %>%
            select(
               -PSGC_REG,
               -PSGC_PROV,
               -PSGC_MUNC
            )
      },

      # method to decode faci data
      get_faci          = function(linelist, input_set = NULL, return_type = "nhsss", addr_names = NULL) {
         # faci_set format:
         # name = c(faci_id, sub_faci_id)
         final_faci  <- names(input_set)
         faci_id     <- input_set[[1]][1]
         sub_faci_id <- input_set[[1]][2]

         get <- switch(
            return_type,
            nhsss = "FACI_NAME_CLEAN",
            code  = "FACI_CODE",
            name  = "FACI_NAME"
         )

         # check if sub_faci_id col exists
         if (!(sub_faci_id %in% names(linelist)))
            linelist[, sub_faci_id] <- NA_character_

         # check if addresses to  be extracted
         addr_cols <- NULL
         if (!is.null(addr_names)) {
            addr_cols <- c("FACI_PSGC_REG", "FACI_PSGC_PROV", "FACI_PSGC_MUNC")
         }

         # convert to names
         faci_id     <- as.name(faci_id)
         sub_faci_id <- as.name(sub_faci_id)

         # rename columns
         linelist %<>%
            mutate(
               { { faci_id } }     := if_else(
                  condition = is.na({ { faci_id } }),
                  true      = "",
                  false     = { { faci_id } },
                  missing   = { { faci_id } }
               ),
               { { sub_faci_id } } := case_when(
                  is.na({ { sub_faci_id } }) ~ "",
                  StrLeft({ { sub_faci_id } }, 6) != { { faci_id } } ~ "",
                  TRUE ~ { { sub_faci_id } }
               )
            ) %>%
            # get referenced data
            left_join(
               y  = .self$ref_faci %>%
                  select(
                     { { faci_id } }     := FACI_ID,
                     { { sub_faci_id } } := SUB_FACI_ID,
                     { { final_faci } }  := { { get } },
                     if (!is.null(addr_names)) {
                        any_of(addr_cols)
                     }
                  ),
               by = input_set[[1]]
            ) %>%
            # move then rename to old version
            relocate({ { final_faci } }, .after = { { sub_faci_id } }) %>%
            # remove id data
            select(-any_of(input_set[[1]]))

         # extract address
         if (!is.null(addr_names)) {
            names(addr_cols) <- addr_names
            linelist %<>%
               .self$get_addr(
                  addr_cols,
                  return_type
               ) %>%
               relocate(addr_names, .after = { { final_faci } })
         }

         return(linelist)
      },

      # method to decode user/staff data
      get_staff         = function(data, user_set = NULL) {
         # column
         coded_user <- user_set[1] %>% as.symbol()
         named_user <- names(user_set[1]) %>% as.symbol()

         data %<>%
            left_join(
               y  = .self$ref_staff %>%
                  select(
                     !!coded_user := STAFF_ID,
                     !!named_user := STAFF_NAME
                  ),
               by = as.character(coded_user)
            ) %>%
            # move then rename to old version
            relocate(!!named_user, .after = !!coded_user) %>%
            select(-as.character(coded_user))
      },

      # method to pull client records from OHASIS
      get_patient       = function(pid = NULL) {
         lw_conn <- .self$conn("lw")
         db_conn <- .self$conn("db")

         # get central id if available
         cidQuery <- "SELECT CENTRAL_ID FROM ohasis_interim.registry WHERE PATIENT_ID = ?"
         cidData  <- dbxSelect(db_conn, cidQuery, pid)$CENTRAL_ID

         # get list
         pidData <- pid
         if (length(cidData) > 0) {
            pidQuery <- "SELECT PATIENT_ID FROM ohasis_interim.registry WHERE CENTRAL_ID = ?"
            pidData  <- dbxSelect(db_conn, pidQuery, cidData)$PATIENT_ID
         }

         finalQuery <- "SELECT * FROM ohasis_lake.px_pii WHERE PATIENT_ID IN (?)"
         finalData  <- dbxSelect(lw_conn, finalQuery, list(pidData))

         finalData %<>%
            # convert addresses
            .self$get_addr(
               c(
                  PERM_REG  = "PERM_PSGC_REG",
                  PERM_PROV = "PERM_PSGC_PROV",
                  PERM_MUNC = "PERM_PSGC_MUNC"
               ),
               "name"
            ) %>%
            .self$get_addr(
               c(
                  CURR_REG  = "CURR_PSGC_REG",
                  CURR_PROV = "CURR_PSGC_PROV",
                  CURR_MUNC = "CURR_PSGC_MUNC"
               ),
               "name"
            ) %>%
            .self$get_addr(
               c(
                  BIRTH_REG  = "BIRTH_PSGC_REG",
                  BIRTH_PROV = "BIRTH_PSGC_PROV",
                  BIRTH_MUNC = "BIRTH_PSGC_MUNC"
               ),
               "name"
            ) %>%
            # get facility data
            .self$get_faci(
               list(RECORD_FACI = c("FACI_ID", "SUB_FACI_ID")),
               "name"
            ) %>%
            # get user data
            .self$get_staff(c(CREATED = "CREATED_BY")) %>%
            .self$get_staff(c(UPDATED = "UPDATED_BY")) %>%
            .self$get_staff(c(DELETED = "DELETED_BY")) %>%
            # remove unneeded columns
            select(
               -starts_with("DEATH_PSGC"),
               -starts_with("SERVICE_PSGC")
            ) %>%
            arrange(RECORD_DATE, MODULE)

         dbDisconnect(lw_conn)
         dbDisconnect(db_conn)

         return(finalData)
      },

      # method to load old dataset
      load_old_dta      = function(
         path = NULL,
         corr = NULL,
         warehouse_table = NULL,
         id_col = NULL,
         dta_pid = NULL,
         remove_cols = NULL,
         remove_rows = NULL,
         id_registry = NULL
      ) {
         # open connections
         .log_info("Opening connections.")
         db_conn <- .self$conn("lw")
         db_name <- "ohasis_warehouse"

         # references for old dataset
         old_tblspace  <- Id(schema = db_name, table = warehouse_table)
         old_tblschema <- dbplyr::in_schema(db_name, warehouse_table)
         oh_id_schema  <- dbplyr::in_schema(db_name, "id_registry")

         # check if dataset is to be re-loaded
         # TODO: add checking of latest version
         reload <- input(
            prompt  = "How do you want to load the previous dataset?",
            options = c("1" = "reprocess", "2" = "download"),
            default = "1"
         )

         if (!is.null(id_registry))
            tbl_ids <- id_registry
         else
            tbl_ids <- tbl(db_conn, oh_id_schema) %>%
               select(CENTRAL_ID, PATIENT_ID) %>%
               collect()

         # if Yes, re-process registry
         if (reload == "1") {
            .log_info("Re-processing the dataset from the previous reporting period.")

            old_dataset <- path %>%
               read_dta() %>%
               zap_labels() %>%
               # convert Stata string missing data to NAs
               mutate_if(
                  .predicate = is.character,
                  ~if_else(. == '', NA_character_, .)
               ) %>%
               select(-any_of(remove_cols)) %>%
               rename_all(
                  ~case_when(
                     . == dta_pid ~ "PATIENT_ID",
                     TRUE ~ .
                  )
               ) %>%
               left_join(
                  y  = tbl_ids,
                  by = "PATIENT_ID"
               ) %>%
               mutate(
                  CENTRAL_ID = if_else(
                     condition = is.na(CENTRAL_ID),
                     true      = PATIENT_ID,
                     false     = CENTRAL_ID
                  )
               ) %>%
               relocate(CENTRAL_ID, .before = 1)

            if (!is.null(corr)) {
               .log_info("Performing cleaning on the dataset.")
               old_dataset <- .cleaning_list(old_dataset, as.data.frame(corr), toupper(names(id_col)), id_col)
            }

            # drop clients
            if (!is.null(remove_rows)) {
               old_dataset <- old_dataset %>%
                  anti_join(
                     y  = remove_rows,
                     by = names(id_col)
                  )
            }

            .log_info("Updating warehouse table.")
            # delete existing data, full refresh always
            if (dbExistsTable(db_conn, old_tblspace))
               dbExecute(db_conn, glue(r"(DROP TABLE `ohasis_warehouse`.`{warehouse_table}`;)"))

            # upload info
            # .self$upsert(db_conn, "warehouse", warehouse_table, old_dataset, "PATIENT_ID")
            .self$upsert(db_conn, "warehouse", warehouse_table, old_dataset, names(id_col))
         }

         if (reload == "2") {
            .log_info("Downloading the dataset from the previous reporting period.")
            old_dataset <- dbTable(
               db_conn,
               db_name,
               warehouse_table,
               join = list(
                  "ohasis_warehouse.id_registry" = list(by = c("PATIENT_ID" = "PATIENT_ID"), cols = "CENTRAL_ID")
               )
            ) %>%
               mutate(
                  CENTRAL_ID = if_else(
                     condition = is.na(CENTRAL_ID),
                     true      = PATIENT_ID,
                     false     = CENTRAL_ID
                  )
               ) %>%
               relocate(CENTRAL_ID, .before = 1)
         }

         .log_info("Closing connections.")
         dbDisconnect(db_conn)

         .log_success("Done.")
         return(old_dataset)
      }
   )
)