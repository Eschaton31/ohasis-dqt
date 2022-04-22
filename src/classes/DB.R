##  DBMS Class -----------------------------------------------------------------

# Database accessor
DB <- setRefClass(
   Class    = "DB",
   contains = "Project",
   fields   = list(
      timestamp    = "character",
      output_title = "character",
      internet     = "data.frame",
      db_checks    = "list",
      ref_addr     = "data.frame",
      ref_country  = "data.frame",
      ref_faci     = "data.frame",
      ref_staff    = "data.frame"
   ),
   methods  = list(
      initialize        = function(mo = NULL, yr = NULL, title = NULL) {
         "This method is called when you create an instance of this class."

         # get current time
         timestamp    <<- format(Sys.time(), "%Y.%m.%d.%H%M%S")
         output_title <<- paste0(timestamp, "-", Sys.getenv("LW_USER"))

         # set the report
         callSuper()$set_report()

         # check internet speed
         check_speed <- input(
            prompt  = "Run the speed test?",
            options = c("1" = "yes", "2" = "no"),
            default = "1"
         )
         check_speed <- substr(toupper(check_speed), 1, 1)
         if (check_speed == "1") {
            .log_info("Checking internet speed.")
            internet <<- .self$speedtest()
            .log_info("Current download speed: {underline(red(internet$speed_down_megabits_sec))} Mbps")
            .log_info("Current upload speed: {underline(red(internet$speed_up_megabits_sec))} Mbps")
         }

         # check database consistency
         .log_info("Checking database for inconsistencies.")
         db_checks <<- .self$check_consistency()
         if (length(db_checks) > 0) {
            .log_warn("DB inconsistencies found! See {underline(red('db_checks'))} for more info.")
            if ("duped_rec_id" %in% names(db_checks)) {
               .log_error("Duplicated Record IDs found.")
            }
            if ("flipped_cid" %in% names(db_checks))
               .log_error("Flipped Central IDs found in OHASIS registry.")
         } else {
            .log_info("DB is clean.")
         }

         # update data lake
         update <- input(
            prompt  = "Update the data lake?",
            options = c("1" = "yes", "2" = "no", "3" = "all"),
            default = "1"
         )
         update <- substr(toupper(update), 1, 1)
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
               lapply(list.files(lake_dir, pattern = "ref_*"), lake_table)
               lapply(list.files(lake_dir, pattern = "px_*"), lake_table)
               lapply(list.files(lake_dir, pattern = "lab_*"), lake_table)
               lapply(list.files(lake_dir, pattern = "disp_*"), lake_table)
            }

            .log_info("Data lake updated!")
         }

         # download latest references before final initialization
         .log_info("Downloading references.")
         db_conn           <- .self$conn("db")
         lw_conn           <- .self$conn("lw")
         .self$ref_addr    <<- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "ref_addr")) %>% collect()
         .self$ref_country <<- tbl(db_conn, dbplyr::in_schema("ohasis_interim", "addr_country")) %>% collect()
         .self$ref_faci    <<- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "ref_faci")) %>% collect()
         .self$ref_staff   <<- tbl(lw_conn, dbplyr::in_schema("ohasis_lake", "ref_staff")) %>% collect()
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

      # upsert data
      upsert            = function(db_conn = NULL, db_type = NULL, table_name = NULL, data = NULL, id_col = NULL) {
         db_name     <- paste0("ohasis_", db_type)
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
         text_faci <- c("PREV_TEST_FACI", "DELIVER_FACI", "FACI_LABEL", "FACI_NAME", "FACI_NAME_CLEAN")

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
                  Var1 == "FACI_CODE" ~ "CHAR(3) NULL COLLATE 'utf8_general_ci'",
                  Var1 == "REC_ID" ~ "CHAR(25) NULL COLLATE 'utf8_general_ci'",
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
         if (!("CENTRAL_ID" %in% id_col))
            index <- ", INDEX `CENTRAL_ID` (`CENTRAL_ID`)"
         if (!("PATIENT_ID" %in% id_col))
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
         db_name     <- paste0("ohasis_", db_type)
         table_space <- Id(schema = db_name, table = table_name)

         # get input
         if (default_yes == TRUE) {
            update <- "1"
            .log_info("Updating {red(table_name)} @ the {red(db_type)}.")
         } else {
            update <- input(
               prompt  = paste0("Update ", red(table_name), "?"),
               options = c("1" = "yes", "2" = "no"),
               default = "1"
            )
         }
         update <- substr(toupper(update), 1, 1)

         # update
         if (update == "1") {
            # open connections
            .log_info("Opening connections.")
            db_conn <- .self$conn("db")
            lw_conn <- .self$conn("lw")

            # data for deletion (warehouse)
            for_delete <- data.frame()

            # get snapshots
            snapshot      <- .self$get_snapshots(db_type, table_name)
            snapshot$data <- if_else(!is.na(snapshot$data), snapshot$data + 1, snapshot$data)
            snapshot_old  <- if_else(snapshot$old < snapshot$data, snapshot$old, snapshot$data) %>% as.character()
            snapshot_new  <- snapshot$new %>% as.character()

            # if specified days to update
            if (!is.null(from))
               snapshot_old <- from

            # if specified days to update
            if (!is.null(to))
               snapshot_new <- to

            if (update_type == "refresh")
               snapshot_old <- "1970-01-01 00:00:00"

            # run data lake script for object
            .log_info("Getting new/updated data.")
            factory_file <- file.path(getwd(), "src", paste0("data_", db_type), "refresh", paste0(table_name, '.R'))
            if (!file.exists(factory_file))
               factory_file <- file.path(getwd(), "src", paste0("data_", db_type), "upsert", paste0(table_name, '.R'))

            source(factory_file, local = TRUE)

            # keep connection alive
            dbDisconnect(lw_conn)
            lw_conn <- .self$conn("lw")

            # check if there is data for deletion
            if (nrow(for_delete) > 0 && dbExistsTable(lw_conn, table_space)) {
               .log_info("Number of invalidated records = {red(formatC(nrow(for_delete), big.mark = ','))}.")
               dbxDelete(
                  lw_conn,
                  table_space,
                  for_delete,
                  batch_size = 1000
               )
               .log_success("Invalidated records removed.")
            }

            if (continue > 0) {
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
      }
   )
)