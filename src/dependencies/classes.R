##------------------------------------------------------------------------------
##  Classes
##------------------------------------------------------------------------------

# main project class
Project <- setRefClass(
   Class   = "Project",
   fields  = list(
      mo        = "character",
      yr        = "character",
      run_title = "character"
   ),
   methods = list(
      # set the current reporting period
      set_report = function() {
         # reporting date
         mo <<- .self$input(prompt = "What is the reporting month?", max.char = 2)
         mo <<- mo %>% stri_pad_left(width = 2, pad = "0")
         yr <<- .self$input(prompt = "What is the reporting year?", max.char = 4)
         yr <<- yr %>% stri_pad_left(width = 4, pad = "0")

         # label the current run
         run_title <<- .self$input(prompt = "Label the current run (brief, concise)")

         log_success("Project parameters defined!")
      },

      # taking user input
      input      = function(prompt, options = NULL, default = NULL, max.char = NULL) {
         if (!is.null(options) && !is.null(default)) {
            options <- paste(collapse = "/", options)
            options <- stri_replace_first_fixed(options, default, stri_trans_totitle(default))
            prompt  <- paste0(prompt, " [", options, "]")
         } else if (!is.null(default)) {
            prompt <- paste0(prompt, " [", default, "]")
         }

         # get user input
         data <- readline(paste0(prompt, ": "))

         # if empty, use default
         if (data == "" & !is.null(default))
            data <- default

         # if no default, throw error
         if (data == "" & is.null(default))
            stop("This is a required input!")

         # check if max characters defined
         if (!is.null(max.char) && nchar(data) > max.char)
            stop("Input exceeds the maximum number of characters!")


         # return value
         return(data)
      }
   )
)

# data_warehouse class
DB <- setRefClass(
   Class    = "DB",
   contains = "Project",
   fields   = list(
      timestamp = "character",
      internet  = "data.frame",
      db_checks = "list"
   ),
   methods  = list(
      initialize        = function(mo = NULL, yr = NULL, title = NULL) {
         "This method is called when you create an instance of this class."

         # get current time
         .self$timestamp <<- format(Sys.time(), "%Y.%m.%d.%H%M%S")

         # set the report
         callSuper()$set_report()

         # check internet speed
         check_speed <- export("Project")$input(prompt = "Run the speed test?", c("yes", "no"), "yes")
         check_speed <- substr(toupper(check_speed), 1, 1)
         if (check_speed == "Y") {
            log_info("Checking internet speed...")
            internet <<- .self$speedtest()
            log_info("Current download speed: {internet$speed_down_megabits_sec} Mbps")
            log_info("Current upload speed: {internet$speed_up_megabits_sec} Mbps")
         }

         # check database consistency
         log_info("Checking database for inconsistencies...")
         .self$check_consistency()
         if (length(.self$db_checks) > 0)
            log_warn("DB inconsistencies found! See `db_checks` for more info.")
         else
            log_info("DB is clean.")

         # update data lake
         update <- export("Project")$input(prompt = "Update the data lake?", c("all", "yes", "no"), "yes")
         update <- substr(toupper(update), 1, 1)
         if (update %in% c("Y", "A")) {
            # get required refresh data & upsert only
            for (path in (c("refresh", "upsert"))) {
               if (path == "refresh")
                  refresh <- TRUE
               else
                  refresh <- FALSE

               lake_dir <- file.path("src", "data_lake", path)

               # if all should be updated
               if (update == "A")
                  update_all <- TRUE
               else
                  update_all <- FALSE

               lake_table <- function(table) {
                  table <- stri_replace_last_fixed(table, ".R", "")
                  .self$lake(table, refresh = refresh, path = lake_dir, default = update_all)
               }

               lapply(list.files(lake_dir, pattern = "ref_*"), lake_table)
               lapply(list.files(lake_dir, pattern = "px_*"), lake_table)
            }

            log_info("Data lake updated!")
         }

         log_success("OHASIS initialized!")
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
               timeout  = -1,
               Sys.getenv("DB_NAME")
            )

         # live data lake
         if (db == "dl")
            db_conn <- dbConnect(
               RMariaDB::MariaDB(),
               user     = Sys.getenv("DL_USER"),
               password = Sys.getenv("DL_PASS"),
               host     = Sys.getenv("DL_HOST"),
               port     = Sys.getenv("DL_PORT"),
               timeout  = -1,
               Sys.getenv("DL_NAME")
            )

         # live data warehouse
         if (db == "dw")
            db_conn <- dbConnect(
               RMariaDB::MariaDB(),
               user     = Sys.getenv("DW_USER"),
               password = Sys.getenv("DW_PASS"),
               host     = Sys.getenv("DW_HOST"),
               port     = Sys.getenv("DW_PORT"),
               timeout  = -1,
               Sys.getenv("DW_NAME")
            )

         return(db_conn)
      },

      # upsert data
      upsert            = function(db_conn = NULL, table = NULL, data = NULL, id_col = NULL) {
         # check if table exists, if not create
         if (!dbExistsTable(db_conn, table)) {
            sql <- .self$create(table, data, id_col)
            dbExecute(db_conn, sql)
         }

         # compare columns match, if not re-create table
         if (!identical(sort(names(data)), sort(dbListFields(db_conn, table)))) {
            # get current records (assuming change is only added columns)
            df <- dbReadTable(db_conn, table)

            # recreate table using new columns
            dbRemoveTable(db_conn, table)
            dbCreateTable(db_conn, table, data)

            # re-insert data
            dbAppendTable(db_conn, table, df)
         }

         # upsert data
         chunk_size <- 100
         if (nrow(data) >= chunk_size) {
            # upload in chunks to monitor progress
            n_rows     <- nrow(data)
            n_chunks   <- rep(1:ceiling(n_rows / chunk_size), each = chunk_size)[1:n_rows]
            data       <- split(data, n_chunks)
            data_bytes <- as.numeric(object.size(data))

            # get progress
            pb <- progress_bar$new(format = "Uploaded :bytes chunks @ :rate [:bar] (:percent) ETA: :eta; Elapsed: :elapsed", total = data_bytes, width = 80, clear = FALSE)
            pb$tick(0)
            for (i in seq_len(length(data))) {
               chunk_bytes <- as.numeric(object.size(data[[i]]))
               dbxUpsert(db_conn, table, data[[i]], id_col)
               pb$tick(chunk_bytes)
            }
            cat("\n")
         } else {
            dbxUpsert(db_conn, table, data, id_col)
         }
      },

      # create table
      create            = function(table = NULL, data = NULL, id_col = NULL) {
         # attached columns
         user_cols <- c("CREATED_BY", "UPDATED_BY", "DELETED_BY", "PROVIDER_ID", "SIGNATORY_1", "SIGNATORY_2", "SIGNATORY_2", "USER_ID", "STAFF_ID")
         text_faci <- c("PREV_TEST_FACI", "DELIVER_FACI")

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
                  stri_detect_fixed(Var1, "PSGC") ~ "CHAR(9) NULL COLLATE 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "SUB_FACI") ~ "CHAR(10) NULL COLLATE 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "SUB_SOURCE") ~ "CHAR(10) NULL COLLATE 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "FACI") ~ "CHAR(6) NULL COLLATE 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "SOURCE") ~ "CHAR(6) NULL COLLATE 'utf8_general_ci'",
                  Var1 == "REC_ID" ~ "CHAR(25) NULL COLLATE 'utf8_general_ci'",
                  Var1 == "PATIENT_ID" ~ "CHAR(18) NULL DEFAULT NULL COLLATE 'utf8_general_ci'",
                  Mode == "numeric" & Class == "Date" ~ "DATE NULL DEFAULT NULL",
                  Mode == "numeric" & Class == "POSIXct" ~ "DATETIME NULL DEFAULT NULL",
                  Mode == "numeric" ~ "INT(11) NULL DEFAULT NULL",
                  Mode == "character" ~ "TEXT NULL DEFAULT NULL COLLATE 'utf8_general_ci'",
                  TRUE ~ NA_character_
               ),
               SQL  = paste0("`", Var1, "` ", Type),
            )

         # implode into query
         pk_sql     <- paste(collapse = "`,`", id_col)
         create_sql <- paste(collapse = ",", df_str$SQL)
         create_sql <- paste0("CREATE TABLE `", table, "` (\n",
                              create_sql, ",\nPRIMARY KEY(`", pk_sql, "`)\n)\n",
                              "COLLATE='utf8_general_ci'\nENGINE=InnoDB;")
         return(create_sql)
      },

      # update lake
      lake              = function(table = NULL, refresh = FALSE, path = NULL, default = NULL) {
         # get input
         if (!is.null(default)) {
            update <- "Y"
            log_info(paste0("Updating `", table, "`."))
         } else {
            update <- export("Project")$input(prompt = paste0("Update `", table, "`?"),
                                              c("yes", "no"),
                                              "yes")
         }
         update <- substr(toupper(update), 1, 1)

         # update
         if (update == "Y") {
            # open connections
            log_info("Opening connections...")
            db_conn <- .self$conn("db")
            dl_conn <- .self$conn("dl")

            # get snapshots
            snapshot     <- .self$get_snapshots(dl_conn, table)
            snapshot_old <- if_else(snapshot$old < snapshot$data, snapshot$old, snapshot$data)
            snapshot_new <- snapshot$new

            if (refresh)
               snapshot_old <- as.POSIXct("1970-01-01 00:00:00")

            # run data lake script for object
            log_info("Getting new data...")
            source(file.path(path, paste0(table, '.R')), local = TRUE)

            if (nrow(object) > 0 ||
               !dbExistsTable(dl_conn, table) ||
               !identical(sort(names(object)), sort(dbListFields(dl_conn, table)))) {
               log_info("Updating data lake...")
               .self$upsert(dl_conn, table, object, id_col)
               # update reference
               df <- data.frame(
                  user      = Sys.getenv("DL_USER"),
                  report_yr = .self$yr,
                  report_mo = .self$mo,
                  run_title = paste0(.self$timestamp, " (", .self$run_title, ")"),
                  table     = table,
                  rows      = nrow(object),
                  snapshot  = snapshot_new %>% as.character()
               )

               # log if successful
               dbAppendTable(
                  dl_conn,
                  "logs",
                  df
               )
               log_info("Done!")
            } else {
               log_info("None found.")
            }

            # close connections
            dbDisconnect(db_conn)
            dbDisconnect(dl_conn)
         }
      },


      # update warehouse
      warehouse         = function(table = NULL, refresh = FALSE, path = NULL) {
         # get input
         update <- export("Project")$input(prompt = paste0("Update `", table, "`?"),
                                           c("yes", "no"),
                                           "yes")
         update <- substr(toupper(update), 1, 1)

         # update
         if (update == "Y") {
            # open connections
            log_info("Opening connections...")
            db_conn <- .self$conn("db")
            dl_conn <- .self$conn("dl")
            dw_conn <- .self$conn("dw")

            # get snapshots
            snapshot     <- .self$get_snapshots(dw_conn, table)
            snapshot_old <- if_else(snapshot$old < snapshot$data, snapshot$old, snapshot$data, snapshot$old)
            snapshot_new <- snapshot$new

            if (refresh)
               snapshot_old <- as.POSIXct("1970-01-01 00:00:00")

            # run data lake script for object
            log_info("Getting new data...")
            source(file.path(path, paste0(table, '.R')), local = TRUE)

            if (nrow(object) > 0 ||
               !dbExistsTable(dw_conn, table) ||
               !identical(sort(names(object)), sort(dbListFields(dw_conn, table)))) {
               log_info("Updating data lake...")
               .self$upsert(dw_conn, table, object, id_col)
               # update reference
               df <- data.frame(
                  user      = Sys.getenv("DL_USER"),
                  report_yr = .self$yr,
                  report_mo = .self$mo,
                  run_title = paste0(.self$timestamp, " (", .self$run_title, ")"),
                  table     = table,
                  rows      = nrow(object),
                  snapshot  = snapshot_new %>% as.character()
               )

               # log if successful
               dbAppendTable(
                  dw_conn,
                  "logs",
                  df
               )
               log_info("Done!")
            } else {
               log_info("None found.")
            }

            # close connections
            dbDisconnect(db_conn)
            dbDisconnect(dl_conn)
            dbDisconnect(dw_conn)
         }
      },

      # get snapshot
      get_snapshots     = function(db_conn = NULL, ref_table = NULL) {
         snapshot <- list()

         # check current snapshot
         snapshot$old <- tbl(db_conn, "logs") %>%
            filter(table == ref_table) %>%
            summarise(id = max(id, na.rm = TRUE)) %>%
            inner_join(
               y  = tbl(db_conn, "logs"),
               by = "id"
            )

         # check if already available
         if ((snapshot$old %>% tally() %>% collect())$n > 0) {
            snapshot$old <- (snapshot$old %>% collect())$snapshot
            log_info("Current data lake snapshot as of `{format(snapshot$old, \"%a %b %d, %Y %X\")}`.")
         } else {
            snapshot$old <- as.POSIXct("1970-01-01 00:00:00")
            log_info("No version found in data lake.")
         }

         # check if already exists
         if (dbExistsTable(db_conn, ref_table) && "SNAPSHOT" %in% dbListFields(db_conn, ref_table)) {
            sql           <- dbSendQuery(db_conn, paste0("SELECT MAX(SNAPSHOT) AS SNAPSHOT FROM `", ref_table, "`;"))
            snapshot$data <- dbFetch(sql)$SNAPSHOT %>% as.POSIXct()
            dbClearResult(sql)
         } else {
            snapshot$data <- as.POSIXct("1970-01-01 00:00:00")
         }

         snapshot$new <- as.POSIXct(format(Sys.time(), "%Y-%m-%d %H:%M:%S"))

         return(snapshot)
      },

      # consistency checks for live database
      check_consistency = function() {
         db_conn <- .self$conn("db")

         # get list of queries
         checks <- list.files("src/diagnostics") %>% sort(decreasing = TRUE)
         for (query in checks) {
            issue <- stri_replace_last_fixed(query, ".sql", "")
            rs    <- dbSendQuery(db_conn, read_file(file.path("src/diagnostics", query)))

            data <- dbFetch(rs)

            # if there are issues, break
            if (dbGetRowCount(rs) > 0)
               .self$db_checks[[issue]] <<- data

            # clear results
            dbClearResult(rs)
         }
         dbDisconnect(db_conn)
      },

      # speedtest
      speedtest         = function() {
         data <- shell(paste0(file.path(getwd(), "src/speedtest.exe"), " -f tsv --output-header -u Mbps"), intern = TRUE)
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