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

         cat("[", format(Sys.time(), "%a %b %d, %Y %X"), "] Project parameters defined!\n")
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
      timestamp = "character"
   ),
   methods  = list(
      initialize    = function(mo = NULL, yr = NULL, title = NULL) {
         "This method is called when you create an instance of this class."

         # get current time
         .self$timestamp <<- format(Sys.time(), "%Y.%m.%d.%H%M%S")

         # set the report
         callSuper()$set_report()

         # database connection
         update <- export("Project")$input(prompt = "Would you like to update the data lake?", c("yes", "no"), "yes")
         update <- substr(toupper(update), 1, 1)
         if (update == "Y") {
            # get required refresh data & upsert only
            for (path in (c("src/data_lake/refresh", "src/data_lake/upsert"))) {
               if (path == "src/data_lake/refresh")
                  reset <- TRUE
               else
                  reset <- FALSE

               lapply(list.files(path, pattern = "ref_*"), function(table) {
                  table <- stri_replace_last_fixed(table, ".R", "")
                  .self$lake(table, reset = reset, path = path)
               })
               lapply(list.files(path, pattern = "px_*"), function(table) {
                  table <- stri_replace_last_fixed(table, ".R", "")
                  .self$lake(table, reset = reset, path = path)
               })
            }


            cat("[", format(Sys.time(), "%a %b %d, %Y %X"), "] Closing connections... ")
            cat("Data lake updated!\n")
         }

         cat("[", format(Sys.time(), "%a %b %d, %Y %X"), "] OHASIS initialized!\n")
      },

      # refreshes db connection
      conn          = function(db = NULL) {
         # live database
         if (db == "db_live")
            db_conn <- dbConnect(
               RMariaDB::MariaDB(),
               user     = Sys.getenv("DB_USER"),
               password = Sys.getenv("DB_PASS"),
               host     = Sys.getenv("DB_HOST"),
               port     = Sys.getenv("DB_PORT"),
               timeout  = -1,
               Sys.getenv("DB_NAME")
            )

         # live data late
         if (db == "dl_live")
            db_conn <- dbConnect(
               RMariaDB::MariaDB(),
               user     = Sys.getenv("DL_USER"),
               password = Sys.getenv("DL_PASS"),
               host     = Sys.getenv("DL_HOST"),
               port     = Sys.getenv("DL_PORT"),
               timeout  = -1,
               Sys.getenv("DL_NAME")
            )

         return(db_conn)
      },

      # upsert data
      upsert        = function(db_conn = NULL, table = NULL, data = NULL, cols = NULL) {
         # check if table exists, if not create
         if (!dbExistsTable(db_conn, table))
            dbCreateTable(db_conn, table, data)

         # compare columns match, if not re-create table
         if (!identical(sort(names(data)), sort(dbListFields(db_conn, table)))) {
            dbRemoveTable(db_conn, table)
            dbCreateTable(db_conn, table, data)
         }

         # upsert data
         dbxUpsert(db_conn, table, data, cols, batch_size = 1000)
      },

      # update lake
      lake          = function(table = NULL, reset = FALSE, path = NULL) {
         # get input
         update <-
            export("Project")$input(prompt = paste0("Update `", table, "`?"),
                                    c("yes", "no"),
                                    "yes")
         update <- substr(toupper(update), 1, 1)

         # update
         if (update == "Y") {
            # activate connections
            db_conn <- .self$conn("db_live")
            dl_conn <- .self$conn("dl_live")

            # get snapshots
            snapshot     <- .self$get_snapshots(table)
            snapshot_old <- snapshot$old
            snapshot_new <- snapshot$new

            if (reset)
               snapshot_old <- as.POSIXct("1970-01-01 00:00:00")

            # run data lake script for object
            cat("[", format(Sys.time(), "%a %b %d, %Y %X"), "] Getting new data...\n")
            source(file.path(path, paste0(table, '.R')), local = TRUE)

            if (nrow(object) > 0 ||
               !dbExistsTable(dl_conn, table) ||
               !identical(sort(names(object)), sort(dbListFields(dl_conn, table)))) {
               cat("[", format(Sys.time(), "%a %b %d, %Y %X"), "] Updating data lake...\n")
               .self$upsert(dl_conn, table, object, id_col)
               # update reference
               df <- data.frame(user      = Sys.getenv("DL_USER"),
                                report_yr = .self$yr,
                                report_mo = .self$mo,
                                run_title = paste0(.self$timestamp, " (", .self$run_title, ")"),
                                table     = table,
                                rows      = nrow(object),
                                snapshot  = snapshot_new %>% as.character())

               dbWriteTable(
                  dl_conn,
                  "logs",
                  df,
                  append = TRUE
               )
               cat("[", format(Sys.time(), "%a %b %d, %Y %X"), "] Done!\n")
            } else {
               cat("[", format(Sys.time(), "%a %b %d, %Y %X"), "] None found.\n")
            }

            dbDisconnect(db_conn)
            dbDisconnect(dl_conn)
         }
      },

      # get snapshot
      get_snapshots = function(ref_table = NULL) {
         dl_conn  <- .self$conn("dl_live")
         snapshot <- list()

         # check current snapshot
         snapshot$old <- tbl(dl_conn, "logs") %>%
            filter(table == ref_table) %>%
            summarise(id = max(id, na.rm = TRUE)) %>%
            inner_join(
               y  = tbl(dl_conn, "logs"),
               by = "id"
            )

         # check if already available
         if ((snapshot$old %>% tally() %>% collect())$n > 0) {
            snapshot$old <- (snapshot$old %>% collect())$snapshot
            cat("[", format(Sys.time(), "%a %b %d, %Y %X"), "] Current data lake snapshot as of `", format(snapshot$old, "%a %b %d, %Y %X"), "`\n")
         } else {
            snapshot$old <- as.POSIXct("1970-01-01 00:00:00")
            cat("[", format(Sys.time(), "%a %b %d, %Y %X"), "] No version found in data lake,\n")
         }

         snapshot$new <- as.POSIXct(format(Sys.time(), "%Y-%m-%d %H:%M:%S"))

         dbDisconnect(dl_conn)
         return(snapshot)
      }
   )
)