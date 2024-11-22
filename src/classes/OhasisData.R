 OhasisData <- R6Class(
   "OhasisData",
   public  = list(
      periods      = list(
         curr  = list(
            yr  = NA_integer_,
            mo  = NA_integer_,
            ym  = NA_character_,
            min = NA_Date_,
            max = NA_Date_
         ),
         prev  = list(
            yr  = NA_integer_,
            mo  = NA_integer_,
            ym  = NA_character_,
            min = NA_Date_,
            max = NA_Date_
         ),
         after = list(
            yr  = NA_integer_,
            mo  = NA_integer_,
            ym  = NA_character_,
            min = NA_Date_,
            max = NA_Date_
         )
      ),

      initialize   = function(end_date) {
         if (missing(end_date)) {
            end_date <- Sys.Date()
         }

         if (is.character(end_date)) {
            end_date <- as.Date(end_date)
         }

         self$periods$curr  <- private$calculatePeriod(end_date)
         self$periods$prev  <- private$calculatePeriod(end_date %m-% months(1))
         self$periods$after <- private$calculatePeriod(end_date %m+% months(1))
      },

      updateForms  = function(from = NULL, to = NULL) {
         lapply(private$tables$lake, ohasis$data_factory, db_type = "lake", update_type = "upsert", default_yes = TRUE, from = from, to = to)
         lapply(private$tables$warehouse, ohasis$data_factory, db_type = "warehouse", update_type = "upsert", default_yes = TRUE, from = from, to = to)

         invisible(self)
      },

      tableFromSql = function(table, path) {
         log_info("Processing {green(table)}.")

         lw_conn     <- connect("ohasis-lw")
         db_name     <- "ohasis_warehouse"
         table_space <- Id(schema = db_name, table = table)

         # update lake
         if (dbExistsTable(lw_conn, table_space)) dbRemoveTable(lw_conn, table_space)

         create <- stri_c("CREATE TABLE ", db_name, ".", table, " AS ")
         select <- read_file(file.path(path, stri_c(table, ".sql")))

         dbExecute(
            lw_conn,
            stri_c(create, select)
         )

         log_success("Done!")
         dbDisconnect(lw_conn)

         invisible(self)
      }
   ),
   private = list(
      calculatePeriod      = function(end_date) {
         yr  <- year(end_date)
         mo  <- month(end_date)
         ym  <- format(end_date, "%Y.%m")
         min <- as.character(floor_date(end_date, "months"))
         max <- as.character(end_date)

         return(list(yr = yr, mo = mo, ym = ym, min = min, max = max))
      },
      fetchPreviousDataset = function(surv, type, period, id, corr, drop) {
         table <- stri_c(surv, ".", type, "_", period)

         conn <- connect("ohasis-lw")
         data <- QB$new(conn)$from(table)$get()
         dbDisconnect(conn)

         data %<>%
            apply_corrections(corr, id)

         if (!missing(drop)) {
            data %<>%
               anti_join(
                  y  = drop,
                  by = id
               )
         }

         return(data)
      },
      tables               = list(
         lake      = list(),
         warehouse = list()
      )
   )
)
