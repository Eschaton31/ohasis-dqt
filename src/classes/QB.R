QB <- R6Class(
   "QB",
   public  = list(
      columns      = "*",
      wheres       = list(),
      joins        = list(),
      title        = NULL,

      initialize   = function(db_conn = NULL) {
         private$conn <- db_conn
      },

      table        = function(table, as = NULL, database = NULL) {
         as       <- private$quoteIdentifier(as)
         database <- private$quoteIdentifier(database)
         table    <- private$quoteIdentifier(table)

         actual <- str_flatten(collapse = ".", na.rm = TRUE, c(database, table))
         alias  <- str_flatten(collapse = " AS ", na.rm = TRUE, c(actual, as))

         return(alias)
      },

      from         = function(table, as = NULL, database = NULL) {
         private$main <- self$table(table, as = as, database = database)
         self$title   <- private$getAlias(private$main)

         invisible(self)
      },

      select       = function(...) {
         columns      <- match.call(expand.dots = FALSE)$`...`
         columns      <- as.character(columns)
         self$columns <- columns

         invisible(self)
      },

      where        = function(column, operator = NULL, value = NULL) {
         if (class(column) != "function") {
            column <- stri_c(sep = " ", column, operator, dbQuoteString(private$conn, value))
         } else {
            column <- do.call(column, list())
         }

         if (length(self$wheres) > 0) {
            column <- stri_c("AND ", column)
         }

         self$wheres <- append(self$wheres, column)

         invisible(self)
      },

      orWhere      = function(column, operator = NULL, value = NULL) {
         column <- stri_c(sep = " ", column, operator, dbQuoteString(private$conn, value))

         if (length(self$wheres) > 0) {
            column <- stri_c("OR ", column)
         }

         self$wheres <- append(self$wheres, column)

         invisible(self)
      },

      whereIn      = function(column, value) {
         self$wheres <- append(self$wheres, stri_c(column, " IN ('", str_flatten(collapse = "','", value), "')"))

         invisible(self)
      },

      whereNotIn   = function(column, value) {
         self$wheres <- append(self$wheres, stri_c(column, " NOT IN ('", str_flatten(collapse = "','", value), "')"))

         invisible(self)
      },

      whereNull    = function(column) {
         self$wheres <- append(self$wheres, stri_c(column, " IS NULL"))

         invisible(self)
      },

      whereNotNull = function(column) {
         self$wheres <- append(self$wheres, stri_c(column, " IS NOT NULL"))

         invisible(self)
      },

      leftJoin     = function(right, col_left, operator, col_right) {
         join       <- private$joinQuery(right, col_left, operator, col_right)
         join       <- stri_c(sep = " ", "LEFT", join)
         self$joins <- append(self$joins, join)

         invisible(self)
      },

      rightJoin    = function(right, col_left, operator, col_right) {
         join       <- private$joinQuery(right, col_left, operator, col_right)
         join       <- stri_c(sep = " ", "RIGHT", join)
         self$joins <- append(self$joins, join)

         invisible(self)
      },

      join         = function(right, col_left, operator, col_right) {
         self$joins <- append(self$joins, private$joinQuery(right, col_left, operator, col_right))

         invisible(self)
      },

      get          = function() {
         query <- self$query

         n_rows <- dbGetQuery(conn, query$nrow)
         n_rows <- sum(as.numeric(n_rows$nrow), na.rm = TRUE)

         results <- list()

         # get actual result set
         rs         <- dbSendQuery(conn, query$results)
         chunk_size <- 1000
         if (n_rows >= chunk_size) {
            # upload in chunks to monitor progress
            n_chunks <- ceiling(n_rows / chunk_size)

            pb_name <- stri_c(self$title, ": :current of :total chunks [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed")
            pb      <- progress_bar$new(format = pb_name, total = n_chunks, width = 100, clear = FALSE)
            pb$tick(0)

            # fetch in chunks
            for (i in seq_len(n_chunks)) {
               chunk   <- dbFetch(rs, chunk_size)
               results <- bind_rows(results, chunk)
               pb$tick(1)
            }
         } else {
            results <- dbFetch(rs)
         }
         dbClearResult(rs)

         return(results)
      }
   ),
   private = list(
      main            = NULL,
      conn            = NULL,

      quoteIdentifier = function(identifier) {
         if (is.null(identifier)) {
            return()
         }

         return(dbQuoteIdentifier(private$conn, identifier))
      },

      getAlias        = function(identifier) {
         return(ifelse(str_detect(identifier, " AS "), str_extract(identifier, " AS (.*)", 1), identifier))
      },

      joinOn          = function(right, col_left, operator, col_right) {
         join_on   <- ""
         col_left  <- private$quoteIdentifier(col_left)
         col_right <- private$quoteIdentifier(col_right)

         if (col_left == col_right & operator == "=") {
            join_on <- stri_c("USING (", col_left, ")")
         } else {
            col_left  <- stri_c(private$getAlias(private$main), ".", col_left)
            col_right <- stri_c(private$getAlias(right), ".", col_right)
            join_on   <- stri_c(sep = " ", "ON", col_left, operator, col_right)
         }

         return(join_on)
      },

      joinQuery       = function(right, col_left, operator, col_right) {
         on <- private$joinOn(right, col_left, operator, col_right)
         return(stri_c(sep = " ", "JOIN", right, on))
      }
   ),
   active  = list(
      query       = function() {
         query  <- list()
         select <- str_flatten(collapse = ", ", self$columns)
         select <- stri_c(sep = " ", "SELECT", select, "FROM")

         where <- stri_c("WHERE ", self$whereNested)
         join  <- str_flatten(collapse = "\n ", self$joins)

         query$results <- c(select, private$main, join, where)
         query$nrow    <- c("SELECT COUNT(*) AS nrow FROM", private$main, join, where)
         query         <- lapply(query, str_flatten, collapse = " ", na.rm = TRUE)

         return(query)
      },

      whereNested = function() {
         where <- NA_character_
         if (length(self$wheres) > 0) {
            where <- str_flatten(collapse = " ", self$wheres)
            where <- stri_c("(", where, ")")
         }

         return(where)
      }
   )
)
