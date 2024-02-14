QB <- R6Class(
   "QB",
   public  = list(
      columns      = "*",
      wheres       = list(),
      joins        = list(),
      limits       = NULL,
      title        = NULL,

      initialize   = function(db_conn = NULL) {
         private$conn <- db_conn
      },

      from         = function(table) {
         private$main <- private$quoteIdentifier(table)
         self$title   <- private$getAlias(private$main)

         invisible(self)
      },

      select       = function(...) {
         columns      <- match.call(expand.dots = FALSE)$`...`
         columns      <- as.character(columns)
         self$columns <- lapply(columns, private$quoteIdentifier)

         invisible(self)
      },

      selectRaw    = function(expression) {
         self$columns <- append(self$columns, expression)

         invisible(self)
      },

      where        = function(column, operator = NULL, value = NULL, boolean = "and") {
         if (class(column) != "function") {
            if (!is.null(operator) & is.null(value)) {
               value    <- operator
               operator <- "="
            }

            value  <- dbQuoteString(private$conn, value)
            column <- private$quoteIdentifier(column)
            column <- stri_c(sep = " ", column, operator, value)
         } else {
            column <- do.call(column, list())
         }

         private$addWhere(column, boolean)

         invisible(self)
      },

      whereIn      = function(column, value, boolean = "and") {
         column <- private$quoteIdentifier(column)
         column <- stri_c(column, " IN ('", str_flatten(collapse = "','", value), "')")

         private$addWhere(column, boolean)

         invisible(self)
      },

      whereNotIn   = function(column, value, boolean = "and") {
         column <- private$quoteIdentifier(column)
         column <- stri_c(column, " NOT IN ('", str_flatten(collapse = "','", value), "')")

         private$addWhere(column, boolean)

         invisible(self)
      },

      whereNull    = function(column, boolean = "and") {
         column <- private$quoteIdentifier(column)
         column <- stri_c(column, " IS NULL")

         private$addWhere(column, boolean)

         invisible(self)
      },

      whereNotNull = function(column, boolean = "and") {
         column <- private$quoteIdentifier(column)
         column <- stri_c(column, " IS NOT NULL")

         private$addWhere(column, boolean)

         invisible(self)
      },

      whereBetween = function(column, values, boolean = "and") {
         start <- dbQuoteString(private$conn, values[1])
         end   <- dbQuoteString(private$conn, values[2])

         column <- private$quoteIdentifier(column)
         column <- stri_c(sep = " ", column, "BETWEEN", start, "AND", end)

         private$addWhere(column, boolean)

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

      limit        = function(value) {
         self$limits <- stri_c(sep = " ", "LIMIT", value)

         invisible(self)
      },

      get          = function() {
         n_rows <- self$count()

         results <- list()

         # get actual result set
         rs         <- dbSendQuery(private$conn, self$query$results)
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
      },

      count        = function() {
         n_rows <- dbGetQuery(private$conn, self$query$nrow)
         n_rows <- sum(as.numeric(n_rows$nrow), na.rm = TRUE)

         return(n_rows)
      }
   ),
   private = list(
      main            = NULL,
      conn            = NULL,

      quoteIdentifier = function(identifier) {
         if (is.null(identifier)) {
            return()
         }

         identifier <- str_replace(identifier, " as ", " AS ")
         pieces     <- str_split(identifier, " AS ", simplify = TRUE)
         pieces     <- as.character(pieces)

         actual <- NA_character_
         actual <- str_split(pieces[1], "\\.", simplify = TRUE)
         actual <- dbQuoteIdentifier(private$conn, as.character(actual))
         actual <- str_flatten(collapse = ".", actual)
         actual <- str_replace(actual, "`\\*`", "*")

         alias <- NA_character_
         if (length(pieces) == 2) {
            alias <- dbQuoteIdentifier(private$conn, pieces[2])
         }

         query <- str_flatten(collapse = " AS ", na.rm = TRUE, c(actual, alias))

         return(query)
      },

      getAlias        = function(identifier) {
         if (!str_detect(identifier, " AS ")) {
            pieces <- str_split(identifier, "\\.", simplify = TRUE)
            pieces <- as.character(pieces)
            return(pieces[length(pieces)])
         }

         return(str_extract(identifier, " AS (.*)", 1))
      },

      joinOn          = function(right, col_left, operator, col_right) {
         join_on   <- ""
         col_left  <- private$quoteIdentifier(col_left)
         col_right <- private$quoteIdentifier(col_right)

         if (private$getAlias(col_left) == private$getAlias(col_right) & operator == "=") {
            join_on <- stri_c("USING (", private$getAlias(col_left), ")")
         } else {
            join_on <- stri_c(sep = " ", "ON", col_left, operator, col_right)
         }

         return(join_on)
      },

      joinQuery       = function(right, col_left, operator, col_right) {
         right <- private$quoteIdentifier(right)
         on    <- private$joinOn(right, col_left, operator, col_right)
         return(stri_c(sep = " ", "JOIN", right, on))
      },

      addWhere        = function(query, boolean) {
         if (length(self$wheres) > 0) {
            boolean <- ifelse(toupper(boolean) == "OR", "OR", "AND")
            query   <- stri_c(sep = " ", boolean, query)
         }
         self$wheres <- append(self$wheres, query)

         invisible(self)
      }
   ),
   active  = list(
      query       = function() {
         query  <- list()
         select <- str_flatten(collapse = ", ", self$columns)
         select <- stri_c(sep = " ", "SELECT", select, "FROM")

         where <- stri_c("WHERE ", self$whereNested)
         join  <- str_flatten(collapse = "\n ", self$joins)

         query$results <- c(select, private$main, join, where, self$limits)
         query$nrow    <- c("SELECT COUNT(*) AS nrow FROM", private$main, join, where, self$limits)
         query         <- lapply(query, str_flatten, collapse = " ", na.rm = TRUE)
         query         <- lapply(query, stri_c, ";")

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
