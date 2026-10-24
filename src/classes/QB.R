# Kept from the deployed version in case other parts of the app call them.
# QB itself now uses private$cleanChr, which does the same work faster.
hasFixed <- function(pattern) {
   function(x) is.character(x) && any(stri_detect_fixed(x, pattern), na.rm = TRUE)
}

cleanNull <- function(x) {
   if (any(stri_detect_fixed(x, "\\0"), na.rm = TRUE)) {
      x <- gsub("\\\\0", "", x)
   }

   na_if(x, "")
}

# Muffle only the warnings whose message matches `pattern`; let others through.
suppressWarningsMatching <- function(expr, pattern) {
   withCallingHandlers(
      expr,
      warning = function(w) {
         if (grepl(pattern, conditionMessage(w))) invokeRestart("muffleWarning")
      }
   )
}

QB <- R6Class(
   "QB",
   public  = list(
      columns         = c(),
      wheres          = list(),
      joins           = list(),
      limits          = NULL,
      title           = NULL,
      unique          = FALSE,

      initialize      = function(db_conn = NULL) {
         private$conn <- db_conn
      },

      from            = function(table, final = FALSE) {
         private$main <- private$quoteIdentifier(table, final)
         self$title   <- private$getAlias(private$main)

         invisible(self)
      },

      table           = function(table, final = FALSE) {
         private$main <- private$quoteIdentifier(table, final)
         self$title   <- private$getAlias(private$main)

         invisible(self)
      },

      select          = function(...) {
         columns      <- match.call(expand.dots = FALSE)$`...`
         columns      <- as.character(unlist(columns))
         self$columns <- lapply(columns, private$quoteIdentifier)

         invisible(self)
      },

      selectRaw       = function(expression) {
         self$columns <- append(self$columns, expression)

         invisible(self)
      },

      where           = function(column, operator = NULL, value = NULL, boolean = "and") {
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

      whereIn         = function(column, value, boolean = "and") {
         column <- private$quoteIdentifier(column)
         column <- stri_c(column, " IN ('", str_flatten(collapse = "','", value), "')")

         private$addWhere(column, boolean)

         invisible(self)
      },

      whereNotIn      = function(column, value, boolean = "and") {
         column <- private$quoteIdentifier(column)
         column <- stri_c(column, " NOT IN ('", str_flatten(collapse = "','", value), "')")

         private$addWhere(column, boolean)

         invisible(self)
      },

      whereNull       = function(column, boolean = "and") {
         column <- private$quoteIdentifier(column)
         column <- stri_c(column, " IS NULL")

         private$addWhere(column, boolean)

         invisible(self)
      },

      whereNotNull    = function(column, boolean = "and") {
         column <- private$quoteIdentifier(column)
         column <- stri_c(column, " IS NOT NULL")

         private$addWhere(column, boolean)

         invisible(self)
      },

      whereBetween    = function(column, values, boolean = "and") {
         start <- dbQuoteString(private$conn, values[1])
         end   <- dbQuoteString(private$conn, values[2])

         column <- private$quoteIdentifier(column)
         column <- stri_c(sep = " ", column, "BETWEEN", start, "AND", end)

         private$addWhere(column, boolean)

         invisible(self)
      },

      whereNotBetween = function(column, values, boolean = "and") {
         start <- dbQuoteString(private$conn, values[1])
         end   <- dbQuoteString(private$conn, values[2])

         column <- private$quoteIdentifier(column)
         column <- stri_c(sep = " ", column, "NOT BETWEEN", start, "AND", end)

         private$addWhere(column, boolean)

         invisible(self)
      },

      whereRaw        = function(raw, boolean = "and") {
         private$addWhere(raw, boolean)

         invisible(self)
      },

      leftJoin        = function(right, col_left, operator, col_right, final = FALSE) {
         join       <- private$joinQuery(right, col_left, operator, col_right, final)
         join       <- stri_c(sep = " ", "LEFT", join)
         self$joins <- append(self$joins, join)

         invisible(self)
      },

      rightJoin       = function(right, col_left, operator, col_right, final = FALSE) {
         join       <- private$joinQuery(right, col_left, operator, col_right, final)
         join       <- stri_c(sep = " ", "RIGHT", join)
         self$joins <- append(self$joins, join)

         invisible(self)
      },

      join            = function(right, col_left, operator, col_right, final = FALSE) {
         self$joins <- append(self$joins, private$joinQuery(right, col_left, operator, col_right, final))

         invisible(self)
      },

      limit           = function(value, offset = NULL) {
         offset      <- ifelse(!is.null(offset), stri_c("OFFSET ", offset), "")
         self$limits <- stri_c(sep = " ", "LIMIT", value, offset)

         invisible(self)
      },

      distinct        = function() {
         self$unique <- TRUE

         invisible(self)
      },

      raw             = function(query) {
         class(query) <- "RawQB"
         return(query)
      },

      # progress   : show a progress bar (non-HTTP connections only)
      # chunk_size : rows per fetch (non-HTTP connections only); 1000 as before
      # format     : ClickHouseHTTP only. "TabSeparatedWithNamesAndTypes" (default,
      #              same as before) or "Arrow".
      # harmonize  : Arrow only. Cast each column to the class the TSV format
      #              would have returned, so results stay consistent with before.
      # Always returns a tibble.
      get             = function(progress = TRUE, chunk_size = 1000,
                                 format = c("TabSeparatedWithNamesAndTypes", "Arrow"),
                                 harmonize = TRUE) {
         format <- match.arg(format)

         # get actual result set
         conn <- private$conn
         if (inherits(conn, "Pool")) {
            conn <- localCheckout(conn)
         }

         sql <- self$query$results

         if (inherits(conn, "ClickHouseHTTPConnection") && format == "Arrow") {
            # one request, no COUNT(*) round trip
            results <- suppressWarningsMatching(dbGetQuery(conn, sql, format = "Arrow"), "Unsupported")
            if (harmonize) {
               results <- private$harmonizeArrow(conn, sql, results)
            }
         } else if (inherits(conn, "ClickHouseHTTPConnection")) {
            # one request, no COUNT(*) round trip
            results <- private$getTsv(conn, sql)
         } else {
            # Same driver calls as before (COUNT, dbSendQuery, fixed-size
            # dbFetch chunks, dbClearResult): a single large fetch can crash
            # some native drivers. Only the binding has changed.
            n_rows <- self$count()
            rs     <- dbSendQuery(conn, sql)

            if (n_rows >= chunk_size) {
               n_chunks <- ceiling(n_rows / chunk_size)

               if (progress) {
                  pb_name <- stri_c(self$title, ": :current of :total chunks [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed")
                  pb      <- progress_bar$new(format = pb_name, total = n_chunks, width = 100, clear = FALSE)
                  pb$tick(0)
               }

               # collect chunks in a list and bind once at the end; binding
               # inside the loop re-copies the growing data on every chunk
               chunks <- vector("list", n_chunks)
               for (i in seq_len(n_chunks)) {
                  chunks[[i]] <- dbFetch(rs, chunk_size)
                  if (progress) pb$tick(1)
               }
               results <- bind_rows(chunks)
            } else {
               results <- dbFetch(rs)
            }

            dbClearResult(rs)
         }

         results <- results %>%
            as_tibble() %>%
            mutate(
               across(where(is.character), private$cleanChr)
            ) %>%
            rename_with(
               ~case_when(
                  str_detect(., "\\.") ~ str_extract(., ".+\\.(.+)", 1),
                  TRUE ~ .
               )
            )

         return(results)
      },

      # Runs the query in TSV and in Arrow (ClickHouseHTTP only) and reports,
      # per column, the classes each format gives and how many values differ.
      # Values are compared column by column after sorting, so a different row
      # order between the two runs is not counted as a difference.
      compareFormats  = function() {
         tsv       <- self$get(format = "TabSeparatedWithNamesAndTypes")
         arrow_raw <- self$get(format = "Arrow", harmonize = FALSE)
         arrow     <- self$get(format = "Arrow")

         message("Rows: TSV = ", nrow(tsv), ", Arrow = ", nrow(arrow))
         if (ncol(tsv) != ncol(arrow)) {
            stop("Column counts differ: TSV = ", ncol(tsv), ", Arrow = ", ncol(arrow))
         }

         first_class <- function(df) vapply(df, function(x) class(x)[1], character(1))

         report <- tibble(
            column          = names(tsv),
            tsv_class       = first_class(tsv),
            arrow_raw_class = first_class(arrow_raw),
            arrow_class     = first_class(arrow),
            n_diff          = vapply(
               seq_along(tsv),
               function(i) private$nDiff(tsv[[i]], arrow[[i]]),
               numeric(1)
            )
         ) %>%
            mutate(
               same_class = tsv_class == arrow_class,
               same_tz    = vapply(
                  seq_along(tsv),
                  function(i) identical(attr(tsv[[i]], "tzone"), attr(arrow[[i]], "tzone")),
                  logical(1)
               ),
               consistent = same_class & same_tz & n_diff %in% 0
            )

         return(report)
      },

      max             = function(column) {
         prefix <- if_else(self$unique, "SELECT DISTINCT", "SELECT")
         select <- stri_c("MAX(", private$quoteIdentifier(column), ")")
         select <- stri_c(sep = " ", prefix, select, "FROM")

         where <- stri_c("WHERE ", self$whereNested)
         join  <- str_flatten(collapse = "\n ", self$joins)

         query <- str_flatten(c(select, private$main, join, where, self$limits), collapse = " ", na.rm = TRUE)

         # get actual result set
         conn <- private$conn
         if (inherits(conn, "Pool")) {
            conn <- localCheckout(conn)
         }
         results <- dbGetQuery(conn, query)

         return(results[[1]])
      },

      count           = function() {
         n_rows <- dbGetQuery(private$conn, self$query$nrow)
         n_rows <- sum(as.numeric(n_rows$nrow), na.rm = TRUE)

         return(n_rows)
      }
   ),
   private = list(
      main            = NULL,
      conn            = NULL,

      # Same cleanup as hasFixed/cleanNull did, but one pass per column, and the
      # regex replacements only run on the few strings that contain "\" or "Ã".
      cleanChr        = function(x) {
         x <- stri_encode(x, from = "UTF-8", to = "UTF-8")
         x[which(x == "")] <- NA_character_

         hit <- which(stri_detect_regex(x, "[\\\\Ã]"))
         if (length(hit) > 0) {
            y <- x[hit]
            y <- gsub("\\\\0", "", y)
            y[which(y == "")] <- NA_character_
            y <- gsub("\\\\'", "'", y)
            y <- gsub("\\\\", "\\", y)
            y <- gsub('Ã‘', 'Ñ', y)
            y <- gsub('Ã±', 'Ñ', y)
            x[hit] <- y
         }

         x
      },

      getTsv          = function(conn, sql) {
         suppressWarningsMatching(
            dbGetQuery(conn, sql, format = 'TabSeparatedWithNamesAndTypes'),
            "Unsupported"
         ) %>%
            as_tibble() %>%
            mutate(
               across(where(~inherits(., "IDate")), as.Date)
            )
      },

      # Cast Arrow columns to the classes the TSV format gives for the same
      # query. The TSV classes come from a LIMIT 0 run of the query, which
      # ClickHouse answers without reading data. Columns are matched by
      # position, since both runs return them in the same order.
      harmonizeArrow  = function(conn, sql, results) {
         template_sql <- stri_c("SELECT * FROM (", sql, ") AS `qb_template` LIMIT 0")
         template     <- tryCatch(
            private$getTsv(conn, template_sql),
            error = function(e) {
               warning("Arrow harmonization skipped: TSV template query failed: ", conditionMessage(e))
               NULL
            }
         )
         if (is.null(template)) {
            return(as_tibble(results))
         }

         if (ncol(template) != ncol(results)) {
            warning("Arrow harmonization skipped: TSV template has ", ncol(template),
                    " columns, Arrow result has ", ncol(results), ".")
            return(as_tibble(results))
         }

         results <- as_tibble(results)
         for (i in seq_along(results)) {
            results[[i]] <- private$castLike(results[[i]], template[[i]], names(results)[i])
         }

         results
      },

      castLike        = function(x, like, name = "") {
         is_time <- function(v) inherits(v, c("Date", "POSIXct"))

         # same class: only the time zone can still differ
         if (identical(class(x), class(like))) {
            if (inherits(like, "POSIXct")) attr(x, "tzone") <- attr(like, "tzone")
            return(x)
         }

         # An empty TSV template can report an all-NA logical for some types.
         # Casting real data to logical would destroy it, so leave it alone.
         if (is.logical(like) && !is.logical(x)) {
            warning("Column '", name, "': TSV class is logical, Arrow class is ",
                    class(x)[1], "; left as is.")
            return(x)
         }

         # String columns that Arrow delivered as binary (list of raw vectors)
         if (is.character(like) && is.list(x)) {
            is_raw <- vapply(x, function(v) is.null(v) || is.raw(v), logical(1))
            if (all(is_raw)) {
               return(vapply(x, function(v) if (is.null(v)) NA_character_ else rawToChar(v), character(1)))
            }
            warning("Column '", name, "': Arrow returned a list column, TSV returned character; left as is.")
            return(x)
         }

         if (is.factor(x)) x <- as.character(x)

         # Real UInt16/UInt32 columns: with convert_uint = TRUE, Arrow turns
         # them into Date/POSIXct. Take back the underlying number.
         if (is_time(x) && !is_time(like) && !is.character(like)) {
            x <- as.numeric(unclass(x))
         }

         if (inherits(like, "integer64")) return(bit64::as.integer64(x))
         if (inherits(x, "integer64"))    x <- as.numeric(x)

         if (inherits(like, "Date")) {
            if (inherits(x, "POSIXct")) {
               tz <- attr(x, "tzone")
               return(as.Date(x, tz = if (is.null(tz) || tz == "") "UTC" else tz))
            }
            return(as.Date(x))
         }
         if (inherits(like, "POSIXct")) {
            x <- as.POSIXct(x)
            attr(x, "tzone") <- attr(like, "tzone")
            return(x)
         }

         if (is.integer(like))   return(as.integer(x))
         if (is.double(like))    return(as.double(x))
         if (is.character(like)) return(as.character(x))

         warning("Column '", name, "': no cast from ", class(x)[1], " to ", class(like)[1], "; left as is.")
         x
      },

      # Number of positions where the sorted values of a and b differ
      # (NA if they can't be compared).
      nDiff           = function(a, b) {
         if (length(a) != length(b)) return(NA_real_)
         if (is.list(a) || is.list(b)) return(NA_real_)

         to_cmp <- function(v) {
            if (inherits(v, "POSIXct")) return(as.numeric(v))
            if (inherits(v, "integer64")) return(as.character(v))
            v
         }
         a <- to_cmp(a)
         b <- to_cmp(b)
         if (!identical(class(a), class(b))) {
            a <- as.character(a)
            b <- as.character(b)
         }

         a <- sort(a, na.last = TRUE)
         b <- sort(b, na.last = TRUE)

         same <- (is.na(a) & is.na(b)) | (!is.na(a) & !is.na(b) & a == b)
         if (is.double(a) && is.double(b)) {
            same <- same | (!is.na(a) & !is.na(b) & abs(a - b) <= 1e-9 * pmax(1, abs(a)))
         }

         sum(!same)
      },

      quoteIdentifier = function(identifier, final = FALSE) {
         if (is.null(identifier)) {
            return()
         }

         if (class(identifier) == "RawQB") {
            return(identifier)
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

         if (final) {
            query <- str_c(query, ' final')
         }

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

      joinQuery       = function(right, col_left, operator, col_right, final = FALSE) {
         right <- private$quoteIdentifier(right)
         on    <- private$joinOn(right, col_left, operator, col_right)
         if (isTRUE(final)) {
            right <- stri_c(right, " FINAL")
         }

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
         select <- self$columns
         if (length(select) == 0) {
            select <- "*"
         }

         prefix <- if_else(self$unique, "SELECT DISTINCT", "SELECT")
         select <- str_flatten(collapse = ", ", select)
         select <- stri_c(sep = " ", prefix, select, "FROM")

         where <- stri_c("WHERE ", self$whereNested)
         join  <- str_flatten(collapse = "\n ", self$joins)

         query$results <- c(select, private$main, join, where, self$limits)
         query$nrow    <- c("SELECT COUNT(*) AS nrow FROM (", query$results, ") as `dataset`")
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