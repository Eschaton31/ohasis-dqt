QB <- setRefClass(
   Class    = "DB",
   contains = "Project",
   fields   = list(
      conn   = "ANY",
      left   = "character",
      cols   = "character",
      filter = "list",
      joins  = "list",
      main   = "ANY"
   ),
   methods  = list(
      initialize = function(db_conn, table) {
         "This method is called when you create an instance of this class."

         main   <<- table
         left   <<- .self$alias(table)
         conn   <<- db_conn
         cols   <<- "*"
         filter <<- list()
         joins  <<- list()
      },

      select     = function(...) {
         .self$cols <- as.character(match.call(expand.dots = FALSE)$`...`)
      },

      where      = function(col, operator, value) {
         .self$filter <- append(.self$filter, stri_c(sep = " ", col, operator, dbQuoteString(con, value)))
      },

      whereIn    = function(col, value) {
         .self$filter <- append(.self$filter, stri_c(col, " IN ('", stri_c(collapse = "','", value), "')"))
      },

      whereNotIn    = function(col, value) {
         .self$filter <- append(.self$filter, stri_c(col, " NOT IN ('", stri_c(collapse = "','", value), "')"))
      },

      leftJoin   = function(right, col_left, operator, col_right) {
         .self$joins <- append(.self$joins, stri_c(sep = " ", "LEFT JOIN", right, .self$joinOn(right, col_left, operator, col_right)))
      },

      rightJoin  = function(right, col_left, operator, col_right) {
         .self$joins <- append(.self$joins, stri_c(sep = " ", "RIGHT JOIN", right, .self$joinOn(right, col_left, operator, col_right)))
      },

      join       = function(right, col_left, operator, col_right) {
         .self$joins <- append(.self$joins, stri_c(sep = " ", "JOIN", right, .self$joinOn(right, col_left, operator, col_right)))
      },

      joinOn     = function(right, col_left, operator, col_right) {
         join_on <- ""
         if (col_left == col_right & operator == "=") {
            join_on <- stri_c("USING (", col_left, ")")
         } else {
            col_left  <- stri_c(.self$left, ".", col_left)
            col_right <- stri_c(.self$alias(right), ".", col_right)
            join_on   <- stri_c(sep = " ", "ON", col_left, operator, col_right)
         }

         return(join_on)
      },

      alias      = function(identifier) {
         return(ifelse(str_detect(identifier, " AS "), str_extract(identifier, " AS (.*)", 1), identifier))
      },

      query      = function() {
         query  <- list()
         select <- stri_c(collapse = ", ", .self$cols)
         select <- stri_c(sep = " ", "SELECT", select, "FROM")

         where <- ""
         if (length(.self$filter) > 0) {
            where <- stri_c(collapse = " AND ", .self$filter)
            where <- stri_c("WHERE ", where)
         }

         join <- ""
         if (length(.self$joins) > 0) {
            join <- stri_c(collapse = "\n ", .self$joins)
         }

         query$results <- stri_c(sep = " ", select, .self$main, join, where)
         query$nrow    <- stri_c(sep = " ", "SELECT COUNT(*) AS nrow FROM", .self$main, join, where)

         return(query)
      },


      get        = function() {
         query <- .self$query()

         n_rows <- dbGetQuery(conn, query$nrow)
         n_rows <- sum(as.numeric(n_rows$nrow), na.rm = TRUE)

         results <- list()

         # get actual result set
         rs         <- dbSendQuery(conn, query$results)
         chunk_size <- 1000
         if (n_rows >= chunk_size) {
            # upload in chunks to monitor progress
            n_chunks <- ceiling(n_rows / chunk_size)

            pb_name <- stri_c(.self$left, ": :current of :total chunks [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed")
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
   )
)

dx <- hs_data("harp_dx", "reg", 2023, 10) %>%
   read_dta(
      col_select = c(
         REC_ID,
         idnum,
         CONFIRM_CODE = labcode2,
         FIRST        = firstname,
         MIDDLE       = middle,
         LAST         = last,
         SUFFIX       = name_suffix,
         BIRTHDATE    = bdate,
         SEX          = sex
      )
   ) %>%
   rename(
      CONFIRM_CODE = labcode2,
      FIRST        = firstname,
      MIDDLE       = middle,
      LAST         = last,
      SUFFIX       = name_suffix,
      BIRTHDATE    = bdate,
      SEX          = sex
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~clean_pii(.)
   )

dx <- hs_data("harp_dx", "reg", 2023, 10) %>%
   read_dta(
      col_select = c(
         REC_ID,
         idnum,
         CONFIRM_CODE = labcode2,
         FIRST        = firstname,
         MIDDLE       = middle,
         LAST         = last,
         SUFFIX       = name_suffix,
         BIRTHDATE    = bdate,
         SEX          = sex
      )
   ) %>%
   rename(
      CONFIRM_CODE = labcode2,
      FIRST        = firstname,
      MIDDLE       = middle,
      LAST         = last,
      SUFFIX       = name_suffix,
      BIRTHDATE    = bdate,
      SEX          = sex
   ) %>%
   mutate_if(
      .predicate = is.character,
      ~clean_pii(.)
   )

try <- QB(con, "ohasis_lake.px_hiv_testing AS test")
try$select(
   test.REC_ID,
   test.CONFIRM_CODE,
   pii.FIRST,
   pii.MIDDLE,
   pii.LAST,
   pii.SUFFIX,
   pii.BIRTHDATE,
   pii.SEX
)
try$whereIn("REC_ID", dx$REC_ID)
try$leftJoin("ohasis_lake.px_pii AS pii", "REC_ID", "=", "REC_ID")
all <- try$get() %>%
   mutate_if(
      .predicate = is.character,
      ~clean_pii(.)
   ) %>%
   mutate(
      SEX = remove_code(SEX)
   )

check <- dx %>% left_join(all %>% mutate(oh = 1))
update <- check %>%
   filter(is.na(oh)) %>%
   left_join(all, join_by(REC_ID)) %>%
   filter(is.na(BIRTHDATE.y), !is.na(BIRTHDATE.x)) %>%
   mutate(
      SQL = stri_c("UPDATE px_info AS name JOIN px_record AS rec USING (REC_ID) SET name.BIRTHDATE = '", BIRTHDATE.x, "', rec.UPDATED_BY = '1300000001', rec.UPDATED_AT = NOW() WHERE REC_ID = '", REC_ID, "';")
   )

write_clip(update$SQL)

con <- ohasis$conn("lw")
pos <- QB(con, "ohasis_lake.px_hiv_testing AS test")
pos$leftJoin("ohasis_lake.px_pii AS pii", "REC_ID", "=", "REC_ID")
pos$where("CONFIRM_RESULT", 'REGEXP', "Positive")
pos$select(
   test.REC_ID,
   test.CONFIRM_CODE,
   pii.FIRST,
   pii.MIDDLE,
   pii.LAST,
   pii.SUFFIX,
   pii.BIRTHDATE,
   pii.SEX
)
pos <- pos$get()
dbDisconnect(con)


con <- ohasis$conn("db")
oh <- QB(con, "ohasis_interim.px_confirm AS conf")
oh$where('FINAL_RESULT', "REGEXP", "Positive")
oh$whereNotIn('REC_ID', pos$REC_ID)
oh <- oh$get()
dbDisconnect(con)

update_credentials(pos$REC_ID)