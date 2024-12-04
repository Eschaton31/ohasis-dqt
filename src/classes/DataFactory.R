DataFactory <- R6Class(
   'DataFactory',
   public  = list(
      timestamp   = NA_character_,
      from        = NA_character_,
      to          = NA_character_,

      lake        = list(),
      warehouse   = list(),

      initialize  = function(lake, warehouse) {
         if (!missing(lake))
            self$lake <- lake

         if (!missing(warehouse))
            self$warehouse <- warehouse

         self$timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
      },

      upsertTable = function(schema, table) {
         log_info("Updating {red(table)} @ the {red(schema)}.")
         file   <- private$schemaFile(schema, table)
         action <- strsplit(file, "/")[[1]][1]

         factory <- switch(
            fs::path_ext(file),
            R   = private$executeR(file),
            sql = private$executeSql(schema, file)
         )
      }
   ),
   private = list(
      schemaId        = function(schema, table) Id(schema = schema, table = table),
      schemaName      = function(schema, table) stri_c(schema, ".", table),
      schemaFile      = function(schema, table) {
         dir <- switch(
            schema,
            lake             = "data_lake",
            ohasis_lake      = "data_lake",
            warehouse        = "data_warehouse",
            ohasis_warehouse = "data_warehouse"
         )

         files <- list.files(here("src", dir, pattern = table, recursive = TRUE))

         ref <- files[str_detect(files, "\\.sql")]
         if (length(ref) == 0) {
            ref <- files[str_detect(files, "\\.R")]
         }

         return(ref)
      },

      lastSync        = function(table) QB$new(.GlobalEnv$`oh-lw`)$from(table)$max('SNAPSHOT'),

      determinePeriod = function(table) {
         start <- ifelse(!is.na(self$from), self$from, private$lastSync(table))
         end   <- ifelse(!is.na(self$to), self$to, self$timestamp)
         return(list(start = start, end = end))
      },

      executeR        = function(file) {
         period       <- determinePeriod(tools::file_path_sans_ext(basename(file)))
         snapshot_old <- period$start
         snapshot_new <- period$end

         db_conn <- localCheckout(.GlobalEnv$`oh-live`)
         lw_conn <- localCheckout(.GlobalEnv$`oh-lw`)

         source(file, local = TRUE)

         return(
            list(
               continue = continue,
               delete   = for_delete,
               data     = object
            )
         )
      },
      executeSql      = function(schema, file) {
         table_name <- tools::file_path_sans_ext(basename(file))

         schema_id   <- private$schemaId(table, name)
         schema_name <- private$schemaName(table, name)

         sql       <- read_file(file)
         from_join <- str_extract(sql, "FROM[^:]*(?=;)")
         delete    <- str_extract(sql, "(?<=-- DELETE: ).*?(?=;)")
         if (is.na(delete)) {
            delete <- str_extract(sql, "(?<=-- DELETED: ).*?(?=;)")
         }

         id     <- str_extract(sql, "(?<=-- ID_COLS: ).*?(?=;)")
         id_col <- str_split(id, ", ")[[1]]

         query <- list(
            table    = str_extract(query, "^[\\s\\S][^;]+[^;]"),
            nrow     = stri_c("SELECT COUNT(*) AS nrow ", from_join),
            delete   = stri_c("DELETE FROM ", schema_name, " WHERE ", delete),
            affected = ifelse(
               str_detect(id, "REC_ID") & table_name != "px_pii",
               stri_c("SELECT ", stri_c(collapse = ", ", stri_c(table_name, ".", id_col)), " FROM ohasis_lake.px_pii JOIN ", schema_name, " ON px_pii.REC_ID = ", table_name, ".REC_ID WHERE ((px_pii.CREATED_AT BETWEEN ? AND ?) OR (px_pii.UPDATED_AT BETWEEN ? AND ?) OR (px_pii.DELETED_AT BETWEEN ? AND ?))"),
               stri_c("SELECT ", sql_id, " FROM ", schema_name, " WHERE ((CREATED_AT BETWEEN ? AND ?) OR (UPDATED_AT BETWEEN ? AND ?) OR (DELETED_AT BETWEEN ? AND ?))")
            )
         )

         # use a diff connection if factory_sql exists for warehouse data
         db_conn <- localCheckout(.GlobalEnv$`oh-live`)
         lw_conn <- localCheckout(.GlobalEnv$`oh-lw`)
         if (schema == "ohasis_warehouse" && table_name != "id_registry") {
            db_conn <- lw_conn
         }

         table_exists <- dbExistsTable(conn, schema_id)

         period <- determinePeriod(table_name)
         params <- list(period$start, period$end, period$start, period$end, period$start, period$end)

         # get number of affected rows
         n_rows <- dbGetQuery(db_conn, query$nrow, params = params)$nrow
         n_rows <- ifelse(length(n_rows) > 1, length(n_rows), as.numeric(n_rows))
         n_rows <- as.integer(n_rows)

         # get actual result set
         continue   <- 0
         object     <- tibble()
         for_delete <- data.frame()

         if (!is.na(n_rows) && n_rows > 0) {
            continue <- 1
            log_info("Number of records to fetch = {green(formatC(n_rows, big.mark = ','))}.")
            rs <- dbSendQuery(db_conn, query$table, params = params)

            chunk_size <- 1000
            if (n_rows >= chunk_size) {
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

            if (table_exists) {
               for_delete <- dbxSelect(lw_conn, query$affected, params = params)
            }
         }

         return(
            list(
               continue = continue,
               delete   = for_delete,
               data     = object
            )
         )
      }
   )
)

try <- DataFactory$new("px_pii")
try$lastSync("px_pii")

dx        <- read_dta(hs_data("harp_dx", "reg", 2024, 9))
dir       <- here("src", "data_lake")
factories <- fs::dir_info(dir, recurse = TRUE, type = "file") %>%
   select(path) %>%
   mutate(path = substr(str_replace_all(path, dir, ""), 2, 1000)) %>%
   separate_wider_delim(path, "/", names = c("procedure", "file")) %>%
   separate_wider_delim(file, ".", names = c("table", "type")) %>%
   mutate(exists = 1) %>%
   pivot_wider(names_from = type, values_from = exists)
factories <- substr(str_replace_all(factories, dir, ""), 2, 1000)
factories <- lapply(factories, str_split, "/")
factories <- lapply(factories, "[[", 1)

names(factories) <- sapply(factories, "[[", 2)

upsert  <- factories[lapply(factories, "[[", 1) == "upsert"]
refresh <- factories[lapply(factories, "[[", 1) == "refresh"]

classes <- sapply(dx, class)