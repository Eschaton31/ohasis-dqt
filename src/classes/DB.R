##  dbms Class -----------------------------------------------------------------

DB <- R6Class(
   "DB",
   public = list(
      slack_id          = NA_character_,
      timestamp         = NA_character_,
      output_title      = NA_character_,
      yr                = NA_character_,
      mo                = NA_character_,
      ym                = NA_character_,
      run_title         = NA_character_,
      next_date         = NA_character_,
      db_checks         = list(),
      ref_addr          = tibble(),
      ref_country       = tibble(),
      ref_faci          = tibble(),
      ref_faci_code     = tibble(),
      ref_staff         = tibble(),

      initialize        = function(yr = NULL, mo = NULL, title = NULL, update = NULL) {
         self$mo        <- ifelse(!is.null(mo), mo, input(prompt = "What is the reporting month?", max.char = 2))
         self$yr        <- ifelse(!is.null(yr), yr, input(prompt = "What is the reporting year?", max.char = 4))
         self$ym        <- stri_c(self$yr, ".", self$mo)
         self$run_title <- ifelse(!is.null(title), title, input(prompt = "Label the current run (brief, concise)"))

         # next date
         dates          <- self$get_date_ref("next", yr, mo)
         next_mo        <- dates$mo
         next_yr        <- dates$yr
         self$next_date <- as.Date(paste(sep = "-", next_yr, next_mo, "01"))

         # get current time
         self$timestamp    <- format(Sys.time(), "%Y.%m.%d.%H%M%S")
         self$output_title <- paste0(self$timestamp, "-", Sys.getenv("LW_USER"))

         # check database consistency
         log_info("Checking database for inconsistencies.")
         # self$check_consistency()

         # update data lake
         self$update_lake(update)

         # download latest references before final initialization
         self$download_refs()

         # subset of ref_faci
         self$ref_faci_code <- self$ref_faci %>%
            filter(!is.na(faci_code)) %>%
            mutate(
               branch_priority = case_when(
                  faci_id == "130001" ~ 1,
                  faci_id == "130605" ~ 2,
                  faci_id == "130748" ~ 3,
                  TRUE ~ 9999
               )
            ) %>%
            arrange(branch_priority) %>%
            distinct(faci_code, .keep_all = TRUE) %>%
            rename(
               sub_faci_code = faci_code
            ) %>%
            mutate(
               faci_code     = case_when(
                  stri_detect_regex(sub_faci_code, "^hash") ~ "hash",
                  stri_detect_regex(sub_faci_code, "^sail") ~ "sail",
                  stri_detect_regex(sub_faci_code, "^tly") ~ "tly",
                  TRUE ~ sub_faci_code
               ),
               sub_faci_code = if_else(
                  condition = nchar(sub_faci_code) == 3,
                  true      = NA_character_,
                  false     = sub_faci_code
               ),
               sub_faci_code = case_when(
                  faci_code == "hash" & is.na(sub_faci_code) ~ "hash-qc",
                  faci_code == "tly" & is.na(sub_faci_code) ~ "tly-anglo",
                  faci_code == "shp" & is.na(sub_faci_code) ~ "ship-makati",
                  TRUE ~ sub_faci_code
               ),
            ) %>%
            relocate(faci_code, sub_faci_code, .before = 1)

         # self$slack_id <- slackr_users() %>%
         #    filter(name == Sys.getenv("SLACK_PERSONAL")) %>%
         #    pull(id)

         log_success("ohasis initialized!")
      },

      download_refs     = function() {
         log_info("Downloading references.")
         lw_conn          <- connect("mariadb-lw")
         self$ref_country <- QB$new(lw_conn)$from("ohasis_lake.ref_country")$get()
         self$ref_addr    <- QB$new(lw_conn)$from("ohasis_lake.ref_addr")$get()
         # ref_faci's sort key includes faci_name & addr_psgc_reg, so a renamed / moved facility keeps its old
         # row; keep only the latest version per facility before dropping deleted ones
         self$ref_faci    <- QB$new(lw_conn)$from("ohasis_lake.ref_faci")$get() %>%
            mutate(sub_faci_key = coalesce(sub_faci_id, "")) %>%
            arrange(desc(`_version`)) %>%
            distinct(faci_id, sub_faci_key, .keep_all = TRUE) %>%
            select(-sub_faci_key) %>%
            filter(is.na(deleted_at))
         self$ref_staff   <- QB$new(lw_conn)$from("ohasis_lake.ref_staff")$get()
         dbDisconnect(lw_conn)
      },

      # get reference dates for the report
      get_date_ref      = function(type = NULL, yr = NULL, mo = NULL) {
         # next dates
         if (type == 'next') {
            dateMo <- ifelse(as.numeric(mo) == 12, '01', stringi::stri_pad_left(as.character(as.numeric(mo) + 1), 2, '0'))
            dateYr <- ifelse(as.numeric(mo) == 12, as.numeric(yr) + 1, as.numeric(yr)) %>% as.character()
         }

         # prev dates
         if (type == 'prev') {
            dateMo <- ifelse(as.numeric(mo) == 1, '12', stringi::stri_pad_left(as.character(as.numeric(mo) - 1), 2, '0'))
            dateYr <- ifelse(as.numeric(mo) == 1, as.numeric(yr) - 1, as.numeric(yr)) %>% as.character()
         }

         dateReturn <- list(mo = dateMo, yr = dateYr)

         return(dateReturn)
      },

      # refreshes db connection
      conn              = function(db = NULL) {
         return(
            switch(
               db,
               db = dbConnect(
                  RMariaDB::MariaDB(),
                  user     = Sys.getenv("DB_USER"),
                  password = Sys.getenv("DB_PASS"),
                  host     = Sys.getenv("db_host"),
                  port     = Sys.getenv("DB_PORT"),
                  timeout  = -1
               ),
               lw = dbConnect(
                  RClickhouse::clickhouse(),
                  user     = Sys.getenv("LW_USER"),
                  password = Sys.getenv("LW_PASS"),
                  host     = Sys.getenv("LW_HOST"),
                  port     = as.numeric(Sys.getenv("LW_PORT")),
                  timeout  = -1
               )
            )
         )
      },

      update_lake       = function(update = NULL) {
         update <- ifelse(!is.null(update), update, input("Update the data lake?", c("1" = "yes", "2" = "no", "3" = "all"), "1"))
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
                  self$data_factory("lake", table_name, update_type, default_yes)
               }

               lake_dir <- file.path(getwd(), "src", "data_lake", update_type)
               lapply(list.files(lake_dir, pattern = "ref_*.*\\.R"), lake_table)
               lapply(list.files(lake_dir, pattern = "px_*.*\\.R"), lake_table)
               lapply(list.files(lake_dir, pattern = "lab_*.*\\.R"), lake_table)
               lapply(list.files(lake_dir, pattern = "disp_*.*\\.R"), lake_table)
               lapply(list.files(lake_dir, pattern = "disc_*.*\\.R"), lake_table)
            }
            log_info("Data lake updated!")
         }
      },

      # upsert data
      upsert            = function(db_conn = NULL, db_type = NULL, table_name = NULL, data = NULL, id_col = NULL) {
         db_name <- switch(
            db_type,
            warehouse = stri_c("ohasis_", db_type),
            lake      = stri_c("ohasis_", db_type),
            api       = stri_c("ohasis_", db_type),
            db_type
         )

         table_space <- Id(schema = db_name, table = table_name)
         table_sql   <- DBI::SQL(paste0('`', db_name, '`.`', table_name, '`'))

         # check if table exists, if not create
         # if (!dbExistsTable(db_conn, table_name)) {
         #    sql <- self$create(db_name, table_name, data, id_col)
         #    dbExecute(db_conn, sql)
         # }

         # compare columns match, if not re-create table
         # names_data  <- sort(names(data))
         # names_table <- sort(dbListFields(db_conn, table_space))
         # if (!identical(names_data, names_table) & length(setdiff(names_data, names_table)) > 0) {
         #    # get current records (assuming change is only added columns)
         #    df <- dbReadTable(db_conn, table_space)
         #
         #    # recreate table using new columns
         #    sql <- self$create(db_name, table_name, data, id_col)
         #    dbExecute(db_conn, sql)
         #
         #    # re-insert data
         #    dbAppendTable(db_conn, table_sql, df)
         # }

         # clickhouse: dbx issues one DELETE mutation per batch + string-built INSERTs (slow)
         # stage ids in a temp table, delete in a single mutation, then use native bulk insert
         if (inherits(db_conn, c("ClickhouseConnection", "ClickhouseHTTPConnection"))) {
            stage_name <- stri_c("_upsert_", table_name, "_", stri_rand_strings(1, 8))
            stage_sql  <- DBI::SQL(paste0('`', db_name, '`.`', stage_name, '`'))
            id_sql     <- paste0('`', id_col, '`', collapse = ', ')

            # RClickhouse's bundled client only writes (U)Int, Float, String, UUID, Date, DateTime, Enum, Nullable, Array;
            # anything else (FixedString, LowCardinality, Bool, Date32, DateTime64, Decimal, ...) errors out (shown as Void).
            # stage those as a writable type and let clickhouse cast server-side on INSERT ... SELECT.
            # Date32 / DateTime64 go as text (formatted in R) so the full range / sub-second precision survives
            stage_type <- function(type) {
               wrap <- stri_match_first_regex(type, "^(Nullable|LowCardinality|Array)\\((.*)\\)$")
               if (!is.na(wrap[1])) {
                  inner <- stage_type(wrap[3])
                  return(if (wrap[2] == "LowCardinality") inner else stri_c(wrap[2], "(", inner, ")"))
               }
               if (stri_detect_regex(type, "^(U?Int(8|16|32|64)|Float(32|64)|String|UUID|Date|DateTime(\\(.*\\))?|Enum(8|16)\\(.*\\))$")) return(type)
               if (type == "Bool") return("UInt8")
               if (stri_detect_regex(type, "^Decimal")) return("Float64")
               "String"
            }
            server_tz <- dbGetQuery(db_conn, "SELECT timezone() AS tz")$tz
            col_types <- dbGetQuery(db_conn, stri_c("SELECT name, type FROM system.columns WHERE database = '", db_name, "' AND table = '", table_name, "'"))
            is_text   <- function(type) stri_detect_regex(type, "^(Nullable\\()?String\\)?$")
            col_types <- col_types %>%
               filter(name %in% names(data)) %>%
               mutate(
                  stage_type = vapply(type, stage_type, character(1), USE.NAMES = FALSE),
                  # RClickhouse checks the R type against the column type, so character data headed for a
                  # non-text column (e.g. dates kept as text) is sent as String and parsed by clickhouse instead
                  r_chr      = vapply(name, function(col) is.character(data[[col]]), logical(1), USE.NAMES = FALSE),
                  stage_type = if_else(r_chr & !is_text(stage_type), "String", stage_type),
                  tz         = coalesce(stri_match_first_regex(type, "DateTime64\\(\\d+,\\s*'([^']+)'\\)")[, 2], server_tz)
               )

            # columns staged as text but held as non-character in R are formatted here, in a form clickhouse parses back
            text_cols <- col_types %>% filter(is_text(stage_type))
            to_stage  <- function(df) {
               for (col in intersect(text_cols$name, names(df))) {
                  x <- df[[col]]
                  if (is.character(x)) {
                     # '' cannot be cast to a non-text type; send it as NULL so the target's DEFAULT applies
                     if (!stri_detect_regex(text_cols$type[text_cols$name == col], "String")) df[[col]] <- na_if(x, "")
                     next
                  }
                  df[[col]] <- if (inherits(x, "POSIXct")) {
                     # %OS truncates (.123 -> .122999); nudge by half a microsecond so it rounds instead
                     format(x + 5e-7, "%Y-%m-%d %H:%M:%OS6", tz = text_cols$tz[text_cols$name == col])
                  } else if (inherits(x, "Date")) {
                     format(x, "%Y-%m-%d")
                  } else if (is.logical(x)) {
                     as.character(as.integer(x))
                  } else {
                     as.character(x)
                  }
               }
               df
            }
            # RClickhouse maps data frame columns to table columns by position, so keep the data's column order
            # nullable = TRUE lets NA reach the server, where INSERT ... SELECT applies the target's DEFAULT (insert_null_as_default)
            stage_ddl <- function(cols, nullable = FALSE) {
               cols  <- col_types[match(cols, col_types$name), ]
               types <- cols$stage_type
               if (nullable) {
                  wrap        <- !stri_detect_regex(types, "^(Nullable|Array)\\(")
                  types[wrap] <- stri_c("Nullable(", types[wrap], ")")
               }
               stri_c("(", paste0('`', cols$name, '` ', types, collapse = ', '), ") ENGINE = Memory")
            }
            id_cast <- col_types %>%
               filter(name %in% id_col) %>%
               mutate(expr = stri_c("CAST(`", name, "`, '", gsub("'", "\\\\'", type), "') AS `", name, "`"))
            id_cast <- id_cast$expr[match(id_col, id_cast$name)]

            dbExecute(db_conn, stri_c("CREATE TABLE ", stage_sql, " ", stage_ddl(id_col)))
            on.exit(dbExecute(db_conn, stri_c("DROP TABLE IF EXISTS ", stage_sql)), add = TRUE)

            dbAppendTable(db_conn, stage_name, data %>% select(all_of(id_col)) %>% distinct() %>% to_stage(), row.names = NA)
            dbExecute(db_conn, stri_c("DELETE FROM ", table_sql, " WHERE (", id_sql, ") IN (SELECT ", paste(id_cast, collapse = ", "), " FROM ", stage_sql, ")"))

            # route through a stage table only when the target has types RClickhouse cannot write
            insert_chunk <- function(chunk) {
               suppress_warnings(dbAppendTable(db_conn, table_name, chunk, row.names = NA), 'column')
            }
            if (any(col_types$stage_type != col_types$type)) {
               data_name <- stri_c(stage_name, "_data")
               data_sql  <- DBI::SQL(paste0('`', db_name, '`.`', data_name, '`'))
               cols_sql  <- paste0('`', names(data), '`', collapse = ', ')

               dbExecute(db_conn, stri_c("CREATE TABLE ", data_sql, " ", stage_ddl(names(data), nullable = TRUE)))
               on.exit(dbExecute(db_conn, stri_c("DROP TABLE IF EXISTS ", data_sql)), add = TRUE)

               insert_chunk <- function(chunk) {
                  suppress_warnings(dbAppendTable(db_conn, data_name, to_stage(chunk), row.names = NA), 'column')
                  dbExecute(db_conn, stri_c("INSERT INTO ", table_sql, " (", cols_sql, ") SELECT ", cols_sql, " FROM ", data_sql))
                  dbExecute(db_conn, stri_c("TRUNCATE TABLE ", data_sql))
               }
            }

            # insert in chunks to cap memory / request size
            chunk_size <- 100000
            n_rows     <- nrow(data)
            if (n_rows > chunk_size) {
               chunks     <- split(data, ceiling(seq_len(n_rows) / chunk_size))
               chunk_bytes <- vapply(chunks, function(x) as.numeric(object.size(x)), numeric(1))

               pb <- progress_bar$new(format = ":bytes uploaded | :rate [:bar] (:percent) | eta: :eta | Elapsed: :elapsed", total = sum(chunk_bytes), width = 100, clear = FALSE)
               pb$tick(0)
               for (i in seq_along(chunks)) {
                  insert_chunk(chunks[[i]])
                  pb$tick(chunk_bytes[[i]])
               }
               cat("\n")
            } else {
               insert_chunk(data)
            }

            return(invisible(n_rows))
         }

         data %<>%
            mutate_if(is.Date, ~format(., '%Y-%m-%d')) %>%
            mutate_if(is.POSIXct, ~format(., '%Y-%m-%d %H:%M:%S'))

         # upsert data
         chunk_size <- 10000
         if (nrow(data) >= chunk_size) {
            # upload in chunks to monitor progress
            n_rows     <- nrow(data)
            n_chunks   <- rep(1:ceiling(n_rows / chunk_size), each = chunk_size)[1:n_rows]
            data       <- split(data, n_chunks)
            data_bytes <- as.numeric(object.size(data))

            # get progress
            pb <- progress_bar$new(format = ":bytes uploaded | :rate [:bar] (:percent) | eta: :eta | Elapsed: :elapsed", total = data_bytes, width = 100, clear = FALSE)
            pb$tick(0)
            for (i in seq_len(length(data))) {
               chunk_bytes <- as.numeric(object.size(data[[i]]))
               dbxDelete(db_conn, table_space, data[[i]] %>% select(any_of(id_col)), batch_size = 1000)
               suppress_warnings(dbxInsert(db_conn, table_name, data[[i]]), 'column')
               pb$tick(chunk_bytes)
            }
            cat("\n")
         } else {
            dbxDelete(db_conn, table_space, data %>% select(any_of(id_col)), batch_size = 1000)
            suppress_warnings(dbxInsert(db_conn, table_name, data), 'column')
         }
      },

      # create table
      create            = function(db_name = NULL, table_name = NULL, data = NULL, id_col = NULL) {
         # attached columns
         user_cols <- c("created_by", "updated_by", "deleted_by", "provider_id", "signatory_1", "signatory_2", "signatory_2", "user_id", "staff_id")
         text_faci <- c("prev_test_faci", "deliver_faci", "faci_label", "faci_name", "faci_name_clean", "faci_name_reg", "faci_name_prov", "faci_name_munc",
                        "faci_addr", "faci_nhsss_reg", "faci_nhsss_prov", "faci_nhsss_munc", "faci_type")

         # construct create based on data types
         df_str <- data %>%
            summary.default %>%
            as.data.frame %>%
            group_by(Var1) %>%
            spread(key = Var2, value = Freq) %>%
            ungroup %>%
            mutate(
               Type = case_when(
                  Var1 %in% text_faci ~ "text NULL default NULL collate 'utf8_general_ci'",
                  Var1 %in% user_cols & !(Var1 %in% id_col) ~ "char(10) NULL default NULL collate 'utf8_general_ci'",
                  Var1 %in% user_cols & Var1 %in% id_col ~ "char(10) NULL collate 'utf8_general_ci'",
                  Var1 == "faci_code" ~ "varchar(100) NULL collate 'utf8_general_ci'",
                  Var1 == "long" ~ "decimal(10,7) NULL collate 'utf8_general_ci'",
                  Var1 == "lat" ~ "decimal(9,7) NULL collate 'utf8_general_ci'",
                  Var1 == "rec_id" ~ "char(25) NULL collate 'utf8_general_ci'",
                  Var1 == "source_rec" ~ "char(25) NULL collate 'utf8_general_ci'",
                  Var1 == "destination_rec" ~ "char(25) NULL collate 'utf8_general_ci'",
                  Var1 == "rec_id_grp" ~ "varchar(100) NULL collate 'utf8_general_ci'",
                  Var1 == "central_id" ~ "char(18) NULL collate 'utf8_general_ci'",
                  Var1 == "patient_id" ~ "char(18) NULL collate 'utf8_general_ci'",
                  Var1 == "posterior" ~ "decimal(16,15) NULL collate 'utf8_general_ci'",
                  Var1 == "score" ~ "decimal(16,15) NULL collate 'utf8_general_ci'",
                  Var1 == "lv" ~ "decimal(16,15) NULL collate 'utf8_general_ci'",
                  Var1 == "jw" ~ "decimal(16,15) NULL collate 'utf8_general_ci'",
                  Var1 == "qgram" ~ "decimal(16,15) NULL collate 'utf8_general_ci'",
                  Var1 == "avg_dist" ~ "decimal(16,15) NULL collate 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "psgc") ~ "char(9) NULL collate 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "addr") ~ "text NULL default NULL collate 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "sub_faci") ~ "char(10) NULL collate 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "sub_source") ~ "char(10) NULL collate 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "x_age_c") ~ "varchar(9) NULL collate 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "faci") ~ "char(6) NULL collate 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "source") ~ "char(6) NULL collate 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "remarks") ~ "text NULL collate 'utf8_general_ci'",
                  stri_detect_fixed(Var1, "remarks") ~ "text NULL collate 'utf8_general_ci'",
                  Mode == "numeric" & Class == "Date" ~ "date NULL default NULL",
                  Mode == "numeric" & Class == "POSIXct" ~ "datetime NULL default NULL",
                  Mode == "numeric" ~ "int(11) NULL default NULL",
                  Mode == "character" ~ "varchar(150) NULL default NULL collate 'utf8_general_ci'",
                  Mode == "logical" ~ "boolean NULL default NULL",
                  TRUE ~ NA_character_
               ),
               sql  = paste0("`", Var1, "` ", Type),
            )

         # add indices if not in pk
         index <- ""
         if (!("central_id" %in% id_col) & "central_id" %in% names(data))
            index <- stri_c(index, ", index `central_id` (`central_id`)")
         if (!("patient_id" %in% id_col) & "patient_id" %in% names(data))
            index <- stri_c(index, ", index `patient_id` (`patient_id`)")
         if ("source_rec" %in% names(data))
            index <- stri_c(index, ", index `source_rec` (`source_rec`)")
         if ("destination_rec" %in% names(data))
            index <- stri_c(index, ", index `destination_rec` (`destination_rec`)")
         if ("idnum" %in% names(data))
            index <- stri_c(index, ", index `idnum` (`idnum`)")
         if ("art_id" %in% names(data))
            index <- stri_c(index, ", index `art_id` (`art_id`)")
         if ("mort_id" %in% names(data))
            index <- stri_c(index, ", index `mort_id` (`mort_id`)")
         if ("prep_id" %in% names(data))
            index <- stri_c(index, ", index `prep_id` (`prep_id`)")

         # implode into query
         pk_sql     <- paste(collapse = "`,`", id_col)
         create_sql <- paste(collapse = ",", df_str$sql)
         create_sql <- glue("create table `{db_name}`.`{table_name}` (\n",
                            "{create_sql},\n",
                            "\nPRIMARY key(`{pk_sql}`) {index}\n)\n",
                            "collate='utf8_general_ci'\nENGINE=InnoDB;")
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
            log_info("Updating {red(table_name)} @ the {red(db_type)}.")

            # get files
            factory_dir   <- file.path(getwd(), "src", paste0("data_", db_type))
            factory_files <- dir_info(factory_dir, recurse = TRUE, regexp = table_name)
            factory_file  <- filter(factory_files, basename(path) == paste0(table_name, '.R'))$path
            factory_sql   <- filter(factory_files, basename(path) == paste0(table_name, '.sql'))$path

            # open connections
            log_info("Opening connections.")
            db_conn      <- connect("ohasis-live")
            lw_conn      <- connect("ohasis-lw")
            table_exists <- dbExistsTable(lw_conn, table_name)

            # data for deletion (warehouse)
            for_delete <- data.frame()

            # read sql first, then parse for necessary snapshots
            query_snapshot <- paste0("select max(snapshot) as snapshot from ", db_name, ".", table_name)
            if (length(factory_sql) != 0) {
               sql_query  <- read_file(factory_sql)
               sql_tables <- str_extract(sql_query, "from[^:]*(?=;)")
               sql_delete <- str_extract(sql_query, "(?<=-- delete: ).*?(?=;)")
               if (is.na(sql_delete)) {
                  sql_delete <- str_extract(sql_query, "(?<=-- deleted: ).*?(?=;)")
               }

               sql_id <- str_extract(sql_query, "(?<=-- id_cols: ).*?(?=;)")

               id_col <- str_split(sql_id, ", ")[[1]]

               # query_table    <- str_extract(sql_query, "^[^:]*(?=;)")
               query_table    <- str_extract(sql_query, "^[\\s\\S][^;]+[^;]")
               query_nrow     <- stri_c("select count(*) as nrow ", sql_tables)
               query_delete   <- stri_c("delete from ", db_name, ".", table_name, " where ", sql_delete)
               # query_affected <- ifelse(
               #    str_detect(sql_id, "rec_id") &
               #       table_name != "px_pii" &
               #       table_exists,
               #    stri_c("select ", stri_c(collapse = ", ", stri_c(table_name, ".", id_col)), " from ohasis_lake.px_pii join ", db_name, ".", table_name, " on px_pii.rec_id = ", table_name, ".rec_id where px_pii.snapshot between ? and ?"),
               #    stri_c("select ", sql_id, " from ", db_name, ".", table_name, " where snapshot between ? and ?")
               # )
               query_affected <- ifelse(
                  str_detect(sql_id, "rec_id") &
                     table_name != "px_pii" &
                     table_exists,
                  stri_c("select ", stri_c(collapse = ", ", stri_c(table_name, ".", id_col)), " from ohasis_lake.px_pii join ", db_name, ".", table_name, " on px_pii.rec_id = ", table_name, ".rec_id where ((px_pii.created_at between ? and ?) or (px_pii.updated_at between ? and ?) or (px_pii.deleted_at between ? and ?))"),
                  stri_c("select ", sql_id, " from ", db_name, ".", table_name, " where ((created_at between ? and ?) or (updated_at between ? and ?) or (deleted_at between ? and ?))")
               )
            }

            # snapshots are the reference for scoping
            # get start date of records  to be fetched
            if (!is.null(from)) {
               snapshot_old <- from
            } else if (update_type == "refresh" | !table_exists) {
               snapshot_old <- "1970-01-01 00:00:00"

               if (table_exists) {
                  dbRemoveTable(lw_conn, table_space)
                  table_exists <- FALSE
               }
            } else if (table_exists) {
               snapshot_old <- as.character(dbGetQuery(lw_conn, query_snapshot)$snapshot)
            }

            # get end date of records  to be fetched
            if (!is.null(to)) {
               snapshot_new <- to
            } else {
               snapshot_new <- str_split(self$timestamp, "\\.")[[1]]
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
            log_info("Fetching records starting from: {green(format(as.POSIXct(snapshot_old), '%a %b %d, %Y %X'))}")
            if (length(factory_sql) != 0) {
               # get number of affected rows
               object <- tibble()
               params <- list(snapshot_old, snapshot_new, snapshot_old, snapshot_new, snapshot_old, snapshot_new)

               # use a diff connection if factory_sql exists for warehouse data
               if (length(factory_sql) != 0 &&
                  db_type == "warehouse" &&
                  table_name != "id_registry") {
                  dbDisconnect(db_conn)
                  db_conn <- connect("ohasis-lw")
               }
               n_rows <- dbGetQuery(db_conn, query_nrow, params = params)$nrow
               n_rows <- ifelse(length(n_rows) > 1, length(n_rows), as.numeric(n_rows))
               n_rows <- as.integer(n_rows)

               # get actual result set
               if (!is.na(n_rows) && n_rows > 0) {
                  continue <- 1
                  log_info("Number of records to fetch = {green(formatC(n_rows, big.mark = ','))}.")
                  rs <- dbSendQuery(db_conn, query_table, params = params)

                  chunk_size <- 1000
                  if (n_rows >= chunk_size) {
                     # upload in chunks to monitor progress
                     n_chunks <- ceiling(n_rows / chunk_size)

                     # get progress
                     pb_name <- paste0(table_name, ": :current of :total chunks [:bar] (:percent) | eta: :eta | Elapsed: :elapsed")

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

                  for_delete <- data.frame()
                  if (table_exists) {
                     # for_delete <- dbxSelect(lw_conn, query_affected, params = list(snapshot_old, snapshot_new))
                     # if (table_name %in% c("form_hts", "form_cfbs", "form_a")) {
                     #    for_delete <- dbxSelect(lw_conn, query_affected, params = params)
                     # } else {
                     #    for_delete <- dbxSelect(lw_conn, query_affected, params = list(unique(object[, sql_id[1]][[1]])))
                     # }
                     for_delete <- dbxSelect(lw_conn, query_affected, params = params)
                  }
               } else {
                  continue <- 0
               }
            } else {
               source(factory_file, local = TRUE)
            }

            # keep connection alive
            dbDisconnect(lw_conn)
            lw_conn <- connect("ohasis-lw")

            if (continue > 0) {
               # check if there is data for deletion
               if (table_exists && nrow(for_delete) > 0) {
                  dbxDelete(lw_conn, table_space, for_delete, batch_size = 1000)
                  log_success("Affected records removed = {red(formatC(nrow(for_delete), big.mark = ','))}.")
               }

               log_info("Uploading new data.")
               self$upsert(lw_conn, db_type, table_name, object, id_col)
               # update reference
               df <- data.frame(
                  user      = Sys.getenv("LW_USER"),
                  report_yr = self$yr,
                  report_mo = self$mo,
                  run_title = paste0(self$timestamp, " (", self$run_title, ")"),
                  table     = table_name,
                  rows      = nrow(object),
                  snapshot  = snapshot_new
               )

               # log if successful
               dbAppendTable(lw_conn, DBI::SQL(paste0('`', db_name, '`.`logs`')), df)
               log_success("Upload complete.")
            } else {
               log_info("No new/updated data found.")
            }

            # delete records based on schema
            if (length(factory_sql) != 0 && !is.na(query_delete)) {
               invalidate_execute  <- dbSendStatement(lw_conn, query_delete)
               invalidate_affected <- dbGetRowsAffected(invalidate_execute)
               log_success("Invalid records removed = {red(formatC(invalidate_affected, big.mark = ','))}.")
               dbClearResult(invalidate_execute)
            }

            # close connections
            dbDisconnect(db_conn)
            dbDisconnect(lw_conn)
         }
      },

      # get snapshot
      get_snapshots     = function(db_type = NULL, table_name = NULL) {
         snapshot    <- list()
         db_conn     <- connect("ohasis-lw")
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
            log_info("Latest snapshot = {red(format(snapshot$old, \"%a %b %d, %Y %X\"))}.")
         } else {
            snapshot$old <- as.POSIXct("1970-01-01 00:00:00", tz = "utc")
            log_info("No version found in data lake.")
         }

         # check if already exists
         if (dbExistsTable(db_conn, table_name) &&
            "snapshot" %in% dbListFields(db_conn, table_name)) {
            sql           <- dbSendQuery(db_conn, paste0("select max(snapshot) as snapshot from `", db_name, "`.`", table_name, "`;"))
            snapshot$data <- dbFetch(sql)$snapshot %>% as.POSIXct(tz = "utc")
            dbClearResult(sql)
         } else {
            snapshot$data <- as.POSIXct("1970-01-01 00:00:00", tz = "utc")
         }

         snapshot$new <- as.POSIXct(format(Sys.time(), "%Y-%m-%d %H:%M:%S"), tz = "utc")

         dbDisconnect(db_conn)
         return(snapshot)
      },

      check_consistency = function() {
         db_conn   <- connect("ohasis-live")
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
            log_warn("DB inconsistencies found! See {underline(red('db_checks'))} for more info.")
            if ("duped_rec_id" %in% names(checklist)) {
               log_error("Duplicated Record IDs found.")
            }
            if ("flipped_cid" %in% names(checklist))
               log_error("Flipped Central IDs found in ohasis registry.")
         } else {
            log_success("DB is clean.")
         }

         self$db_checks <- checklist

         invisible(self)
      },

      # method to decode address data
      get_addr          = function(data, addr_set = NULL, type = "nhsss") {
         # addr_set format:
         # reg, prov, munc

         type <- tolower(type)
         if (type %in% c("nhsss", "code")) {
            get_reg  <- as.symbol("nhsss_reg")
            get_prov <- as.symbol("nhsss_prov")
            get_munc <- as.symbol("nhsss_munc")
         } else if (type == "label") {
            get_reg  <- as.symbol("label_reg")
            get_prov <- as.symbol("label_prov")
            get_munc <- as.symbol("label_munc")
         } else if (type == "name") {
            get_reg  <- as.symbol("name_reg")
            get_prov <- as.symbol("name_prov")
            get_munc <- as.symbol("name_munc")
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
                     "psgc_reg",
                     "psgc_prov",
                     "psgc_munc"
                  )
               )
            ) %>%
            mutate_at(
               .vars = vars(!!coded_reg, !!coded_prov, !!coded_munc),
               ~if_else(is.na(.), "", .)
            ) %>%
            rename_all(
               ~case_when(
                  . == coded_reg ~ "psgc_reg",
                  . == coded_prov ~ "psgc_prov",
                  . == coded_munc ~ "psgc_munc",
                  TRUE ~ .
               )
            ) %>%
            left_join(
               y  = self$ref_addr %>%
                  select(
                     psgc_reg,
                     psgc_prov,
                     psgc_munc,
                     !!named_reg  := !!get_reg,
                     !!named_prov := !!get_prov,
                     !!named_munc := !!get_munc,
                  ),
               by = c("psgc_reg", "psgc_prov", "psgc_munc")
            ) %>%
            relocate(!!named_reg, !!named_prov, !!named_munc, .before = psgc_reg) %>%
            select(
               -psgc_reg,
               -psgc_prov,
               -psgc_munc
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
            nhsss = "faci_name_nhsss",
            code  = "faci_code",
            name  = "faci_name"
         )

         # check if sub_faci_id col exists
         if (!(sub_faci_id %in% names(linelist)))
            linelist[, sub_faci_id] <- NA_character_

         # check if addresses to  be extracted
         addr_cols <- NULL
         if (!is.null(addr_names)) {
            addr_cols <- c("addr_psgc_reg", "addr_psgc_prov", "addr_psgc_munc")
         }

         # convert to names
         faci_id     <- as.name(faci_id)
         sub_faci_id <- as.name(sub_faci_id)

         linelist %<>%
            mutate(
               fn_id = row_number()
            )

         mergeable <- linelist %>%
            select(fn_id, {{faci_id}}, {{sub_faci_id}}) %>%
            mutate(
               {{faci_id}}     := coalesce({{faci_id}}, ""),
               {{sub_faci_id}} := case_when(
                  str_left({{sub_faci_id}}, 6) != {{faci_id}} ~ "",
                  {{sub_faci_id}} == "130023_001" ~ "130023_001",
                  str_left({{sub_faci_id}}, 6) %in% c("130001", "130605", "040200", "130797") ~ {{sub_faci_id}},
                  TRUE ~ ""
               )
            ) %>%
            # get referenced data
            left_join(
               y  = self$ref_faci %>%
                  mutate(
                     faci_id     = coalesce(faci_id, ""),
                     sub_faci_id = coalesce(sub_faci_id, ""),
                  ) %>%
                  select(
                     {{faci_id}}     := faci_id,
                     {{sub_faci_id}} := sub_faci_id,
                     {{final_faci}}  := {{get}},
                     if (!is.null(addr_names)) {
                        any_of(addr_cols)
                     }
                  ),
               by = input_set[[1]]
            ) %>%
            # remove id data
            select(-any_of(input_set[[1]]))

         # extract address
         if (!is.null(addr_names)) {
            names(addr_cols) <- addr_names
            mergeable %<>%
               get_addr(
                  addr_cols,
                  return_type
               ) %>%
               relocate(names(addr_names), .after = {{final_faci}})
         }

         linelist %<>%
            # remove id data
            left_join(mergeable, join_by(fn_id)) %>%
            # move then rename to old version
            relocate({{final_faci}}, .after = {{sub_faci_id}}) %>%
            select(-any_of(input_set[[1]]), -fn_id)

         return(linelist)
      },

      # method to decode user/staff data
      get_staff         = function(data, user_set = NULL) {
         # column
         coded_user <- user_set[1] %>% as.symbol()
         named_user <- names(user_set[1]) %>% as.symbol()

         data %<>%
            left_join(
               y  = self$ref_staff %>%
                  select(
                     !!coded_user := staff_id,
                     !!named_user := name
                  ),
               by = as.character(coded_user)
            ) %>%
            # move then rename to old version
            relocate(!!named_user, .after = !!coded_user) %>%
            select(-as.character(coded_user))
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
         id_registry = NULL,
         reload = NULL
      ) {
         # open connections
         log_info("Opening connections.")
         db_conn <- connect("ohasis-lw")
         db_name <- "ohasis_warehouse"

         # references for old dataset
         old_tblspace  <- Id(schema = db_name, table = warehouse_table)
         old_tblschema <- dbplyr::in_schema(db_name, warehouse_table)
         oh_id_schema  <- dbplyr::in_schema("ohasis_lake", "id_registry")

         # check if dataset is to be re-loaded
         # todo: add checking of latest version
         reload <- ifelse(
            !is.null(reload) && reload %in% c("1", "2"),
            reload,
            input(
               prompt  = "How do you want to load the previous dataset?",
               options = c("1" = "reprocess", "2" = "download"),
               default = "1"
            )
         )

         if (!is.null(id_registry))
            tbl_ids <- id_registry
         else
            tbl_ids <- update_idreg()

         # if Yes, re-process registry
         if (reload == "1") {
            log_info("Re-processing the dataset from the previous reporting period.")

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
                     . == dta_pid ~ "patient_id",
                     TRUE ~ tolower(.)
                  )
               ) %>%
               get_cid(tbl_ids, patient_id) %>%
               relocate(central_id, .before = 1)

            if (!is.null(corr) && nrow(corr) > 0) {
               log_info("Performing cleaning on the dataset.")
               # old_dataset <- .cleaning_list(old_dataset, as.data.frame(corr), toupper(names(id_col)), id_col)
               old_dataset <- apply_corrections(old_dataset, corr %>%
                  rename_all(tolower) %>%
                  mutate(variable = tolower(variable)), names(id_col))
            }

            # drop clients
            if (!is.null(remove_rows) && nrow(remove_rows) > 0) {
               col         <- as.name(tolower(names(id_col)))
               old_dataset <- old_dataset %>%
                  mutate({{col}} := eval(parse(text = glue("as.{id_col}({names(id_col)})")))) %>%
                  anti_join(
                     y  = remove_rows %>%
                        mutate({{col}} := eval(parse(text = glue("as.{id_col}({names(id_col)})")))),
                     by = names(id_col)
                  )
            }

            log_info("Updating warehouse table.")
            # delete existing data, full refresh always
            if (dbExistsTable(db_conn, warehouse_table))
               suppress_warnings(dbExecute(db_conn, glue(r"(TRUNCATE `ohasis_warehouse`.`{warehouse_table}`;)")), 'column')
            # dbExecute(db_conn, glue(r"(drop table `ohasis_warehouse`.`{warehouse_table}`;)"))

            # upload info
            # .self$upsert(db_conn, "warehouse", warehouse_table, old_dataset, "patient_id")
            self$upsert(db_conn, "warehouse", warehouse_table, old_dataset, names(id_col))
         }

         if (reload == "2") {
            log_info("Downloading the dataset from the previous reporting period.")
            old_dataset <- dbTable(
               db_conn,
               db_name,
               warehouse_table,
               join = list(
                  "ohasis_warehouse.id_registry" = list(by = c("patient_id" = "patient_id"), cols = "central_id")
               )
            ) %>%
               mutate(
                  central_id = if_else(
                     condition = is.na(central_id),
                     true      = patient_id,
                     false     = central_id
                  )
               ) %>%
               relocate(central_id, .before = 1)
         }

         log_info("Closing connections.")
         dbDisconnect(db_conn)

         log_success("Done.")
         return(old_dataset)
      }
   ),
)
