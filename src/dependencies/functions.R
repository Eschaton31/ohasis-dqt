##  Helper functions -----------------------------------------------------------

# taking user input
input <- function(prompt = NULL, options = NULL, default = NULL, max.char = NULL, is.num = NULL) {
   # options must all be quoted
   # options must be of the format: integer = definition (i.e., "1" = "yes")

   prompt      <- paste0("\n", blue(prompt))
   default_txt <- ""
   if (!is.null(default))
      default_txt <- paste0("\nDefault: ", underline(magenta(default)))

   if (!is.null(options)) {
      key     <- names(options)
      val     <- stri_trans_totitle(options)
      options <- paste(collapse = "\n", paste0(underline(green(key)), " ", cyan(val)))
      prompt  <- paste0(prompt, "\n", options)
   }
   prompt <- paste0(prompt, "\n", default_txt, "\nPress <RETURN> to continue: ")

   # get user input
   # cat(prompt, "\n")
   data <- gtools::ask(prompt)

   # check if is integer
   if (data != "" & !is.null(options)) {
      # while (!StrIsNumeric(data))
      #    data <- gtools::ask("Please enter a valid selection: ")

      while (!(data %in% key))
         data <- gtools::ask("Please choose a value from selection: ")
   }

   # if empty, use default
   if (data == "" & !is.null(default))
      data <- default

   # if no default, throw error
   if (data == "" & is.null(default))
      while (data == "")
         data <- gtools::ask("Please provide an input: ")


   # check if max characters defined
   if (!is.null(max.char) && nchar(data) > max.char)
      while (nchar(data) > max.char)
         data <- gtools::ask("Input exceeds the maximum number of characters: ")

   # return value
   return(data)
}

# directory checker-creator
check_dir <- function(dir) {
   if (!dir.exists(file.path(dir))) {
      dir.create(file.path(dir), TRUE, TRUE)
      .log_info("Directory successfully created.")
   } else {
      .log_warn("Directory already exists.\n")
   }
}

# logger
.log_info <- function(msg = NULL) {
   .log_type <- "INFO" %>% stri_pad_right(7, " ")
   log       <- bold(blue(.log_type)) %+% magenta(glue(' [{format(Sys.time(), "%Y-%m-%d %H:%M:%S")}]'))
   msg       <- glue(msg, .envir = parent.frame(1))
   cat(log, msg, "\n")
}

.log_success <- function(msg = NULL) {
   .log_type <- "SUCCESS" %>% stri_pad_right(7, " ")
   log       <- bold(green(.log_type)) %+% magenta(glue(' [{format(Sys.time(), "%Y-%m-%d %H:%M:%S")}]'))
   msg       <- glue(msg, .envir = parent.frame(1))
   cat(log, msg, "\n")
}

.log_warn <- function(msg = NULL) {
   .log_type <- "WARN" %>% stri_pad_right(7, " ")
   log       <- bold(yellow(.log_type)) %+% magenta(glue(' [{format(Sys.time(), "%Y-%m-%d %H:%M:%S")}]'))
   msg       <- glue(msg, .envir = parent.frame(1))
   cat(log, msg, "\n")
}

.log_error <- function(msg = NULL) {
   .log_type <- "ERROR" %>% stri_pad_right(7, " ")
   log       <- bold(red(.log_type)) %+% magenta(glue(' [{format(Sys.time(), "%Y-%m-%d %H:%M:%S")}]'))
   msg       <- glue(msg, .envir = parent.frame(1))
   cat(log, msg, "\n")
}

# sheets cleaning per id
.cleaning_list <- function(data_to_clean = NULL, cleaning_list = NULL, corr_id_name = NULL, corr_id_type = NULL) {
   data    <- data_to_clean %>% filter(!is.na({{corr_id_name}}))
   # for (i in seq_len(nrow(cleaning_list))) {
   #
   #    # load idnum and name of variable
   #    df       <- cleaning_list[i,] %>% as.data.frame()
   #    id       <- paste0("df[,'", corr_id_name, "'] %>% as.", corr_id_type, '()')
   #    id       <- eval(parse(text = id))
   #    eb_id    <- tolower(corr_id_name) %>% as.symbol()
   #    variable <- df$VARIABLE %>% as.symbol()
   #
   #    # evaluate data type of variable
   #    if (df$NEW_VALUE == "NULL")
   #       value <- paste0('as.', df$FORMAT, '(NA)')
   #    else
   #       value <- paste0("'", df$NEW_VALUE, "'", ' %>% as.', df$FORMAT, '()')
   #
   #    value <- eval(parse(text = value))
   #
   #    # update data
   #    data %<>%
   #       mutate(
   #          !!variable := if_else(
   #             condition = !!eb_id == id,
   #             true      = value,
   #             false     = !!variable,
   #             missing   = !!variable
   #          )
   #       )
   # }
   eb_id   <- tolower(corr_id_name)
   id_type <- typeof(data[[eb_id]])
   id_type <- if_else(id_type == "double", "numeric", id_type)
   for (var in unique(cleaning_list$VARIABLE)) {
      var_clean <- cleaning_list %>%
         filter(VARIABLE == var) %>%
         rowwise() %>%
         mutate(
            {{corr_id_name}} := eval(parse(text = glue("as.{id_type}({corr_id_name})"))),
            NEW_VALUE        = eval(parse(text = glue("as.{FORMAT}('{NEW_VALUE}')"))),
         ) %>%
         ungroup() %>%
         select(
            {{eb_id}} := {{corr_id_name}},
            NEW_VALUE
         ) %>%
         mutate(
            update = 1,
         )
      data %<>%
         left_join(
            y  = var_clean,
            by = eb_id
         ) %>%
         mutate(
            {{var}} := if_else(
               condition = update == 1,
               true      = NEW_VALUE,
               false     = !!as.symbol(var),
               missing   = !!as.symbol(var)
            )
         ) %>%
         select(-update, -NEW_VALUE)
   }
   return(data)
}

# upload to gdrive/gsheets validations
.validation_gsheets <- function(data_name = NULL, parent_list = NULL, drive_path = NULL, surv_name = NULL, channels = NULL) {
   .log_info("Uploading to GSheets..")
   slack_by     <- (slackr_users() %>% filter(name == Sys.getenv("SLACK_PERSONAL")))$id
   empty_sheets <- ""
   gsheet       <- paste0(data_name, "_", format(Sys.time(), "%Y.%m.%d"))
   drive_file   <- drive_get(paste0(drive_path, gsheet))

   # list of validations
   issues_list <- names(parent_list)

   # create as new if not existing
   corr_status <- "old"
   if (nrow(drive_file) == 0) {
      corr_status <- "new"
      drive_rm(paste0("~/", gsheet))
      gs4_create(gsheet, sheets = parent_list)
      drive_mv(drive_get(paste0("~/", gsheet))$id %>% as_id(), drive_path, overwrite = TRUE)
   }

   # acquire sheet_id
   drive_file <- drive_get(paste0(drive_path, gsheet))
   drive_link <- paste0("https://docs.google.com/spreadsheets/d/", drive_file$id, "/|GSheets Link: ", gsheet)
   slack_msg  <- glue(">*{surv_name}*\n>Conso validation sheets for `{data_name}` have been updated by <@{slack_by}>.\n><{drive_link}>")
   for (issue in issues_list) {
      # add issue
      if (nrow(parent_list[[issue]]) > 0) {
         if (corr_status == "old")
            sheet_write(parent_list[[issue]], drive_file$id, issue)
         else
            range_autofit(drive_file$id, issue)
      }
   }

   # delete list of empty dataframes from sheet
   .log_info("Deleting empty sheets.")
   for (issue in issues_list)
      if (nrow(parent_list[[issue]]) == 0 & issue %in% sheet_names(drive_file$id))
         empty_sheets <- append(empty_sheets, issue)
   for (issue in sheet_names(drive_file$id))
      if (!(issue %in% issues_list))
         empty_sheets <- append(empty_sheets, issue)

   # delete if existing sheet no longer has values in new run
   if (length(empty_sheets[-1]) > 0)
      sheet_delete(drive_file$id, empty_sheets[-1])

   # log in slack
   if (is.null(channels)) {
      slackr_msg(slack_msg, mrkdwn = "true")
   } else {
      for (channel in channels)
         slackr_msg(slack_msg, mrkdwn = "true", channel = channel)
   }
}

# download entire dropbox folder
remove_trailing_slashes <- function(x) gsub("/*$", "", x)

download_folder <- function(
   path,
   local_path,
   dtoken = rdrop2::drop_auth(),
   unzip = TRUE,
   overwrite = FALSE,
   progress = interactive(),
   verbose = interactive()
) {
   if (unzip && dir.exists(local_path))
      stop("a directory already exists at ", local_path)
   if (!unzip && file.exists(local_path))
      stop("a file already exists at ", local_path)

   path              <- remove_trailing_slashes(path)
   local_path        <- remove_trailing_slashes(local_path)
   local_parent      <- dirname(local_path)
   original_dir_name <- basename(path)
   download_path     <- if (unzip) tempfile("dir") else local_path

   if (!dir.exists(local_parent)) stop("target parent directory ", local_parent, " not found")

   url <- "https://content.dropboxapi.com/2/files/download_zip"
   req <- httr::POST(
      url = url,
      httr::config(token = dtoken),
      httr::add_headers(
         `Dropbox-API-Arg` = jsonlite::toJSON(list(path = paste0("/", path)),
                                              auto_unbox = TRUE)),
      if (progress) httr::progress(),
      httr::write_disk(download_path, overwrite)
   )
   httr::stop_for_status(req)
   if (verbose) {
      size        <- file.size(download_path)
      class(size) <- "object_size"
      base::message(sprintf("Downloaded %s to %s: %s on disk", path,
                            download_path, format(size, units = "auto")))
   }
   if (unzip) {
      if (verbose) base::message("Unzipping file...")
      new_dir_name <- basename(local_path)
      unzip_path   <- tempfile("dir")
      unzip(download_path, exdir = unzip_path)
      file.rename(file.path(unzip_path, original_dir_name),
                  file.path(unzip_path, new_dir_name))
      file.copy(file.path(unzip_path, new_dir_name),
                local_parent,
                recursive = TRUE)
   }

   TRUE
}

# function to extract information from pdf in sections
pdf_section <- function(data, x_seq, y_seq) {
   df <- data %>%
      filter(
         x %in% x_seq,
         y %in% y_seq
      ) %>%
      group_by(y) %>%
      summarise(
         text = paste0(collapse = " ", text)
      )
   return(df)
}

# ohasis patient_id
oh_px_id <- function(db_conn = NULL, faci_id = NULL) {
   letter <- substr(stri_rand_shuffle(paste(collapse = "", LETTERS[seq_len(130)])), 1, 1)
   number <- substr(stri_rand_shuffle(strrep("0123456789", 5)), 1, 3)

   randomized <- stri_rand_shuffle(paste0(letter, number))
   patient_id <- paste0(format(Sys.time(), "%Y%m%d"), faci_id, randomized)

   pid_query <- dbSendQuery(db_conn, glue("SELECT PATIENT_ID FROM `ohasis_interim`.`px_info` WHERE PATIENT_ID = '{patient_id}'"))
   pid_count <- dbFetch(pid_query)
   dbClearResult(pid_query)

   while (nrow(pid_count) > 0) {
      letter <- substr(stri_rand_shuffle(strrep(LETTERS, 5)), 1, 1)
      number <- substr(stri_rand_shuffle(strrep("0123456789", 5)), 1, 3)

      randomized <- stri_rand_shuffle(paste0(letter, number))
      patient_id <- paste0(format(Sys.time(), "%Y%m%d"), faci_id, randomized)

      pid_query <- dbSendQuery(db_conn, glue("SELECT PATIENT_ID FROM `ohasis_interim`.`px_info` WHERE PATIENT_ID = '{patient_id}'"))
      pid_count <- dbFetch(pid_query)
      dbClearResult(pid_query)
   }

   return(patient_id)
}

# clear environment function
clear_env <- function(...) {
   env <- ls(envir = .GlobalEnv)
   if (!exists("currEnv", envir = .GlobalEnv))
      currEnv <- env[env != "currEnv"]

   exclude <- as.character(match.call(expand.dots = FALSE)$`...`)

   remove <- setdiff(env, exclude)
   remove <- setdiff(remove, lsf.str(envir = .GlobalEnv))
   remove <- setdiff(remove, .protected)
   rm(list = remove, envir = .GlobalEnv)
}

remove_code <- function(var) {
   if_else(
      condition = !is.na(var) & stri_detect_fixed(var, '_'),
      true      = substr(var, stri_locate_first_fixed(var, '_') + 1, nchar(var)),
      false     = var
   )
}

keep_code <- function(var) {
   if_else(
      condition = !is.na(var) & stri_detect_fixed(var, '_'),
      true      = substr(var, 1, stri_locate_first_fixed(var, '_') - 1),
      false     = var
   )
}

get_names <- function(parent, pattern = NULL) {
   if (!is.null(pattern))
      names(parent)[grepl(pattern, names(parent))]
   else
      names(parent)
}

# query builder and download tracker for db tables
dbTable <- function(conn, dbname, table, cols = NULL, where = NULL, join = NULL, raw_where = FALSE, name = NULL) {
   # get alias
   tbl_alias <- table
   if (stri_detect_fixed(table, " AS "))
      tbl_alias <- substr(table, stri_locate_first_fixed(table, " AS ") + 4, nchar(table))

   # if cols defined, limit to columns
   if (!is.null(cols))
      cols <- paste(collapse = ", ", paste0(tbl_alias, ".", cols))
   else
      cols <- paste0(tbl_alias, ".*")

   # if to limit number of rows based on conditions
   rows <- ""
   if (!is.null(where)) {
      if (raw_where == TRUE) {
         rows <- glue("WHERE {where}")
      } else {
         where_txt <- ""
         for (i in seq_len(length(where)))
            where_txt[i] <- paste(sep = " = ", names(where)[i], where[i])

         rows <- paste0("WHERE ", paste(collapse = " AND ", where_txt))
      }
   }

   # if to join on tables
   join_tbls <- ""
   join_txt  <- ""
   join_cols <- ""
   if (!is.null(join)) {
      for (i in seq_len(length(join))) {
         join_alias <- strsplit(names(join)[[i]], "\\.")[[1]][2]
         if (stri_detect_fixed(names(join)[[i]], " AS "))
            join_alias <- substr(names(join)[[i]], stri_locate_first_fixed(names(join)[[i]], " AS ") + 4, nchar(names(join)[[i]]))

         # id columns to join by/on
         join_by <- ""
         for (j in seq_len(length(join[[i]]$by)))
            join_by[j] <- paste(sep = " = ", paste0(tbl_alias, ".", names(join[[i]]$by)[j]), paste0(join_alias, ".", join[[i]]$by[j]))

         # columns to get from joined tables
         join_get <- ""
         for (j in seq_len(length(join[[i]]$cols)))
            join_get[j] <- paste0(join_alias, ".", join[[i]]$cols[j])

         join_txt[i]  <- paste0(toupper(ifelse(!is.null(join[[i]]$type), join[[i]]$type, "left")), " JOIN ", names(join)[[i]], " ON ", paste(collapse = " AND ", join_by))
         join_cols[i] <- paste(collapse = ", ", join_get)
      }

      join_tbls <- paste(collapse = "\n", join_txt)
   }

   if (join_cols != "")
      cols <- paste(collapse = ", ", c(cols, join_cols))

   # build query using previous params
   query <- glue(r"(SELECT {cols} FROM {dbname}.{table} {join_tbls} {rows};)")

   # get number of affected rows
   data   <- tibble()
   n_rows <- dbGetQuery(conn, glue(r"(SELECT COUNT(*) FROM {dbname}.{table} {join_tbls} {rows};)"))
   n_rows <- as.numeric(n_rows[1,])

   # get actual result set
   rs <- dbSendQuery(conn, query)

   chunk_size <- 1000
   if (n_rows >= chunk_size) {
      # upload in chunks to monitor progress
      n_chunks <- ceiling(n_rows / chunk_size)

      # get progress
      if (!is.null(name))
         pb_name <- paste0(name, ": :current of :total chunks [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed")
      else
         pb_name <- ":current of :total chunks [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed"

      pb <- progress_bar$new(format = pb_name, total = n_chunks, width = 100, clear = FALSE)
      pb$tick(0)

      # fetch in chunks
      for (i in seq_len(n_chunks)) {
         chunk <- dbFetch(rs, chunk_size)
         data  <- bind_rows(data, chunk)
         pb$tick(1)
      }
   } else {
      data <- dbFetch(rs)
   }
   dbClearResult(rs)
   return(data)
}

# get inventory data
get_inv <- function(iid) {
   inv        <- list()
   db_conn    <- ohasis$conn("db")
   inv$status <- dbGetQuery(db_conn, r"(
SELECT inv.INVENTORY_ID,
       prod.NAME          AS ITEM,
       inv.BATCH_NUM,
       inv.BATCH_QUANTITY AS BATCH_INITIAL,
       inv.BATCH_CURR,
       batch.DEFINITION   AS BATCH_UNIT,
       inv.ITEM_PER_BATCH,
       inv.ITEM_QUANTITY  AS ITEM_INITIAL,
       inv.ITEM_CURR,
       item.DEFINITION    AS ITEM_UNIT,
       inv.RECORD_DATE    AS DATE_RECEIVE,
       inv.EXPIRE_DATE    AS DATE_EXPIRE,
       inv.FACI_ID,
       ''                 AS SUB_FACI_ID
FROM ohasis_interim.inventory AS inv
         LEFT JOIN ohasis_interim.inventory_product AS prod ON inv.ITEM_ID = prod.ITEM
         LEFT JOIN ohasis_interim.ref_text AS batch ON inv.BATCH_UNIT = batch.VALUE AND batch.NAME = 'STOCK_UNIT'
         LEFT JOIN ohasis_interim.ref_text AS item ON inv.ITEM_UNIT = item.VALUE AND item.NAME = 'ITEM_UNIT'
WHERE inv.INVENTORY_ID = ?
)", params = iid)


   trxn      <- list()
   trxn$data <- dbGetQuery(db_conn, r"(
SELECT trans.*,
       IF(ISNULL(rec.DELETED_AT), 0, 1) AS INVALID
FROM ohasis_interim.inventory_transact AS trans
         LEFT JOIN ohasis_interim.px_record AS rec ON trans.TRANSACT_ID = rec.REC_ID
WHERE trans.INVENTORY_ID = ?
)", params = iid)
   trxn$add  <- trxn$data %>%
      filter(
         TRANSACT_TYPE == 1,
         INVALID == 0
      ) %>%
      group_by(UNIT_BASIS) %>%
      summarise(
         TOTAL = sum(TRANSACT_QUANTITY)
      ) %>%
      ungroup() %>%
      mutate(
         TOTAL = if_else(
            condition = UNIT_BASIS == 1,
            true      = TOTAL * inv$status$ITEM_PER_BATCH,
            false     = TOTAL,
            missing   = TOTAL
         )
      ) %>%
      summarise(
         TOTAL = sum(TOTAL)
      )

   trxn$subtract <- trxn$data %>%
      filter(
         TRANSACT_TYPE == 2,
         INVALID == 0
      ) %>%
      group_by(UNIT_BASIS) %>%
      summarise(
         TOTAL = sum(TRANSACT_QUANTITY)
      ) %>%
      ungroup() %>%
      mutate(
         TOTAL = if_else(
            condition = UNIT_BASIS == 1,
            true      = TOTAL * inv$status$ITEM_PER_BATCH,
            false     = TOTAL,
            missing   = TOTAL
         )
      ) %>%
      summarise(
         TOTAL = sum(TOTAL)
      )

   inv$status <- ohasis$get_faci(
      inv$status,
      list("FACI" = c("FACI_ID", "SUB_FACI_ID")),
      "name"
   )

   dbDisconnect(db_conn)
   inv$trxn <- trxn
   return(inv)
}

update_inv <- function(iid) {
   inv <- get_inv(iid)

   remain_item  <- inv$trxn$add$TOTAL - inv$trxn$subtract$TOTAL
   remain_batch <- (remain_item / inv$status$ITEM_PER_BATCH)

   db_conn <- ohasis$conn("db")
   dbExecute(
      db_conn,
      r"(
UPDATE ohasis_interim.inventory
SET ITEM_CURR  = ?,
    BATCH_CURR = ?
WHERE INVENTORY_ID = ?
)",
      params = list(remain_item, remain_batch, iid)
   )
   dbDisconnect(db_conn)
}

chunk_df <- function(data = NULL, chunk_size = NULL) {
   # upsert data
   n_rows     <- nrow(data)
   n_chunks   <- rep(1:ceiling(n_rows / chunk_size), each = chunk_size)[seq_len(n_rows)]
   chunked    <- split(data, n_chunks)

   return(chunked)
}