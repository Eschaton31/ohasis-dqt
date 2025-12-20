tracked_select <- function(conn, query, name, params = NULL) {
   # get number of affected rows
   data   <- tibble()
   n_rows <- dbGetQuery(conn, glue(r"(SELECT COUNT(*) FROM ({gsub(';', '', query)}) AS tbl)"), params = params)
   n_rows <- as.numeric(n_rows[1,])

   # get actual result set

   .log_info("Reading {green(name)}.")
   if (class(conn)[1] != 'ClickHouseHTTPConnection') {
      chunk_size <- 1000
      rs         <- dbSendQuery(conn, query, params = params)

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
   } else {
      data <- suppress_warnings(dbGetQuery(conn, query, params = params, format = 'TabSeparatedWithNamesAndTypes'), 'Unsupported') %>%
         mutate_if(
            ~("IDate" %in% class(.)),
            ~as.Date(.)
         ) %>%
         mutate_if(
            is.character,
            ~na_if(str_replace_all(., "\\\\0", ""), "")
         ) %>%
         rename_all(
            ~case_when(
               str_detect(., "\\.") ~ str_extract(., ".+\\.(.+)", 1),
               TRUE ~ .
            )
         )
   }
   return(data)
}

# update db duplicate rec_ids
change_rec_id <- function(pid, old_recid, new_recid) {
   db_conn <- ohasis$conn("db")

   upd_by <- Sys.getenv("OH_USER_ID")
   upd_at <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
   dbExecute(
      db_conn,
      r"(UPDATE ohasis_interim.px_name SET REC_ID = ?, UPDATED_BY = ?, UPDATED_AT = ? WHERE PATIENT_ID = ? AND REC_ID = ?;)",
      params = list(new_recid, upd_by, upd_at, pid, old_recid)
   )
   dbExecute(
      db_conn,
      r"(UPDATE ohasis_interim.px_info SET REC_ID = ?, UPDATED_BY = ?, UPDATED_AT = ? WHERE PATIENT_ID = ? AND REC_ID = ?;)",
      params = list(new_recid, upd_by, upd_at, pid, old_recid)
   )
   dbExecute(
      db_conn,
      r"(UPDATE ohasis_interim.px_record SET REC_ID = ?, UPDATED_BY = ?, UPDATED_AT = ? WHERE PATIENT_ID = ? AND REC_ID = ?;)",
      params = list(new_recid, upd_by, upd_at, pid, old_recid)
   )
   dbDisconnect(db_conn)
}

# update db duplicate rec_ids
change_px_id <- function(recids, new_pid) {
   db_conn <- connect('ohasis-live')

   upd_by <- Sys.getenv("OH_USER_ID")
   upd_at <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
   # dbExecute(
   #    db_conn,
   #    r"(update ohasis.px_record set patient_id = ?, updated_by = ?, updated_at = ? WHERE rec_id = ? AND patient_id = ?;)",
   #    params = list(new_pid, upd_by, upd_at, recid, old_pid)
   # )
   data   <- tibble(rec_id = recids) %>%
      mutate(
         patient_id = new_pid,
         updated_by = upd_by,
         updated_at = upd_at
      )

   dbxUpsert(db_conn, Id(schema = 'ohasis', table = 'px_record'), data, where_cols = 'rec_id')

   dbDisconnect(db_conn)
}

apply_pii_to_patient <- function(rid, pid) {
   conn <- connect('ohasis-live')

   patients  <- QB$new(conn)$from('ohasis.patients')$where('patient_id', pid)$get()
   px_record <- QB$new(conn)$from('ohasis.px_record')$whereIn('rec_id', rid)$get()
   px_pii    <- QB$new(conn)$from('ohasis.px_pii')$whereIn('rec_id', rid)$get()

   new_pii <- px_record %>%
      select(
         rec_id,
         patient_id,
         faci_id,
         sub_faci_id,
         record_date,
         patient_id,
         created_by,
         created_at,
         updated_by,
         updated_at,
         deleted_by,
         deleted_at,
      ) %>%
      left_join(px_pii %>% select(rec_id, any_of(names(patients))), join_by(rec_id)) %>%
      select(-ends_with('.y')) %>%
      rename_all(~str_replace(., '\\.x$', '')) %>%
      mutate(
         age        = calc_age(birthdate, record_date),
         updated_by = '1300000001',
         updated_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S")
      ) %>%
      select(any_of(names(patients))) %>%
      distinct(patient_id, .keep_all = TRUE)

   dbxUpsert(conn, Id(schema = 'ohasis', table = 'patients'), new_pii, 'patient_id')
   dbDisconnect(conn)
}

# update UPDATED_*
update_credentials <- function(rec_ids) {
   db_conn <- ohasis$conn("db")

   upd_by <- Sys.getenv("OH_USER_ID")
   upd_at <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
   dbExecute(
      db_conn,
      glue(r"(update ohasis.px_record set updated_by = '{upd_by}', updated_at = '{upd_at}' where rec_id in (?);)"),
      params = list(rec_ids)
   )
   dbDisconnect(db_conn)
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

   if (paste(join_cols, collapse = ", ") != "")
      cols <- paste(collapse = ", ", append(cols, join_cols))

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
      if (is.null(name))
         name <- table

      pb_name <- paste0(name, ": :current of :total chunks [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed")
      pb      <- progress_bar$new(format = pb_name, total = n_chunks, width = 100, clear = FALSE)
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

dbTable2 <- function(conn, dbname, table, cols = ..1, where = ..2, join = NULL, raw_where = FALSE, name = NULL) {
   # get ellipsis arguments
   ellipsis <- match.call(expand.dots = FALSE)
   cols     <- ifelse(!is.null(ellipsis$cols), deparse(ellipsis$cols), NA)
   where    <- ifelse(!is.null(ellipsis$where), deparse(ellipsis$where), NA)

   # define params to be used in sql
   sql_cols  <- ""
   sql_from  <- ""
   sql_join  <- ""
   sql_where <- ""

   # get alias
   tbl_alias <- names(table)
   tbl_name  <- ifelse(!is.null(tbl_alias), paste0(table, " AS ", tbl_alias), table)
   tbl_name  <- ifelse(!grepl(dbname, tbl_name), paste0(dbname, ".", tbl_name), tbl_name)
   tbl_alias <- ifelse(is.null(tbl_alias), table, tbl_alias)
   sql_from  <- paste0("FROM ", tbl_name)

   # if cols defined, limit to columns
   if (!is.na(cols)) {
      cols     <- ifelse(str_left(cols, stri_locate_first_fixed(cols, "(")) == "list(",
                         substr(cols, 6, nchar(cols) - 1),
                         substr(cols, 3, nchar(cols) - 1))
      cols     <- str_split(cols, ", ")[[1]]
      cols     <- stri_replace_all_fixed(cols, intToUtf8(34), "")
      cols     <- ifelse(grepl("\\.", cols), cols, paste0(tbl_alias, ".", cols))
      sql_cols <- paste(collapse = ", \n", cols)
   } else {
      sql_cols <- "*"
   }

   # if to limit number of rows based on conditions
   if (!is.na(where)) {
      if (raw_where == TRUE) {
         sql_where <- paste0("WHERE ", where)
      } else {
         where     <- ifelse(str_left(where, stri_locate_first_fixed(where, "(")) == "list(",
                             substr(where, 6, nchar(where) - 1),
                             substr(where, 3, nchar(where) - 1))
         where     <- str_split(where, ", ")[[1]]
         where     <- ifelse(grepl("\\.", where), where, paste0(tbl_alias, ".", where))
         where     <- paste0("(", where, ")")
         where     <- stri_replace_all_fixed(where, "|", " OR ")
         where     <- stri_replace_all_fixed(where, "&", " AND ")
         where     <- stri_replace_all_fixed(where, "==", " = ")
         where     <- stri_replace_all_fixed(where, "!=", " <> ")
         where     <- str_squish(where)
         sql_where <- paste0("WHERE ", paste(collapse = " AND ", where))
      }
   }

   # if to join on tables
   # join list structure is expected to be:
   # list(
   #    join_type = list(table, on)
   # )
   if (!is.null(join)) {
      for (i in seq_len(length(join))) {
         join_type <- names(join)[i]
         join_type <- switch(join_type,
                             left_join  = "LEFT JOIN ",
                             right_join = "RIGHT JOIN ",
                             inner_join = "JOIN ")

         join_table <- join[[i]]$table
         join_alias <- ifelse(!is.null(names(join_table)),
                              paste0(join_table, " AS ", names(join_table)),
                              join_table)
         join_alias <- ifelse(stri_count_fixed(join_alias, ".") == 0, paste0(dbname, ".", join_alias), join_alias)

         # id columns to join by/on
         join_on <- join[[i]]$on
         for (j in seq_len(length(join_on))) {
            join_col <- ifelse(grepl("\\.", names(join_on)[j]), names(join_on)[j], paste0(join_alias, ".", names(join_on)[j]))
            join_on  <- paste0(join_col, " = ", join_on[j])
         }

         sql_join[i] <- paste0(join_type, join_alias, " ON ", paste(collapse = " AND ", join_on))
      }
      sql_join <- paste0(collapse = " \n", sql_join)
   }

   # build query using previous params
   query_table <- paste("SELECT", sql_cols, sql_from, sql_join, sql_where)
   query_nrow  <- paste("SELECT COUNT(*) AS nrow", sql_from, sql_join, sql_where)

   # get number of affected rows
   data   <- tibble()
   n_rows <- dbGetQuery(conn, query_nrow)$nrow
   n_rows <- ifelse(length(n_rows) > 1, length(n_rows), as.numeric(n_rows))
   n_rows <- as.integer(n_rows)

   # get actual result set
   .log_info("Number of records to fetch = {green(formatC(n_rows, big.mark = ','))}.")
   rs <- dbSendQuery(conn, query_table)

   chunk_size <- 1000
   if (n_rows >= chunk_size) {
      # upload in chunks to monitor progress
      n_chunks <- ceiling(n_rows / chunk_size)

      # get progress
      pb_name <- paste0(table, ": :current of :total chunks [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed")

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
   db_conn    <- connect('ohasis-live')
   inv$status <- QB$new(db_conn)$
      from('ohasis.inventories as inv')$
      leftJoin('ohasis.products as prod', 'inv.item_id', '=', 'prod.product_id')$
      where('inv.inventory_id', iid)$
      select('inv.inventory_id',
             'prod.name as item',
             'inv.batch_num',
             'inv.batch_quantity',
             'inv.batch_curr',
             'inv.item_per_batch',
             'inv.item_quantity',
             'inv.item_curr',
             'inv.receipt_date',
             'inv.expire_date',
             'inv.faci_id')$
      get()

   trxn      <- list()
   trxn$data <- QB$new(db_conn)$
      from('ohasis.inventory_transactions as trans')$
      select('trans.*')$
      leftJoin('ohasis.px_record as rec', 'trans.rec_id', '=', 'rec.rec_id')$
      whereNull('rec.deleted_at')$
      where('trans.inventory_id', iid)$
      get()

   trxn$add <- trxn$data %>%
      filter(
         transact_type == 1,
      ) %>%
      group_by(unit_basis) %>%
      summarise(
         total = sum(transact_quantity, na.rm = TRUE)
      ) %>%
      ungroup() %>%
      mutate(
         total = if_else(
            condition = unit_basis == 1,
            true      = total * inv$status$item_per_batch,
            false     = total,
            missing   = total
         )
      ) %>%
      summarise(
         total = coalesce(sum(total, na.rm = TRUE), 0)
      )

   trxn$subtract <- trxn$data %>%
      filter(
         transact_type == 2,
      ) %>%
      group_by(unit_basis) %>%
      summarise(
         total = sum(transact_quantity, na.rm = TRUE)
      ) %>%
      ungroup() %>%
      mutate(
         total = if_else(
            condition = unit_basis == 1,
            true      = total * inv$status$item_per_batch,
            false     = total,
            missing   = total
         )
      ) %>%
      summarise(
         total = coalesce(sum(total, na.rm = TRUE), 0)
      )

   inv$status <- ohasis$get_faci(
      inv$status,
      list("faci" = c("faci_id", "sub_faci_id")),
      "name"
   )

   dbDisconnect(db_conn)
   inv$trxn <- trxn
   return(inv)
}

update_inv <- function(iid) {
   inv <- get_inv(iid)

   remain_item  <- inv$trxn$add$total - inv$trxn$subtract$total
   remain_batch <- (remain_item / inv$status$item_per_batch)

   db_conn <- connect('ohasis-live')
   new_inv <- inv$status %>%
      select(
         inventory_id,
      ) %>%
      mutate(
         item_quantity = inv$trxn$add$total,
         item_curr     = remain_item,
         batch_curr    = remain_batch,
         updated_by    = Sys.getenv("OH_USER_ID"),
         updated_at    = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      )
   dbxUpsert(db_conn, Id(schema = 'ohasis', table = 'inventories'), new_inv, 'inventory_id')
   dbDisconnect(db_conn)
}


update_disp_date <- function(data) {
   rec_ids <- data$REC_ID

   db_conn <- ohasis$conn("db")

   log_info("Downloading live data.")
   px_medicine        <- dbxSelect(db_conn, "SELECT * FROM ohasis_interim.px_medicine WHERE REC_ID IN (?)", params = list(rec_ids))
   inventory_transact <- dbxSelect(db_conn, "SELECT * FROM ohasis_interim.inventory_transact WHERE TRANSACT_ID IN (?)", params = list(rec_ids))
   inventory_product  <- dbxSelect(db_conn, "SELECT * FROM ohasis_interim.inventory_product")

   inv_ids <- inventory_transact$INVENTORY_ID

   # correct data
   log_info("Processing {green('px_medicine')}.")
   px_medicine %<>%
      mutate_at(
         .vars = vars(MEDICINE, DISP_NUM, DISP_DATE),
         ~as.character(.)
      ) %>%
      inner_join(
         y  = data %>%
            select(
               REC_ID   = 1,
               MEDICINE = 2,
               DISP_NUM = 3,
               CORRECT  = 4
            ),
         by = join_by(REC_ID, MEDICINE, DISP_NUM)
      ) %>%
      left_join(
         y  = inventory_product %>%
            mutate(
               ITEM          = as.character(ITEM),
               TYPICAL_BATCH = as.integer(TYPICAL_BATCH)
            ) %>%
            select(
               MEDICINE = ITEM,
               TYPICAL_BATCH
            ),
         by = join_by(MEDICINE)
      ) %>%
      mutate(
         DISP_DATE   = if_else(!is.na(CORRECT), CORRECT, DISP_DATE, DISP_DATE),
         TOTAL_PILLS = if_else(UNIT_BASIS == 1, DISP_TOTAL * coalesce(TYPICAL_BATCH, 1), DISP_TOTAL, DISP_TOTAL) + MEDICINE_LEFT,
         TOTAL_DAYS  = TOTAL_PILLS / PER_DAY,
         NEXT_DATE   = as.character(as.Date(DISP_DATE) %m+% days(as.integer(TOTAL_DAYS)))
      ) %>%
      select(-TYPICAL_BATCH, -CORRECT, -TOTAL_PILLS, -TOTAL_DAYS)

   log_info("Processing {green('inventory_transact')}.")
   inventory_transact %<>%
      mutate_at(
         .vars = vars(TRANSACT_NUM, TRANSACT_DATE),
         ~as.character(.)
      ) %>%
      left_join(
         y  = px_medicine %>%
            select(
               TRANSACT_ID  = REC_ID,
               TRANSACT_NUM = DISP_NUM,
               BATCH_NUM,
               CORRECT      = DISP_DATE
            ),
         by = join_by(TRANSACT_ID, TRANSACT_NUM, BATCH_NUM)
      ) %>%
      mutate(
         TRANSACT_DATE = if_else(!is.na(CORRECT), CORRECT, TRANSACT_DATE, TRANSACT_DATE),
      ) %>%
      select(-CORRECT)

   # update live
   log_info("Uploading.")
   dbxUpsert(db_conn, Id(schema = "ohasis_interim", table = "px_medicine"), px_medicine, c("REC_ID", "MEDICINE", "DISP_NUM"))
   dbxUpsert(db_conn, Id(schema = "ohasis_interim", table = "inventory_transact"), inventory_transact, c("TRANSACT_ID", "TRANSACT_NUM", "INVENTORY_ID"))
   dbDisconnect(db_conn)

   log_info("Updating records.")
   update_credentials(rec_ids)
   invisible(lapply(inv_ids, update_inv))
   log_success("Done.")
}

# ohasis patient_id
oh_px_id <- function(db_conn = NULL, faci_id = NULL, date = NULL) {
   date   <- ifelse(is.null(date), format(Sys.time(), "%Y%m%d"), date)
   letter <- substr(stri_rand_shuffle(paste(collapse = "", LETTERS[seq_len(130)])), 1, 1)
   number <- substr(stri_rand_shuffle(strrep("0123456789", 5)), 1, 3)

   randomized <- stri_rand_shuffle(paste0(letter, number))
   patient_id <- paste0(date, faci_id, randomized)

   pid_query <- dbSendQuery(db_conn, glue("SELECT DISTINCT PATIENT_ID FROM `ohasis_interim`.`px_info` WHERE PATIENT_ID = '{patient_id}'"))
   pid_count <- dbFetch(pid_query)
   dbClearResult(pid_query)

   while (nrow(pid_count) > 0) {
      letter <- substr(stri_rand_shuffle(paste(collapse = "", LETTERS[seq_len(130)])), 1, 1)
      number <- substr(stri_rand_shuffle(strrep("0123456789", 5)), 1, 3)

      randomized <- stri_rand_shuffle(paste0(letter, number))
      patient_id <- paste0(date, faci_id, randomized)

      pid_query <- dbSendQuery(db_conn, glue("SELECT DISTINCT PATIENT_ID FROM `ohasis_interim`.`px_info` WHERE PATIENT_ID = '{patient_id}'"))
      pid_count <- dbFetch(pid_query)
      dbClearResult(pid_query)
   }

   return(patient_id)
}

# ohasis rec_id
oh_rec_id <- function(db_conn = NULL, user_id = NULL) {
   letter <- substr(stri_rand_shuffle(paste(collapse = "", LETTERS[seq_len(130)])), 1, 1)
   number <- substr(stri_rand_shuffle(strrep("0123456789", 5)), 1, 2)

   randomized <- stri_rand_shuffle(paste0(letter, number))
   record_id  <- paste0(format(Sys.time(), "%Y%m%d%H%M"), randomized, user_id)

   rid_query <- dbSendQuery(db_conn, glue("SELECT REC_ID FROM `ohasis_interim`.`px_record` WHERE REC_ID = '{record_id}'"))
   rid_count <- dbFetch(rid_query)
   dbClearResult(rid_query)

   while (nrow(rid_count) > 0) {
      letter <- substr(stri_rand_shuffle(strrep(LETTERS, 5)), 1, 1)
      number <- substr(stri_rand_shuffle(strrep("0123456789", 5)), 1, 3)

      randomized <- stri_rand_shuffle(paste0(letter, number))
      record_id  <- paste0(format(Sys.time(), "%Y%m%d%H%M"), randomized, user_id)

      rid_query <- dbSendQuery(db_conn, glue("SELECT PATIENT_ID FROM `ohasis_interim`.`px_record` WHERE REC_ID = '{patient_id}'"))
      rid_count <- dbFetch(rid_query)
      dbClearResult(rid_query)
   }

   return(record_id)
}

oh_faci_id <- function(db_conn = NULL, region = NULL, exclude = NULL) {
   query_ref <- r"(
   SELECT RIGHT(FACI_ID, 4) AS CTRL_NUM
   FROM ohasis_interim.facility
   WHERE FACI_ID REGEXP ?
   UNION
   SELECT RIGHT(DUPE_FACI, 4) AS CTRL_NUM
   FROM ohasis_interim.facility_duplicates
   WHERE DUPE_FACI REGEXP ?
   )"

   log_info("Constructing new code.")
   region_code   <- str_left(region, 2)
   data_ref      <- dbGetQuery(db_conn, query_ref, params = list(stri_c("^", region_code), stri_c("^", region_code)))
   ctrl_num_curr <- as.integer(data_ref[[1]])
   if (!is.null(exclude)) {
      ctrl_num_curr <- c(ctrl_num_curr, as.integer(str_right(exclude[!is.na(exclude)], 4)))
   }

   if (nrow(data_ref) == 0)
      ctrl_num_curr <- 0

   ctrl_num_seq       <- seq(min(ctrl_num_curr), max(ctrl_num_curr))
   ctrl_num_available <- setdiff(ctrl_num_seq, ctrl_num_curr)

   if (length(ctrl_num_available) == 0) {
      log_warn("Unused control number/s available.")
      ctrl_num_ref <- max(ctrl_num_curr) + 1
   } else {
      log_info("No unused control numbers found.")
      ctrl_num_ref <- min(ctrl_num_available)
   }
   log_info("Using next in sequence.")
   ctrl_num_new <- stri_pad_left(ctrl_num_ref, 4, "0")

   faci_id <- stri_c(sep = "", region_code, ctrl_num_new)
   log_success("New Confirmatory Code: {green(faci_id)}.")

   return(faci_id)
}

batch_px_ids <- function(data, px_id, faci_id, row_ids) {
   pid_col <- deparse(substitute(px_id))
   set.seed(1)
   data %<>%
      select(-matches(pid_col)) %>%
      mutate(
         seed_faci = {{faci_id}}
      )

   gen_pid <- function(faci_id, record_date = NULL) {
      letters <- stri_c(collapse = "", strrep(LETTERS[1:26], 5))
      numbers <- strrep("0123456789", 5)

      letters <- stri_rand_shuffle(letters)
      letter  <- str_left(letters, 1)
      numbers <- stri_rand_shuffle(numbers)
      number  <- str_left(numbers, 3)

      date <- Sys.time()
      if (!is.null(record_date)) {
         date <- as.Date(record_date)
      }

      ohasis_id <- stri_c(letter, number)
      ohasis_id <- stri_rand_shuffle(ohasis_id)
      ohasis_id <- stri_c(format(date, "%Y%m%d"), faci_id, ohasis_id)

      return(ohasis_id)
   }

   get_issues <- function(data, conn) {
      dupes   <- get_dupes(data, ohasis_id)
      already <- data %>%
         inner_join(dbxSelect(conn, "select patient_id as ohasis_id from ohasis.patients where patient_id in (?)", list(data$ohasis_id)), join_by(ohasis_id))

      issues <- dupes %>%
         select(-dupe_count) %>%
         bind_rows(already) %>%
         distinct_all()

      log_info("Duplicate Patient IDs = {green(nrow(issues))}.")

      return(issues)
   }

   db_conn <- ohasis$conn("db")

   if ('record_date' %in% names(data)) {
      data %<>%
         rowwise() %>%
         mutate(
            ohasis_id = gen_pid(seed_faci, record_date)
         ) %>%
         ungroup()
   } else {
      data %<>%
         rowwise() %>%
         mutate(
            ohasis_id = gen_pid(seed_faci)
         ) %>%
         ungroup()
   }

   issues <- get_issues(data, db_conn)
   while (nrow(issues) > 0) {
      if ('record_date' %in% names(data)) {
         new <- issues %>%
            rowwise() %>%
            mutate(
               ohasis_id = gen_pid(seed_faci, record_date)
            ) %>%
            ungroup()
      } else {
         new <- issues %>%
            rowwise() %>%
            mutate(
               ohasis_id = gen_pid(seed_faci)
            ) %>%
            ungroup()
      }
      new %<>%
         select(all_of(row_ids), new_oh = ohasis_id)

      data %<>%
         left_join(new, by = row_ids, na_matches = "never") %>%
         mutate(
            ohasis_id = coalesce(new_oh, ohasis_id)
         ) %>%
         select(-new_oh)

      issues <- get_issues(data, db_conn)
   }
   dbDisconnect(db_conn)

   data %<>%
      select(-seed_faci) %>%
      rename(
         {{px_id}} := ohasis_id
      )

   return(data)
}

batch_rec_ids <- function(data, rec_id, user_id, row_ids) {
   rid_col <- deparse(substitute(rec_id))
   set.seed(1)
   data %<>%
      select(-matches(rid_col)) %>%
      mutate(
         creds_id = {{user_id}}
      )

   gen_rid <- function(creds_id) {
      letters <- stri_c(collapse = "", strrep(LETTERS[1:26], 5))
      numbers <- strrep("0123456789", 5)

      letters <- stri_rand_shuffle(letters)
      letter  <- str_left(letters, 2)
      numbers <- stri_rand_shuffle(numbers)
      number  <- str_left(numbers, 3)

      rec_id <- stri_c(letter, number)
      rec_id <- stri_rand_shuffle(rec_id)
      rec_id <- stri_c(format(Sys.time(), "%Y%m%d%H"), rec_id, creds_id)

      return(rec_id)
   }

   get_issues <- function(data, conn) {
      dupes   <- get_dupes(data, record_id)
      already <- data %>%
         inner_join(dbxSelect(conn, "select rec_id as record_id from ohasis.px_record where rec_id in (?)", list(data$record_id)), join_by(record_id))

      issues <- dupes %>%
         select(-dupe_count) %>%
         bind_rows(already) %>%
         distinct_all()

      log_info("Duplicate REC_IDs = {green(nrow(issues))}.")

      return(issues)
   }

   db_conn <- ohasis$conn("db")
   data %<>%
      rowwise() %>%
      mutate(
         record_id = gen_rid(creds_id)
      ) %>%
      ungroup()
   issues <- get_issues(data, db_conn)
   while (nrow(issues) > 0) {
      new <- issues %>%
         rowwise() %>%
         mutate(
            record_id = gen_rid(creds_id)
         ) %>%
         ungroup() %>%
         select(all_of(row_ids), new_rec = record_id)
      data %<>%
         left_join(new, by = row_ids, na_matches = "never") %>%
         mutate(
            record_id = coalesce(new_rec, record_id)
         ) %>%
         select(-new_rec)

      issues <- get_issues(data, db_conn)
   }
   dbDisconnect(db_conn)

   data %<>%
      select(-creds_id) %>%
      rename(
         {{rec_id}} := record_id
      )

   return(data)
}


dup_faci_id <- function(keep_faci, drop_faci, reason = NA_character_) {
   # get dupes
   dupes <- ohasis$ref_faci %>%
      filter(faci_id %in% c(keep_faci, drop_faci)) %>%
      filter(sub_faci_id == "") %>%
      mutate(
         reason     = reason,
         main_faci  = keep_faci,
         created_by = '1300000001',
         created_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      ) %>%
      select(
         main_faci,
         dupe_faci       = faci_id,
         reason,
         faci_name,
         faci_name_clean = faci_name_nhsss,
         faci_code,
         pubpriv         = ownership,
         lat             = latitude,
         long            = longitude,
         email,
         mobile,
         landline,
         reg             = addr_psgc_reg,
         prov            = addr_psgc_prov,
         munc            = addr_psgc_munc,
         address         = physical_address,
      )

   #  prepare update & select queries queries per table
   sql_update <- list()
   sql_select <- list()
   table_cols <- list(
      "patients.faci_id",
      "px_cfbs.faci_id",
      "px_cfbs.partner_faci",
      "px_confirm.faci_id",
      "px_confirm.source",
      "px_service.faci_id",
      "px_service.refer_by_id",
      "px_medicine.faci_id",
      "px_medicine_disc.faci_id",
      "px_record.faci_id",
      "px_test.faci_id",
      "users.faci_id",
      "inventories.faci_id",
      "inventories.source_id"
   )
   for (table_col in table_cols) {
      pair  <- strsplit(table_col, "\\.")[[1]]
      table <- pair[1]
      col   <- pair[2]

      sql_update[[table_col]] <- paste0("update ohasis.", table, " set updated_by = ?, updated_at = now(), ", col, " = ? where ", col, " = ?;")

      if (grepl("^px", table))
         sql_select[[table_col]] <- paste0("select distinct rec_id from ohasis.", table, " where ", col, " = ?;")
   }

   # get record ids for those affected
   db_conn <- connect('ohasis-live')
   rec_ids <- data.frame()
   for (table_col in names(sql_select)) {
      log_info("Selecting = {green(table_col)}")
      query   <- sql_select[[table_col]]
      data    <- dbGetQuery(db_conn, query, params = list(drop_faci))
      rec_ids <- rec_ids %>% bind_rows(data) %>% distinct(rec_id)
   }

   # update records
   for (table_col in names(sql_update)) {
      log_info("Updating = {green(table_col)}")
      query <- sql_update[[table_col]]
      dbExecute(db_conn, query, params = list('1300000001', keep_faci, drop_faci))
   }
   update_credentials(rec_ids$rec_id)

   # remove duplicate facility from referece data
   log_info("Deleting profile.")
   dbExecute(db_conn, "delete from ohasis.facilities where faci_id = ?;", params = list(drop_faci))

   # log data in facility_duplicates
   dbxUpsert(db_conn,
             Id(schema = "ohasis", table = "facility_duplicates"),
             dupes,
             c("main_faci", "dupe_faci"))
   dbDisconnect(db_conn)
}

subunit_to_faci <- function(subunit, to_faci) {
   sql_update <- list()
   sql_select <- list()
   table_cols <- list(
      "patients.sub_faci_id,faci_id",
      "px_cfbs.sub_faci_id,faci_id",
      "px_confirm.sub_faci_id,faci_id",
      "px_confirm.sub_source,source",
      "px_service.sub_faci_id,faci_id",
      "px_medicine.sub_faci_id,faci_id",
      "px_medicine_disc.sub_faci_id,faci_id",
      "px_record.sub_faci_id,faci_id",
      "px_test.sub_faci_id,faci_id"
   )
   for (table_col in table_cols) {
      pair     <- strsplit(table_col, "\\.")[[1]]
      table    <- pair[1]
      cols     <- strsplit(pair[2], ",")[[1]]
      col_main <- cols[1]
      col_sub  <- cols[2]

      sql_update[[table_col]] <- paste0("update ohasis.", table, " set updated_by = ?, updated_at = now(), ", col_main, " = ? where ", col_sub, " = ?;")

      if (grepl("^px", table))
         sql_select[[table_col]] <- paste0("select distinct rec_id from ohasis.", table, " where ", col_sub, " = ?;")
   }

   # get record ids for those affected
   db_conn <- connect('ohasis-live')
   rec_ids <- data.frame()
   for (table_col in names(sql_select)) {
      log_info("Selecting = {green(table_col)}")
      data    <- dbGetQuery(db_conn, sql_select[[table_col]], params = list(subunit))
      rec_ids <- rec_ids %>% bind_rows(data) %>% distinct(rec_id)
   }

   # update records
   for (query in sql_update) {
      dbExecute(db_conn, query, params = list('1300000001', to_faci, subunit))
   }
   update_credentials(rec_ids$rec_id)
   dbDisconnect(db_conn)
}

nullify_subunit <- function(subunit) {
   sql_update <- list()
   sql_select <- list()
   table_cols <- list(
      "patients.sub_faci_id",
      "px_cfbs.sub_faci_id",
      "px_confirm.sub_faci_id",
      "px_confirm.sub_source",
      "px_service.sub_faci_id",
      "px_medicine.sub_faci_id",
      "px_medicine_disc.sub_faci_id",
      "px_record.sub_faci_id",
      "px_test.sub_faci_id"
   )
   for (table_col in table_cols) {
      pair  <- strsplit(table_col, "\\.")[[1]]
      table <- pair[1]
      col   <- pair[2]

      sql_update[[table_col]] <- paste0("update ohasis.", table, " set updated_by = ?, updated_at = now(), ", col, " = null where ", col, " = ?;")

      if (grepl("^px", table))
         sql_select[[table_col]] <- paste0("select distinct rec_id from ohasis.", table, " where ", col, " = ?;")
   }

   # get record ids for those affected
   db_conn <- connect('ohasis-live')
   rec_ids <- data.frame()
   for (query in sql_select) {
      data    <- dbGetQuery(db_conn, query, params = list(drop_faci))
      rec_ids <- rec_ids %>% bind_rows(data) %>% distinct(rec_id)
   }

   # update records
   for (query in sql_update) {
      dbExecute(db_conn, query, params = list('1300000001', keep_faci, drop_faci))
   }
   update_credentials(rec_ids$rec_id)
   dbDisconnect(db_conn)
}

# dup_faci_id <- function(keep_faci, drop_faci, reason = NA_character_) {
#    #  prepare update & select queries queries per table
#    sql_update <- list()
#    sql_select <- list()
#    table_cols <- list(
#       "px_cfbs.faci_id",
#       "px_cfbs.partner_faci",
#       "px_confirm.faci_id",
#       "px_confirm.source",
#       "px_service.faci_id",
#       "px_service.refer_by_id",
#       "px_medicine.faci_id",
#       "px_medicine_disc.faci_id",
#       "px_record.faci_id",
#       "px_test.faci_id",
#       "users.faci_id",
#       "inventories.faci_id",
#       "inventories.source_id"
#    )
#    for (table_col in table_cols) {
#       pair  <- strsplit(table_col, "\\.")[[1]]
#       table <- pair[1]
#       col   <- pair[2]
#
#       sql_update[[table_col]] <- paste0("UPDATE ohasis.", table, " SET ", col, " = ? WHERE ", col, " = ?;")
#
#       if (grepl("^px", table))
#          sql_select[[table_col]] <- paste0("SELECT DISTINCT rec_id FROM ohasis.", table, " WHERE ", col, " = ?;")
#    }
#
#    # get record ids for those affected
#    db_conn <- connect('oh2')
#    rec_ids <- data.frame()
#    for (query in sql_select) {
#       data    <- dbGetQuery(db_conn, query, params = list(drop_faci))
#       rec_ids <- rec_ids %>% bind_rows(data) %>% distinct(rec_id)
#    }
#
#    # update records
#    for (query in sql_update) {
#       dbExecute(db_conn, query, params = list(keep_faci, drop_faci))
#    }
#
#    # remove duplicate facility from referece data
#    dbExecute(db_conn, "DELETE FROM ohasis.facilities WHERE faci_id = ?;", params = list(drop_faci))
#    dbDisconnect(db_conn)
#
#    return(rec_ids)
# }

##  update medicine
disp_bottle_to_pill <- function(rec_ids) {
   where_recs <- str_c("('", stri_c(collapse = "', '", rec_ids), "')")
   disp       <- list()
   update     <- list()
   db_conn    <- ohasis$conn("db")
   db_name    <- "ohasis_interim"

   # get records
   disp$disepesing   <- dbTable(db_conn, db_name, "px_medicine", raw_where = TRUE, where = stri_c("REC_ID IN ", where_recs))
   disp$transactions <- dbTable(db_conn, db_name, "inventory_transact", raw_where = TRUE, where = stri_c("TRANSACT_ID IN ", where_recs))

   # update with new data
   update$px_medicine <- disp$disepesing %>%
      filter(UNIT_BASIS == 1) %>%
      mutate(
         UNIT_BASIS  = 2,
         TOTAL_PILLS = DISP_TOTAL + MEDICINE_LEFT,
         TOTAL_DAYS  = TOTAL_PILLS / PER_DAY,
         NEXT_DATE   = DISP_DATE %m+% days(as.integer(TOTAL_DAYS))
      ) %>%
      select(REC_ID, MEDICINE, DISP_NUM, UNIT_BASIS, NEXT_DATE)

   update$inventory_transact <- disp$transactions %>%
      mutate(
         UNIT_BASIS = 2,
      ) %>%
      select(TRANSACT_ID, TRANSACT_NUM, INVENTORY_ID, UNIT_BASIS)

   # update live records
   table_space <- Id(schema = db_name, table = "px_medicine")
   dbxUpsert(
      db_conn,
      table_space,
      update[["px_medicine"]],
      c("REC_ID", "MEDICINE", "DISP_NUM")
   )
   table_space <- Id(schema = db_name, table = "inventory_transact")
   dbxUpsert(
      db_conn,
      table_space,
      update[["inventory_transact"]],
      c("TRANSACT_ID", "TRANSACT_NUM", "INVENTORY_ID")
   )
   dbDisconnect(db_conn)

   # update inventory to reflect changes
   lapply(disp$transactions$INVENTORY_ID, update_inv)
   update_credentials(rec_ids)
}

# changing rhivda lot_no
change_rhivda_test <- function(rec_id, test_num, new_lot_no) {
   log_info("Opening connections.")
   conn   <- ohasis$conn("db")
   dbname <- "ohasis_interim"

   log_info("Constructing filters.")
   num_test_hiv   <- switch(test_num, `1` = "31", `2` = "32", `3` = "33")
   num_transact   <- switch(test_num, `1` = "HIV RDT #1", `2` = "HIV RDT #2", `3` = "HIV RDT #3")
   where_record   <- stri_c("REC_ID = '", rec_id, "' AND TEST_TYPE = '", num_test_hiv, "' AND FINAL_RESULT <> 0")
   where_transact <- stri_c("TRANSACT_ID = '", rec_id, "' AND TRANSACT_REMARKS = '", num_transact, "'")

   # live data
   log_info("Getting live test and transaction data.")
   px_test_hiv  <- dbTable(conn, dbname, "px_test_hiv", raw_where = TRUE, where = where_record)
   transactions <- dbTable(conn, dbname, "inventory_transact", raw_where = TRUE, where = where_transact)

   # get inventory data
   log_info("Getting live inventory data.")
   faci_id         <- px_test_hiv[1,]$FACI_ID
   where_inventory <- stri_c("FACI_ID = '", faci_id, "' AND BATCH_NUM = '", new_lot_no, "' AND ITEM_CURR > 0")
   inventory       <- dbTable(conn, dbname, "inventory", raw_where = TRUE, where = where_inventory)

   # get item name
   log_info("Getting products data.")
   item_id    <- inventory[1,]$ITEM_ID
   where_item <- stri_c("ITEM = '", item_id, "'")
   items      <- dbTable(conn, dbname, "inventory_product", raw_where = TRUE, where = where_item)

   # update tables
   log_info("Updating relevant records.")
   item_name  <- items[1,]$NAME
   inv_id_old <- transactions[1,]$INVENTORY_ID
   inv_id_new <- inventory[1,]$INVENTORY_ID
   dbExecute(conn, stri_c("UPDATE ohasis_interim.inventory_transact SET BATCH_NUM = ?, INVENTORY_ID = ?, TRANSACT_NUM = 1 WHERE ", where_transact), params = list(new_lot_no, inv_id_new))
   dbExecute(conn, stri_c("UPDATE ohasis_interim.px_test_hiv SET LOT_NO = ?, KIT_NAME = ? WHERE ", where_record), params = list(new_lot_no, item_name))
   update_inv(inv_id_old)
   update_inv(inv_id_new)
   update_credentials(rec_id)

   dbDisconnect(conn)
   log_success("Done.")
}

# update error rhivda codes
change_rhivda_code <- function(rec_id) {
   # queries
   query_rec <- r"(
   SELECT test_hiv.REC_ID,
          test_hiv.DATE_COLLECT,
          test_hiv.DATE_RECEIVE,
          conf.CONFIRM_CODE
   FROM ohasis_interim.px_test_hiv AS test_hiv
            JOIN ohasis_interim.px_confirm AS conf ON test_hiv.REC_ID = conf.REC_ID
   WHERE test_hiv.REC_ID = ?
     AND test_hiv.TEST_TYPE = 31
     AND test_hiv.TEST_NUM = 1
     AND DATE_COLLECT IS NOT NULL
   )"
   query_ref <- r"(
   SELECT RIGHT(CONFIRM_CODE, 5) AS CTRL_NUM
   FROM ohasis_interim.px_confirm
   WHERE CONFIRM_CODE REGEXP ?
   )"

   # get reference data
   log_info("Opening connections.")
   conn <- ohasis$conn("db")

   log_info("Getting reference data.")
   data_rec     <- dbGetQuery(conn, query_rec, params = rec_id)
   faci_code    <- str_left(data_rec[1,]$CONFIRM_CODE, 3)
   date_receive <- data_rec[1,]$DATE_RECEIVE
   code_year    <- stri_c(faci_code, format(date_receive, "%y"))
   code_month   <- format(date_receive, "%m")

   log_info("Constructing new code.")
   data_ref      <- dbGetQuery(conn, query_ref, params = code_year)
   ctrl_num_curr <- as.integer(data_ref[[1]])
   if (nrow(data_ref) == 0)
      ctrl_num_curr <- 0

   ctrl_num_seq       <- seq(min(ctrl_num_curr), max(ctrl_num_curr))
   ctrl_num_available <- setdiff(ctrl_num_seq, ctrl_num_curr)

   if (length(ctrl_num_available) == 0) {
      log_warn("Unused control number/s available.")
      ctrl_num_ref <- max(ctrl_num_curr) + 1
   } else {
      log_info("No unused control numbers found.")
      ctrl_num_ref <- min(ctrl_num_available)
   }
   log_info("Using next in sequence.")
   ctrl_num_new <- stri_pad_left(ctrl_num_ref, 5, "0")

   confirm_code <- stri_c(sep = "-", code_year, code_month, ctrl_num_new)
   log_success("New Confirmatory Code: {green(confirm_code)}.")

   log_info("Updating relevant tables.")
   dbExecute(conn, r"(UPDATE ohasis_interim.px_confirm SET CONFIRM_CODE = ? WHERE REC_ID = ?)", params = list(confirm_code, rec_id))
   dbExecute(conn, r"(UPDATE ohasis_interim.px_info SET CONFIRMATORY_CODE = ? WHERE REC_ID = ?)", params = list(confirm_code, rec_id))
   update_credentials(rec_id)
   dbDisconnect(conn)
   log_success("Done.")
}

oh_new_patient <- function(faci_id,
                           first = NA_character_,
                           middle = NA_character_,
                           last = NA_character_,
                           suffix = NA_character_,
                           birthdate = NA_character_,
                           sex = NA_character_,
                           confirmatory_code = NA_character_,
                           uic = NA_character_,
                           patient_code = NA_character_,
                           philhealth = NA_character_,
                           mobile = NA_character_,
                           email = NA_character_) {
   db_conn    <- ohasis$conn("db")
   patient_id <- oh_px_id(db_conn, faci_id)
   rec_id     <- oh_rec_id(db_conn, Sys.getenv("OH_USER_ID"))
   sex        <- toupper(sex)
   sex        <- case_when(
      sex %in% c("1", "MALE") ~ "1",
      sex %in% c("2", "FEMALE") ~ "2",
   )
   upload     <- tibble(
      REC_ID            = rec_id,
      PATIENT_ID        = patient_id,
      FACI_ID           = faci_id,
      SUB_FACI_ID       = NA_character_,
      FIRST             = first,
      MIDDLE            = middle,
      LAST              = last,
      SUFFIX            = suffix,
      BIRTHDATE         = birthdate,
      SEX               = sex,
      CONFIRMATORY_CODE = confirmatory_code,
      UIC               = uic,
      PATIENT_CODE      = patient_code,
      PHILHEALTH_NO     = philhealth,
      CLIENT_MOBILE     = mobile,
      CLIENT_EMAIL      = email,
      DISEASE           = "101000",
      MODULE            = "0",
      RECORD_DATE       = format(Sys.time(), "%Y-%m-%d"),
      CREATED_BY        = Sys.getenv("OH_USER_ID"),
      CREATED_AT        = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
   )

   tables           <- list()
   tables$px_record <- list(
      name = "px_record",
      pk   = c("REC_ID", "PATIENT_ID"),
      data = upload %>%
         select(
            REC_ID,
            PATIENT_ID,
            FACI_ID,
            SUB_FACI_ID,
            RECORD_DATE,
            DISEASE,
            MODULE,
            CREATED_BY,
            CREATED_AT,
         )
   )

   tables$px_info <- list(
      name = "px_info",
      pk   = c("REC_ID", "PATIENT_ID"),
      data = upload %>%
         select(
            REC_ID,
            PATIENT_ID,
            CONFIRMATORY_CODE,
            UIC,
            PATIENT_CODE,
            SEX,
            BIRTHDATE,
            CREATED_BY,
            CREATED_AT,
         )
   )

   tables$px_name <- list(
      name = "px_name",
      pk   = c("REC_ID", "PATIENT_ID"),
      data = upload %>%
         select(
            REC_ID,
            PATIENT_ID,
            FIRST,
            MIDDLE,
            LAST,
            CREATED_BY,
            CREATED_AT,
         )
   )

   tables$px_contact <- list(
      name = "px_contact",
      pk   = c("REC_ID", "CONTACT_TYPE"),
      data = upload %>%
         select(
            REC_ID,
            CREATED_BY,
            CREATED_AT,
            CLIENT_MOBILE,
            CLIENT_EMAIL
         ) %>%
         pivot_longer(
            cols      = c(CLIENT_MOBILE, CLIENT_EMAIL),
            names_to  = "CONTACT_TYPE",
            values_to = "CONTACT"
         ) %>%
         mutate(
            CONTACT_TYPE = case_when(
               CONTACT_TYPE == "CLIENT_MOBILE" ~ "1",
               CONTACT_TYPE == "CLIENT_EMAIL" ~ "2",
               TRUE ~ CONTACT_TYPE
            )
         )
   )

   lapply(tables, function(ref, db_conn) {
      log_info("Uploading {green(ref$name)}.")
      table_space <- Id(schema = "ohasis_interim", table = ref$name)
      dbxUpsert(db_conn, table_space, ref$data, ref$pk)
      # dbExecute(db_conn, glue("DELETE FROM ohasis_interim.{ref$name} WHERE REC_ID IN (?)"), params = list(unique(ref$data$REC_ID)))
   }, db_conn)
   dbDisconnect(db_conn)

   return(patient_id)
}

move_art_to_prep <- function(rec_ids) {
   db_sql <- stri_c("UPDATE ohasis_interim.px_record SET MODULE = 6 WHERE REC_ID IN ('", stri_c(collapse = "','", rec_ids), "');")
   lw_sql <- stri_c("DELETE FROM ohasis_warehouse.form_art_bc WHERE REC_ID IN ('", stri_c(collapse = "','", rec_ids), "');")

   db_conn <- ohasis$conn("db")
   dbx::dbxExecute(db_conn, db_sql)
   dbDisconnect(db_conn)

   lw_conn <- ohasis$conn("lw")
   dbx::dbxExecute(lw_conn, lw_sql)
   dbDisconnect(lw_conn)
}

update_art <- function(min, max, exclude) {
   if (missing(max)) {
      max <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
   }

   tables <- list(
      lake      = c(
         "px_pii",
         "px_faci_info",
         "px_key_pop",
         "px_staging",
         "lab_wide",
         "px_vaccine",
         "px_tb_info",
         "px_prophylaxis",
         "px_oi",
         "px_ob",
         "disc_meds",
         "disp_meds"
      ),
      warehouse = "form_art_bc"
   )

   if (!missing(exclude)) {
      tables$lake <- tables$lake[!(tables$lake %in% exclude)]
   }

   lapply(tables$lake, function(table) ohasis$data_factory("lake", table, "upsert", TRUE, from = min, to = max))
   lapply(tables$warehouse, function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE, from = min, to = max))
}

update_hts <- function(min, max, exclude) {
   if (missing(max)) {
      max <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
   }

   tables <- list(
      lake      = c(
         "px_pii",
         "px_faci_info",
         "px_ob",
         "px_hiv_testing",
         "px_consent",
         "px_occupation",
         "px_ofw",
         "px_risk",
         "px_expose_profile",
         "px_test_reason",
         "px_test_refuse",
         "px_test_previous",
         "px_med_profile",
         "px_staging",
         "px_cfbs",
         "px_reach",
         "px_linkage",
         "px_other_service"
      ),
      warehouse = c("form_a", "form_hts")
   )

   if (!missing(exclude)) {
      tables$lake <- tables$lake[!(tables$lake %in% exclude)]
   }

   lapply(tables$lake, function(table) ohasis$data_factory("lake", table, "upsert", TRUE, from = min, to = max))
   lapply(tables$warehouse, function(table) ohasis$data_factory("warehouse", table, "upsert", TRUE, from = min, to = max))
}

oh_batch_newpx <- function(data, id_col) {
   pii_cols <- c(
      'sub_faci_id',
      'confirmatory_code',
      'patient_code',
      'uic',
      'philhealth_no',
      'philsys_id',
      'first',
      'middle',
      'last',
      'suffix',
      'birthdate',
      'age',
      'age_mo',
      'sex',
      'self_ident',
      'self_ident_other',
      'client_email',
      'client_mobile',
      'nationality',
      'civil_status',
      'educ_level',
      'curr_reg',
      'curr_prov',
      'curr_munc',
      'curr_brgy',
      'curr_addr',
      'perm_reg',
      'perm_prov',
      'perm_munc',
      'perm_brgy',
      'perm_addr',
      'birth_reg',
      'birth_prov',
      'birth_munc',
      'birth_brgy',
      'birth_addr',
      'signature',
      'verbal_consent',
      'esig',
      'created_by',
      'created_at',
      'updated_by',
      'updated_at',
      'deleted_by',
      'deleted_at'
   )

   for (col in pii_cols) {
      if (!(col %in% names(data))) {
         data[[col]] <- NA_character_
      }
   }

   new <- data %>%
      filter(!if_all(all_of(pii_cols), ~is.na(.))) %>%
      mutate(
         patient_id = NA_character_,
         created_by = "1300000048",
         created_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      )

   new %<>%
      filter(!is.na(patient_id)) %>%
      bind_rows(
         batch_px_ids(new %>% filter(is.na(patient_id)), patient_id, faci_id, id_col)
      )

   patients <- new %>%
      select(
         patient_id,
         faci_id,
         sub_faci_id,
         confirmatory_code,
         patient_code,
         uic,
         philhealth_no,
         philsys_id,
         first,
         middle,
         last,
         suffix,
         birthdate,
         age,
         age_mo,
         sex,
         self_ident,
         self_ident_other,
         client_email,
         client_mobile,
         nationality,
         civil_status,
         educ_level,
         curr_reg,
         curr_prov,
         curr_munc,
         curr_brgy,
         curr_addr,
         perm_reg,
         perm_prov,
         perm_munc,
         perm_brgy,
         perm_addr,
         birth_reg,
         birth_prov,
         birth_munc,
         birth_brgy,
         birth_addr,
         signature,
         verbal_consent,
         esig,
         created_by,
         created_at,
         updated_by,
         updated_at,
         deleted_by,
         deleted_at,
      ) %>%
      mutate(
         sex              = case_when(
            sex == 'MALE' ~ '1',
            sex == 'FEMALE' ~ '2',
            TRUE ~ sex
         ),
         self_ident       = case_when(
            self_ident == 'MAN' ~ '1',
            self_ident == 'WOMAN' ~ '2',
            self_ident == 'TRANSWOMAN' ~ '3',
            self_ident == 'Q/NB/NC' ~ '3',
            TRUE ~ self_ident
         ),
         self_ident_other = case_when(
            self_ident == 'TRANSWOMAN' ~ self_ident,
            self_ident == 'Q/NB/NC' ~ self_ident,
            TRUE ~ self_ident_other
         ),
         client_mobile    = str_replace_all(client_mobile, "[^[:digit:]]", ""),
         client_mobile    = case_when(
            str_left(client_mobile, 1) == "9" ~ stri_c("0", client_mobile),
            str_left(client_mobile, 2) == "63" ~ str_replace(client_mobile, "^63", "0"),
            TRUE ~ client_mobile
         ),
         client_mobile    = if_else(nchar(client_mobile) == 11, str_c(sep = " ", str_left(client_mobile, 4), str_mid(client_mobile, 5, 3), str_right(client_mobile, 4)), client_mobile, client_mobile)
      )

   db_conn     <- connect('ohasis-live')
   table_space <- Id(schema = "ohasis", table = "patients")
   dbxUpsert(db_conn, table_space, patients, 'patient_id')
   dbDisconnect(db_conn)

   return(new)
}

lake_ref_table <- function(table) {
   log_info("Downloading references.")
   lw_conn <- connect("ohasis-lw")
   data    <- QB$new(lw_conn)$from(stri_c("ohasis_lake.", table))$get()
   dbDisconnect(lw_conn)

   return(data)
}

coalesce_faci_cols <- function(warehouse_table) {
   log_info("Cleaning facility columns.")
   schema  <- "ohasis_warehouse"
   sql_id  <- Id(schema = schema, table = warehouse_table)
   queries <- list()

   sub_notequal_main <- function(faci, sub) {
      set   <- stri_c("`", sub, "` = ''")
      where <- stri_c("`", faci, "` <> LEFT(`", sub, "`, 6) AND `", faci, "` <> ''")
      return(c(set, where))
   }

   use_other_faci_col <- function(main_faci, main_sub, other_faci, other_sub) {
      set   <- stri_c("`", main_faci, "` = `", other_faci, "`, `", main_sub, "` = `", other_sub, "`")
      where <- stri_c("`", main_faci, "` = '' AND `", other_faci, "` <> ''")
      return(c(set, where))
   }

   remove_nulls <- function(col) {
      set   <- stri_c("`", col, "` = ''")
      where <- stri_c("`", col, "` IS NULL")
      return(c(set, where))
   }

   if (warehouse_table %in% c("form_hts", "form_a")) {
      queries <- list(
         remove_nulls("SERVICE_SUB_FACI"),
         remove_nulls("SERVICE_FACI"),
         remove_nulls("CONFIRM_SUB_FACI"),
         remove_nulls("CONFIRM_FACI"),
         remove_nulls("SPECIMEN_SUB_SOURCE"),
         remove_nulls("SPECIMEN_SOURCE"),
         remove_nulls("SUB_FACI_ID"),
         remove_nulls("FACI_ID"),
         sub_notequal_main("SERVICE_FACI", "SERVICE_SUB_FACI"),
         sub_notequal_main("CONFIRM_FACI", "CONFIRM_SUB_FACI"),
         sub_notequal_main("SPECIMEN_SOURCE", "SPECIMEN_SUB_SOURCE"),
         use_other_faci_col("SERVICE_FACI", "SERVICE_SUB_FACI", "SPECIMEN_SOURCE", "SPECIMEN_SUB_SOURCE"),
         use_other_faci_col("SERVICE_FACI", "SERVICE_SUB_FACI", "FACI_ID", "SUB_FACI_ID")
      )
   }

   if (warehouse_table == "form_art_bc") {
      queries <- list(
         remove_nulls("SERVICE_SUB_FACI"),
         remove_nulls("SERVICE_FACI"),
         remove_nulls("SUB_FACI_DISP"),
         remove_nulls("FACI_DISP"),
         remove_nulls("SUB_FACI_ID"),
         remove_nulls("FACI_ID"),
         sub_notequal_main("SERVICE_FACI", "SERVICE_SUB_FACI"),
         sub_notequal_main("FACI_DISP", "SUB_FACI_DISP"),
         use_other_faci_col("SERVICE_FACI", "SERVICE_SUB_FACI", "FACI_DISP", "SUB_FACI_DISP"),
         use_other_faci_col("SERVICE_FACI", "SERVICE_SUB_FACI", "FACI_ID", "SUB_FACI_ID")
      )
   }

   collapse_update <- function(elems, warehouse_table) {
      statement <- stri_c("UPDATE ohasis_warehouse.", warehouse_table, " SET ")

      return(stri_c(statement, elems[[1]], " WHERE ", elems[[2]], ";"))
   }

   if (length(queries) > 0) {
      queries <- lapply(queries, collapse_update, warehouse_table)

      conn     <- connect("ohasis-lw")
      affected <- pblapply(queries, dbExecute, conn = conn)
      dbDisconnect(conn)
   }

   log_success("Done!")
   return(queries)
}

trial_to_live <- function(form, faci_id, min, max) {
   lw_conn <- connect("ohasis-lw")
   rec_ids <- QB$new(lw_conn)$
      select("REC_ID")$
      from("ohasis_interim.px_record")$
      where('FACI_ID', faci_id)$
      where('MODULE', '3')$
      whereBetween("CREATED_AT", c(min, max))$
      get()

   tables                  <- list()
   tables$px_record        <- list(
      name = "px_record",
      pk   = c("REC_ID", "PATIENT_ID"),
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_record WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_info          <- list(
      name = "px_info",
      pk   = c("REC_ID", "PATIENT_ID"),
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_info WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_name          <- list(
      name = "px_name",
      pk   = c("REC_ID", "PATIENT_ID"),
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_name WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_contact       <- list(
      name = "px_contact",
      pk   = c("REC_ID", "CONTACT_TYPE"),
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_contact WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_addr          <- list(
      name = "px_addr",
      pk   = c("REC_ID", "ADDR_TYPE"),
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_addr WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_profile       <- list(
      name = "px_profile",
      pk   = "REC_ID",
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_profile WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_form          <- list(
      name = "px_form",
      pk   = c("REC_ID", "FORM"),
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_form WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_faci          <- list(
      name = "px_faci",
      pk   = c("REC_ID", "SERVICE_TYPE"),
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_faci WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_remarks       <- list(
      name = "px_remarks",
      pk   = c("REC_ID", "REMARK_TYPE"),
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_remarks WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_key_pop       <- list(
      name = "px_key_pop",
      pk   = c("REC_ID", "KP"),
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_key_pop WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_staging       <- list(
      name = "px_staging",
      pk   = "REC_ID",
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_staging WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_labs          <- list(
      name = "px_labs",
      pk   = c("REC_ID", "LAB_TEST"),
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_labs WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_vaccine       <- list(
      name = "px_vaccine",
      pk   = c("REC_ID", "DISEASE_VAX", "VAX_NUM"),
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_vaccine WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_tb            <- list(
      name = "px_tb",
      pk   = "REC_ID",
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_tb WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_tb_ipt        <- list(
      name = "px_tb_ipt",
      pk   = "REC_ID",
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_tb_ipt WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_tb_active     <- list(
      name = "px_tb_active",
      pk   = "REC_ID",
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_tb_active WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_prophylaxis   <- list(
      name = "px_prophylaxis",
      pk   = c("REC_ID", "PROPHYLAXIS"),
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_prophylaxis WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_oi            <- list(
      name = "px_oi",
      pk   = c("REC_ID", "OI"),
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_oi WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_ob            <- list(
      name = "px_ob",
      pk   = "REC_ID",
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_ob WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_medicine      <- list(
      name = "px_medicine",
      pk   = c("REC_ID", "MEDICINE", "DISP_NUM"),
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_medicine WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_medicine_disc <- list(
      name = "px_medicine_disc",
      pk   = c("REC_ID", "MEDICINE"),
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_medicine_disc WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   tables$px_other_service <- list(
      name = "px_other_service",
      pk   = c("REC_ID", "SERVICE"),
      data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_other_service WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
   )
   dbDisconnect(lw_conn)

   db_conn <- connect("ohasis-live")
   pblapply(tables, function(ref, db_conn) {
      table_space <- Id(schema = "ohasis_interim", table = ref$name)
      dbxUpsert(db_conn, table_space, ref$data, ref$pk)
      # dbExecute(db_conn, glue("DELETE FROM ohasis_interim.{ref$name} WHERE REC_ID IN (?)"), params = list(unique(ref$data$REC_ID)))
   }, db_conn)
   dbDisconnect(db_conn)

   update_credentials(tables$px_record$data$REC_ID)

   return(tables)
}

update_idreg <- function(start = NULL) {
   if (!file.exists(Sys.getenv("LOC_IDREG"))) {
      df <- data.frame(
         patient_id = NA_character_,
         central_id = NA_character_,
         created_by = NA_character_,
         created_at = NA_POSIXct_,
         updated_by = NA_character_,
         updated_at = NA_POSIXct_,
         deleted_by = NA_character_,
         deleted_at = NA_POSIXct_
      )
      write_rds(df, Sys.getenv("LOC_IDREG"))
   }

   log_info("Reading Local File")
   idreg <- read_rds(Sys.getenv("LOC_IDREG"))

   loc_snap <- suppress_warnings(max(max(idreg$created_at, na.rm = TRUE), max(idreg$updated_at, na.rm = TRUE), max(idreg$deleted_at, na.rm = TRUE)), 'no non-missing')
   loc_snap <- format(as.POSIXct(ifelse(is.na(loc_snap) | is.infinite(loc_snap), "1970-01-01", loc_snap)), "%Y-%m-%d %H:%M:%S")
   loc_snap <- ifelse(!is.null(start), start, loc_snap)

   # conn_lw <- ohasis$conn("lw")
   # lw_snap <- QB$new(conn_lw)$from("ohasis_warehouse.id_registry")$selectRaw("MAX(SNAPSHOT) AS snap")$get()
   # lw_snap <- QB$new(conn_lw)$from("ohasis_warehouse.id_registry")$max("SNAPSHOT")
   # lw_snap <- lw_snap [1,1]
   # dbDisconnect(conn_lw)

   log_info("Fetching Data")

   conn_lw   <- connect('mariadb-lw')
   new_idreg <- QB$new(conn_lw)$
      from("ohasis_lake.id_registry")$
      where("created_at", ">=", loc_snap, 'or')$
      where("updated_at", ">=", loc_snap, 'or')$
      where("deleted_at", ">=", loc_snap, 'or')$
      get()
   dbDisconnect(conn_lw)

   updated_idreg <- idreg %>%
      anti_join(
         y  = new_idreg,
         by = join_by(patient_id)
      ) %>%
      bind_rows(
         new_idreg
      ) %>%
      filter(
         !is.na(patient_id)
      )

   write_rds(updated_idreg, Sys.getenv("LOC_IDREG"))

   new_rows     <- nrow(updated_idreg) - nrow(idreg)
   updated_rows <- nrow(inner_join(idreg, new_idreg, join_by(patient_id)))
   log_info("New IDs = {red(new_rows)} rows added")
   log_info("Updated IDs = {red(updated_rows)} rows added")
   log_success("Done!")

   return(updated_idreg)
}

update_pending_positives <- function() {
   log_info("Checking for new rows.")
   con  <- connect("ohasis-lw")
   data <- QB$new(con)$from("ohasis_lake.px_hiv_testing")$whereNotNull("T3_RESULT")$where("CONFIRM_RESULT", "like", "4%")$get()
   dbDisconnect(con)

   update <- data %>%
      mutate(
         FINAL_RESULT = case_when(
            str_left(T3_RESULT, 1) == "1" ~ "Positive for HIV Antibodies",
            str_left(T3_RESULT, 1) == "2" ~ "Inconclusive",
         ),
         REMARKS      = case_when(
            str_left(T3_RESULT, 1) == "1" ~ "Client is advised to proceed to the nearest/preferred HIV treatment hub for linkage to management and care.",
            str_left(T3_RESULT, 1) == "2" ~ "To come back after 2-6 weeks for retesting.",
         )
      ) %>%
      filter(!is.na(FINAL_RESULT)) %>%
      select(
         REC_ID,
         FINAL_RESULT,
         REMARKS
      )

   if (nrow(update) > 0) {
      log_info("Payload = {red(formatC(nrow(update), big.mark = ','))} rows.")
      con <- connect("ohasis-live")
      dbxUpsert(con, Id(schema = "ohasis_interim", table = "px_confirm"), update, "REC_ID")
      dbDisconnect(con)

      update_credentials(update$REC_ID)
   } else {
      log_info("No records found.")
   }

   log_success("Done.")
}

disp_update_px_medicine <- function(rec_ids) {
   current <- QB$new(`oh-live`)$
      from("ohasis_interim.px_medicine AS meds")$
      join("ohasis_interim.px_record AS rec", "meds.REC_ID", "=", "rec.REC_ID")$
      join("ohasis_interim.inventory_product AS items", "meds.MEDICINE", "=", "items.ITEM")$
      whereIn("meds.REC_ID", rec_ids)$
      select("meds.*", "items.TYPICAL_BATCH", "rec.RECORD_DATE")$
      get()

   update <- current %>%
      filter(interval(DISP_DATE, RECORD_DATE) / years(1) > 1) %>%
      mutate(
         DISP_DATE   = RECORD_DATE,
         TOTAL_PILLS = if_else(UNIT_BASIS == 1, DISP_TOTAL * coalesce(parse_number(TYPICAL_BATCH), 1), DISP_TOTAL, DISP_TOTAL) + coalesce(MEDICINE_LEFT, 0),
         TOTAL_DAYS  = TOTAL_PILLS / PER_DAY,
         NEXT_DATE   = as.character(as.Date(DISP_DATE) %m+% days(as.integer(TOTAL_DAYS)))
      ) %>%
      select(REC_ID, MEDICINE, DISP_NUM, DISP_DATE, DISP_DATE, NEXT_DATE)

   conn <- connect("ohasis-live")
   dbxUpsert(conn, Id(schema = "ohasis_interim", table = "px_medicine"), update, where_cols = c("REC_ID", "MEDICINE", "DISP_NUM"))
   dbDisconnect(conn)

   update_credentials(unique(update$REC_ID))

   return(update)
}
