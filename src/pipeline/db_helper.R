tracked_select <- function(conn, query, name, params = NULL) {
   # get number of affected rows
   data   <- tibble()
   n_rows <- dbGetQuery(conn, glue(r"(SELECT COUNT(*) FROM ({gsub(';', '', query)}) AS tbl;)"), params = params)
   n_rows <- as.numeric(n_rows[1,])

   # get actual result set
   rs <- dbSendQuery(conn, query, params = params)

   .log_info("Reading {green(name)}.")
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
change_px_id <- function(recid, old_pid, new_pid) {
   db_conn <- ohasis$conn("db")

   upd_by <- Sys.getenv("OH_USER_ID")
   upd_at <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
   dbExecute(
      db_conn,
      r"(UPDATE ohasis_interim.px_name SET PATIENT_ID = ?, UPDATED_BY = ?, UPDATED_AT = ? WHERE REC_ID = ? AND PATIENT_ID = ?;)",
      params = list(new_pid, upd_by, upd_at, recid, old_pid)
   )
   dbExecute(
      db_conn,
      r"(UPDATE ohasis_interim.px_info SET PATIENT_ID = ?, UPDATED_BY = ?, UPDATED_AT = ? WHERE REC_ID = ? AND PATIENT_ID = ?;)",
      params = list(new_pid, upd_by, upd_at, recid, old_pid)
   )
   dbExecute(
      db_conn,
      r"(UPDATE ohasis_interim.px_record SET PATIENT_ID = ?, UPDATED_BY = ?, UPDATED_AT = ? WHERE REC_ID = ? AND PATIENT_ID = ?;)",
      params = list(new_pid, upd_by, upd_at, recid, old_pid)
   )
   dbDisconnect(db_conn)
}

# update UPDATED_*
update_credentials <- function(rec_ids) {
   db_conn <- ohasis$conn("db")

   upd_by <- Sys.getenv("OH_USER_ID")
   upd_at <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
   dbExecute(
      db_conn,
      glue(r"(UPDATE ohasis_interim.px_record SET UPDATED_BY = '{upd_by}', UPDATED_AT = '{upd_at}' WHERE REC_ID IN (?);)"),
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
      cols     <- ifelse(StrLeft(cols, stri_locate_first_fixed(cols, "(")) == "list(",
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
         where     <- ifelse(StrLeft(where, stri_locate_first_fixed(where, "(")) == "list(",
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

dup_faci_id <- function(keep_faci, drop_faci, reason = NA_character_) {
   # get dupes
   dupes <- ohasis$ref_faci %>%
      filter(FACI_ID %in% c(keep_faci, drop_faci)) %>%
      filter(SUB_FACI_ID == "") %>%
      mutate(
         REASON     = reason,
         MAIN_FACI  = keep_faci,
         CREATED_BY = '1300000001',
         CREATED_AT = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      ) %>%
      select(
         MAIN_FACI,
         DUPE_FACI = FACI_ID,
         REASON,
         FACI_NAME,
         FACI_NAME_CLEAN,
         FACI_CODE,
         PUBPRIV,
         LAT,
         LONG,
         EMAIL,
         MOBILE,
         LANDLINE,
         REG       = FACI_PSGC_REG,
         PROV      = FACI_PSGC_PROV,
         MUNC      = FACI_PSGC_MUNC,
         ADDRESS   = FACI_ADDR,
      )

   #  prepare update & select queries queries per table
   sql_update <- list()
   sql_select <- list()
   table_cols <- list(
      "px_cfbs.FACI_ID",
      "px_cfbs.PARTNER_FACI",
      "px_confirm.FACI_ID",
      "px_confirm.SOURCE",
      "px_faci.FACI_ID",
      "px_faci.REFER_BY_ID",
      "px_faci.REFER_TO_ID",
      "px_medicine.FACI_ID",
      "px_medicine_disc.FACI_ID",
      "px_record.FACI_ID",
      "px_test.FACI_ID",
      "px_test_hiv.FACI_ID",
      "referral.REFER_BY_ID",
      "referral.REFER_TO_ID",
      "users.FACI_ID",
      "inventory.FACI_ID",
      "inventory.SOURCE"
   )
   for (table_col in table_cols) {
      pair  <- strsplit(table_col, "\\.")[[1]]
      table <- pair[1]
      col   <- pair[2]

      sql_update[[table_col]] <- paste0("UPDATE ohasis_interim.", table, " SET ", col, " = ? WHERE ", col, " = ?;")

      if (grepl("^px", table))
         sql_select[[table_col]] <- paste0("SELECT DISTINCT REC_ID FROM ohasis_interim.", table, " WHERE ", col, " = ?;")
   }

   # get record ids for those affected
   db_conn <- ohasis$conn("db")
   rec_ids <- data.frame()
   for (query in sql_select) {
      data    <- dbGetQuery(db_conn, query, params = list(drop_faci))
      rec_ids <- rec_ids %>% bind_rows(data) %>% distinct(REC_ID)
   }

   # update records
   for (query in sql_update) {
      dbExecute(db_conn, query, params = list(keep_faci, drop_faci))
   }
   update_credentials(rec_ids$REC_ID)

   # remove duplicate facility from referece data
   dbExecute(db_conn, "DELETE FROM ohasis_interim.facility WHERE FACI_ID = ?;", params = list(drop_faci))

   # log data in facility_duplicates
   dbxUpsert(db_conn,
             Id(schema = "ohasis_interim", table = "facility_duplicates"),
             dupes,
             c("MAIN_FACI", "DUPE_FACI"))
   dbDisconnect(db_conn)
}

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
   faci_code    <- StrLeft(data_rec[1,]$CONFIRM_CODE, 3)
   date_receive <- data_rec[1,]$DATE_RECEIVE
   code_year    <- stri_c(faci_code, format(date_receive, "%y"))
   code_month   <- format(date_receive, "%m")

   log_info("Constructing new code.")
   data_ref           <- dbGetQuery(conn, query_ref, params = code_year)
   ctrl_num_curr      <- as.integer(data_ref[[1]])
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
