tracked_select <- function(conn, query, name) {
   # get number of affected rows
   data   <- tibble()
   n_rows <- dbGetQuery(conn, glue(r"(SELECT COUNT(*) FROM ({gsub(';', '', query)}) AS tbl;)"))
   n_rows <- as.numeric(n_rows[1,])

   # get actual result set
   rs <- dbSendQuery(conn, query)

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

   upd_by <- "1300000001"
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

# update UPDATED_*
update_credentials <- function(rec_ids) {
   db_conn <- ohasis$conn("db")

   upd_by <- "1300000001"
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
