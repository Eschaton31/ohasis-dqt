tracked_select <- function (conn, query, name) {
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