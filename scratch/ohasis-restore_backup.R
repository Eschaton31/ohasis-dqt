affected <- QB$new(`oh-live`)$
   select("rec.REC_ID")$
   from("ohasis_interim.px_record AS rec")$
   join("ohasis_interim.px_faci AS service", "rec.REC_ID", "=", "service.REC_ID")$
   where("rec.FACI_ID", "<>", "service.FACI_ID")$
   where("service.FACI_ID", "=", "100003")$
   distinct()$
   get()


batches <- chunk_df(affected, 100000)

get_tables <- function() {
   data <- QB$new(`oh-live`)$
      select("TABLE_NAME")$
      from("information_schema.tables")$
      where("TABLE_SCHEMA", "ohasis_interim")$
      where("TABLE_NAME", "regexp", "^px_+")$
      get()

   return(data[[1]])
}

get_pk <- function(table) {
   data <- QB$new(`oh-live`)$
      select("COLUMN_NAME")$
      from('information_schema.KEY_COLUMN_USAGE')$
      where('TABLE_SCHEMA', 'ohasis_interim')$
      where('CONSTRAINT_NAME', 'PRIMARY')$
      where('TABLE_NAME', table)$
      get()

   return(data[[1]])
}

tables    <- get_tables()
tables    <- tables[tables != "px_pii"]
tables    <- list("px_record", "px_faci", "px_test")
pk        <- lapply(tables, get_pk)
names(pk) <- tables


total   <- length(batches)
pb_name <- ":current of :total steps [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed"
pb      <- progress_bar$new(format = pb_name, total = total, width = 100, clear = FALSE)
pb$tick(0)

data        <- as.list(seq_len(length(tables)))
data        <- lapply(data, tibble)
data        <- lapply(data, slice, -1)
names(data) <- tables

conn <- connect("local")
for (batch in batches) {
   for (table in tables) {
      data[[table]] <- bind_rows(
         data[[table]],
         QB$new(conn)$
            from(stri_c("ohasis_interim.", table))$
            whereIn("REC_ID", batch$REC_ID)$
            get()
      )

   }
   pb$tick(1)
}
dbDisconnect(conn)


upload           <- list()
upload$px_record <- list(
   name = "px_record",
   pk   = pk$px_record,
   data = data$px_record %>% select(-`1L`)
)
upload$px_faci   <- list(
   name = "px_faci",
   pk   = pk$px_faci,
   data = data$px_faci %>% select(-`2L`)
)
upload$px_test   <- list(
   name = "px_test",
   pk   = pk$px_test,
   data = data$px_test %>% select(-`3L`)
)

db_conn <- connect("ohasis-live")
lapply(upload, function(ref, db_conn) {
   log_info("Uploading {green(ref$name)}.")
   table_space <- Id(schema = "ohasis_interim", table = ref$name)
   dbxUpsert(db_conn, table_space, ref$data, ref$pk)
}, db_conn)
dbDisconnect(db_conn)

#### check px_test multiple t1 records
px_test <- QB$new(`oh-live`)$from('ohasis_interim.px_test')$whereIn("REC_ID", affected$REC_ID)$where("TEST_TYPE", "10")$get()

delete <- px_test %>%
   filter(FACI_ID == "100003") %>%
   inner_join(y = px_test %>% filter(FACI_ID != "100003") %>% select(REC_ID), join_by(REC_ID))

remain <- px_test %>%
   filter(FACI_ID != "100003") %>%
   filter(REC_ID %in% delete$REC_ID)

delete$REC_ID %>% write_clip()

#### check those who did not have t0 before
px_test <- QB$new(`oh-lw`)$from('ohasis_lake.px_hiv_testing')$whereIn("REC_ID", affected$REC_ID)$whereNull("T0_DATE")$get()







db_conn <- connect("ohasis-live")
restore <-