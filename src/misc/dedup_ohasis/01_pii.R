lw_conn <- ohasis$conn("lw")

dedup$id_registry <- dbTable(
   lw_conn,
   "ohasis_warehouse",
   "id_registry",
   cols = c("CENTRAL_ID", "PATIENT_ID")
)

reload <- input(
   prompt  = "Do you want to re-download all PIIs?",
   options = c("1" = "yes", "2" = "no"),
   default = "1"
)
if (reload == "1") {
   # dedup$pii <- dbTable(
   #    lw_conn,
   #    "ohasis_lake",
   #    "px_pii",
   #    cols = c(
   #       "PATIENT_ID",
   #       "FIRST",
   #       "MIDDLE",
   #       "LAST",
   #       "UIC",
   #       "CONFIRMATORY_CODE",
   #       "PATIENT_CODE",
   #       "BIRTHDATE",
   #       "PHILSYS_ID",
   #       "PHILHEALTH_NO",
   #       "SEX",
   #       "DELETED_AT",
   #       "SNAPSHOT"
   #    )
   # )

   # build query using previous params
   query <- glue(r"(SELECT * FROM ohasis_lake.px_pii WHERE DELETED_AT IS NULL;)")

   # get number of affected rows
   data   <- tibble()
   n_rows <- dbGetQuery(lw_conn, query)
   n_rows <- as.numeric(n_rows[1,])

   query <- r"(
SELECT DISTINCT PATIENT_ID,
                FIRST,
                MIDDLE,
                LAST,
                UIC,
                CONFIRMATORY_CODE,
                PATIENT_CODE,
                BIRTHDATE,
                PHILSYS_ID,
                PHILHEALTH_NO,
                SEX,
                MONTH(SNAPSHOT) AS UPDATE_MO
                YEAR(SNAPSHOT) AS UPDATE_YR
FROM ohasis_lake.px_pii
WHERE DELETED_AT IS NULL
   )"

   # get actual result set
   rs <- dbSendQuery(conn, query)

   chunk_size <- 1000
   if (n_rows >= chunk_size) {
      # upload in chunks to monitor progress
      n_chunks <- ceiling(n_rows / chunk_size)

      # get progress
      pb_name <- ":current of :total chunks [:bar] (:percent) | ETA: :eta | Elapsed: :elapsed"

      pb <- progress_bar$new(format = pb_name, total = n_chunks, width = 100, clear = FALSE)
      pb$tick(0)

      # fetch in chunks
      for (i in seq_len(n_chunks)) {
         chunk <- dbFetch(rs, chunk_size)
         data  <- bind_rows(data, chunk)
         pb$tick(1)
      }

   }
   dedup$pii <- data
   dbDisconnect(lw_conn)

   cols <- c(
      "FIRST",
      "MIDDLE",
      "LAST",
      "UIC",
      "CONFIRMATORY_CODE",
      "PATIENT_CODE",
      "BIRTHDATE",
      "PHILSYS_ID",
      "PHILHEALTH_NO",
      "SEX"
   )

   for (col in cols) {
      .log_info("Getting latest data for {green(col)}.")
      col_name          <- as.name(col)
      dedup$vars[[col]] <- dedup$pii %>%
         filter(is.na(DELETED_AT), !is.na(!!col_name)) %>%
         left_join(
            y  = dedup$id_registry,
            by = "PATIENT_ID"
         ) %>%
         mutate(
            CENTRAL_ID = if_else(
               condition = is.na(CENTRAL_ID),
               true      = PATIENT_ID,
               false     = CENTRAL_ID
            )
         ) %>%
         arrange(desc(SNAPSHOT)) %>%
         distinct(
            CENTRAL_ID,
            !!col_name
         ) %>%
         rename(
            DATA = 1
         ) %>%
         mutate(
            VAR = col
         ) %>%
         mutate_all(~as.character(.)) %>%
         distinct(CENTRAL_ID, .keep_all = TRUE)
   }

   rm(lw_conn, col_name, col, cols)

   dedup$linelist <- bind_rows(dedup$vars) %>%
      pivot_wider(
         id_cols     = CENTRAL_ID,
         names_from  = VAR,
         values_from = DATA
      )


   dedup$standard <- dedup_prep(
      data         = dedup$linelist %>%
         mutate(
            SUFFIX    = NA_character_,
            BIRTHDATE = if_else(!is.na(BIRTHDATE), as.Date(BIRTHDATE), NA_Date_)
         ),
      name_f       = FIRST,
      name_m       = MIDDLE,
      name_l       = LAST,
      name_s       = SUFFIX,
      uic          = UIC,
      birthdate    = BIRTHDATE,
      code_confirm = CONFIRMATORY_CODE,
      code_px      = PATIENT_CODE,
      phic         = PHILHEALTH_NO,
      philsys      = PHILSYS_ID
   )