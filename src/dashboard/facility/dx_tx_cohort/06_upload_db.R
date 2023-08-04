create_tables <- function(data, cascade, params) {
   lw_conn <- ohasis$conn("lw")

   cascade_ids <- c(
      "FACI_ID",
      "dx_age_c1",
      "dx_age_c2",
      "curr_age_c1",
      "curr_age_c2",
      "kap_type",
      "sex",
      "mot",
      "linkage_facility",
      "outcome",
      "outcome_new",
      "indicator"
   )

   add_faci_name <- function(data, params) {
      data %<>%
         mutate(
            dashboard_date = as.Date(params$max),
            FACI           = FACI_ID,
            SUB_FACI_ID    = NA_character_
         ) %>%
         ohasis$get_faci(
            list(FACI_NAME = c("FACI", "SUB_FACI_ID")),
            "name",
         )
      return(data)
   }

   data    <- lapply(data, add_faci_name, params)
   cascade <- lapply(cascade, function(data, params) {
      data %<>%
         mutate(
            dashboard_date = as.Date(params$max),
         )
      return(data)
   }, params)
   tables  <- list(
      dx_cohort       = list(data = data$dx, pk = "idnum"),
      tx_cohort       = list(data = data$tx, pk = c("art_id", "FACI_ID")),
      prep_cohort     = list(data = data$prep, pk = c("prep_id", "FACI_ID")),
      dx_tx_cascade   = list(data = cascade$dx_tx, pk = c(cascade_ids, "data_src")),
      nr_prep_cascade = list(data = cascade$prep, pk = c("FACI_ID", "curr_age_c1", "curr_age_c2", "kap_type", "sex", "linkage_facility", "indicator")),
      reach           = list(data = data$reach, pk = c("CENTRAL_ID", "REC_ID")),
      hts             = list(data = data$hts, pk = c("CENTRAL_ID", "REC_ID")),
      db_faci_clients = list(
         data = bind_rows(
            data$dx %>% distinct(CENTRAL_ID, FACI_ID),
            data$tx %>% distinct(CENTRAL_ID, FACI_ID),
            data$prep %>% distinct(CENTRAL_ID, FACI_ID),
            data$reach %>% distinct(CENTRAL_ID, FACI_ID)
         ) %>% distinct_all(),
         pk   = c("CENTRAL_ID", "FACI_ID")
      )
   )
   for (table in names(tables)) {
      log_info("Uploading {green(table)}.")
      table_space <- Id(schema = "db_faci", table = table)
      if (dbExistsTable(lw_conn, table_space))
         dbRemoveTable(lw_conn, table_space)

      upload <- tables[[table]]$data
      keys   <- tables[[table]]$pk
      dbCreateTable(lw_conn, table_space, upload)
      dbExecute(lw_conn, stri_c("ALTER TABLE db_faci.", table, " ADD PRIMARY KEY (", stri_c(collapse = ", ", keys), ");"))
      dbExecute(lw_conn, stri_c("ALTER TABLE db_faci.", table, " ADD INDEX `FACI_ID` (`FACI_ID`);"))
      ohasis$upsert(lw_conn, "db_faci", table, upload, keys)
   }

   dbDisconnect(lw_conn)
}

.init <- function(envir = parent.env(environment())) {
   p <- envir
   create_tables(p$data, p$cascade, p$params)
}