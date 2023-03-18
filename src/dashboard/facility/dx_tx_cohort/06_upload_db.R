create_tables <- function(data) {
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

   tables <- list(
      dx_cohort       = list(data = data$dx, pk = "idnum"),
      tx_cohort       = list(data = data$tx, pk = c("art_id", "FACI_ID")),
      prep_cohort     = list(data = data$prep, pk = c("prep_id", "FACI_ID")),
      dx_tx_cascade   = list(data = cascade$dx_tx, pk = c(cascade_ids, "data_src")),
      nr_prep_cascade = list(data = cascade$prep, pk = c("FACI_ID", "curr_age_c1", "curr_age_c2", "kap_type", "sex", "linkage_facility", "indicator")),
      reach           = list(data = data$reach, pk = c("CENTRAL_ID", "FACI_ID")),
      hts             = list(data = data$hts, pk = c("CENTRAL_ID", "FACI_ID"))
   )
   for (table in names(tables)) {
      table_space <- Id(schema = "db_faci", table = table)
      if (dbExistsTable(lw_conn, table_space))
         dbRemoveTable(lw_conn, table_space)

      upload <- tables[[table]]$data
      keys   <- tables[[table]]$pk
      dbCreateTable(lw_conn, table_space, upload)
      dbExecute(lw_conn, stri_c("ALTER TABLE db_faci.", table, " ADD PRIMARY KEY (", stri_c(collapse = ", ", keys), ");"))
      ohasis$upsert(lw_conn, "db_faci", table, upload, keys)
   }

   dbDisconnect(lw_conn)
}

.init <- function(envir = parent.env(environment())) {
   p <- envir
   create_tables(p$data)
}