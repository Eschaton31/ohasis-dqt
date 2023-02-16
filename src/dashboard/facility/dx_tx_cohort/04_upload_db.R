create_tables <- function(data) {
   lw_conn <- ohasis$conn("lw")

   for (table in c("dx_cohort", "tx_cohort", "dx_cascade", "tx_cascade")) {
      table_space <- Id(schema = "db_faci", table = table)
      if (dbExistsTable(lw_conn, table_space))
         dbRemoveTable(lw_conn, table_space)
   }

   # dbCreateTable(lw_conn, table_space, data$dx)
   ohasis$upsert(lw_conn, "db_faci", "dx_cohort", data$dx, "idnum")
   ohasis$upsert(lw_conn, "db_faci", "tx_cohort", data$tx, c("art_id", "FACI_ID"))

   cascade_ids <- c(
      "FACI_ID",
      "dx_age_c1",
      "dx_age_c2",
      "tx_age_c1",
      "tx_age_c2",
      "kap_type",
      "sex",
      "mot",
      "linkage_facility",
      "outcome",
      "outcome_new",
      "indicator"
   )

   ohasis$upsert(lw_conn, "db_faci", "dx_casacde", cascade$dx, cascade_ids)
   ohasis$upsert(lw_conn, "db_faci", "tx_casacde", cascade$tx, cascade_ids)
   dbDisconnect(lw_conn)
}

.init <- function(envir = parent.env(environment())) {
   p <- envir
   create_tables(p$data)
}