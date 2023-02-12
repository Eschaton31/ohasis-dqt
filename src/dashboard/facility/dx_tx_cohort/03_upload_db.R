create_tables <- function(data) {
   lw_conn     <- ohasis$conn("lw")
   table_space <- Id(schema = "db_faci", table = "dx_cohort")
   if (dbExistsTable(lw_conn, table_space))
      dbRemoveTable(lw_conn, table_space)

   # dbCreateTable(lw_conn, table_space, data$dx)
   ohasis$upsert(lw_conn, "harp", "dx_cohort", data$dx, "idnum")
   ohasis$upsert(lw_conn, "harp", "tx_cohort", data$tx, "art_id")
   dbDisconnect(lw_conn)
}

.init <- function(envir = parent.env(environment())) {
   p      <- envir
   create_tables(p$data)
}