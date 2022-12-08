conn <- ohasis$conn("db")
db   <- "ohasis_interim"
data <- list()
tbls <- c("px_record", "px_name", "px_info", "id_registry")
for (tbl in tbls) {
   data[[tbl]] <- dbTable(conn, db, tbl)
}

dbDisconnect(conn)