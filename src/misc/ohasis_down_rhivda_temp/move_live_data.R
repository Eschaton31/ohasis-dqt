rhivda_sql <- function(table) {
   sql <- stri_c("SELECT tbl.* FROM ohasis_interim.", table, " AS tbl JOIN ohasis_interim.px_confirm AS crcl ON tbl.REC_ID = crcl.REC_ID WHERE crcl.CONFIRM_TYPE = 2;")
   return(sql)
}

db_conn <- ohasis$conn("db")
tbls    <- c("px_info", "px_name", "px_record", "px_confirm", "px_test", "px_test_hiv")
queries <- lapply(tbls, rhivda_sql)
upload  <- lapply(queries, dbxSelect, conn = db_conn)
inv     <- dbxSelect(db_conn, "SELECT * FROM ohasis_interim.inventory")
dbDisconnect(db_conn)
names(upload) <- tbls

lw_conn <- ohasis$conn("lw")
dbxUpsert(lw_conn, Id(schema = "ohasis_interim", table = "px_info"), upload$px_info, c("REC_ID", "PATIENT_ID"))
dbxUpsert(lw_conn, Id(schema = "ohasis_interim", table = "px_name"), upload$px_name, c("REC_ID", "PATIENT_ID"))
dbxUpsert(lw_conn, Id(schema = "ohasis_interim", table = "px_record"), upload$px_record, c("REC_ID", "PATIENT_ID"))
dbxUpsert(lw_conn, Id(schema = "ohasis_interim", table = "px_confirm"), upload$px_confirm, "REC_ID")
dbxUpsert(lw_conn, Id(schema = "ohasis_interim", table = "px_test"), upload$px_test, c("REC_ID", "TEST_TYPE", "TEST_NUM"))
dbxUpsert(lw_conn, Id(schema = "ohasis_interim", table = "px_test_hiv"), upload$px_test_hiv, c("REC_ID", "TEST_TYPE", "TEST_NUM"))
dbxUpsert(lw_conn, Id(schema = "ohasis_interim", table = "inventory"), inv, "INVENTORY_ID")
dbDisconnect(lw_conn)
