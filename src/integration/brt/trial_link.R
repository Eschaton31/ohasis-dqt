lw_conn           <- ohasis$conn("lw")
db_name           <- "ohasis_interim"
rec_ids           <- dbxSelect(lw_conn, r"(SELECT REC_ID FROM ohasis_interim.px_record WHERE FACI_ID = '050002' AND MODULE = 3)")
tables            <- list()
tables$px_record  <- list(
   name = "px_record",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_record WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_info    <- list(
   name = "px_info",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_info WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_name    <- list(
   name = "px_name",
   pk   = c("REC_ID", "PATIENT_ID"),
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_name WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_contact    <- list(
   name = "px_contact",
   pk   = c("REC_ID", "CONTACT_TYPE"),
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_contact WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_addr    <- list(
   name = "px_addr",
   pk   = c("REC_ID", "ADDR_TYPE"),
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_addr WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_profile <- list(
   name = "px_profile",
   pk   = "REC_ID",
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_profile WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_form    <- list(
   name = "px_form",
   pk   = c("REC_ID", "FORM"),
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_form WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_faci    <- list(
   name = "px_faci",
   pk   = c("REC_ID", "SERVICE_TYPE"),
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_faci WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_remarks    <- list(
   name = "px_remarks",
   pk   = c("REC_ID", "REMARK_TYPE"),
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_remarks WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_key_pop    <- list(
   name = "px_key_pop",
   pk   = c("REC_ID", "KP"),
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_key_pop WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_staging    <- list(
   name = "px_staging",
   pk   = "REC_ID",
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_staging WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_labs    <- list(
   name = "px_labs",
   pk   = c("REC_ID", "LAB_TEST"),
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_labs WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_vaccine    <- list(
   name = "px_vaccine",
   pk   = c("REC_ID", "DISEASE_VAX", "VAX_NUM"),
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_vaccine WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_tb    <- list(
   name = "px_tb",
   pk   = "REC_ID",
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_tb WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_tb_ipt    <- list(
   name = "px_tb_ipt",
   pk   = "REC_ID",
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_tb_ipt WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_tb_active    <- list(
   name = "px_tb_active",
   pk   = "REC_ID",
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_tb_active WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_prophylaxis    <- list(
   name = "px_prophylaxis",
   pk   = c("REC_ID", "PROPHYLAXIS"),
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_prophylaxis WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_oi    <- list(
   name = "px_oi",
   pk   = c("REC_ID", "OI"),
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_oi WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_ob    <- list(
   name = "px_ob",
   pk   = "REC_ID",
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_ob WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_medicine    <- list(
   name = "px_medicine",
   pk   = c("REC_ID", "MEDICINE", "DISP_NUM"),
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_medicine WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_medicine_disc    <- list(
   name = "px_medicine_disc",
   pk   = c("REC_ID", "MEDICINE"),
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_medicine_disc WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
tables$px_other_service    <- list(
   name = "px_other_service",
   pk   = c("REC_ID", "SERVICE"),
   data = dbxSelect(lw_conn, "SELECT * FROM ohasis_interim.px_other_service WHERE REC_ID IN (?)", params = list(rec_ids$REC_ID))
)
dbDisconnect(lw_conn)



db_conn <- ohasis$conn("db")
dbxDelete(
   db_conn,
   Id(schema = "ohasis_interim", table = "px_medicine"),
   tables$px_record$data %>% select(REC_ID),
   batch_size = 1000
)
lapply(tables, function(ref, db_conn) {
   log_info("Uploading {green(ref$name)}.")
   table_space <- Id(schema = "ohasis_interim", table = ref$name)
   dbxUpsert(db_conn, table_space, ref$data, ref$pk)
   # dbExecute(db_conn, glue("DELETE FROM ohasis_interim.{ref$name} WHERE REC_ID IN (?)"), params = list(unique(ref$data$REC_ID)))
}, db_conn)
dbDisconnect(db_conn)
