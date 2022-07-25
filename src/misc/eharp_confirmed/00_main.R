eharp    <- list()
eharp$wd <- "H:/Backup/eHARP rHIVda Data"

db_conn          <- ohasis$conn("db")
eharp$oh_ids     <- dbTable(
   db_conn,
   "ohasis_interim",
   "legacy_eb_ids"
)
eharp$px_confirm <- dbTable(
   db_conn,
   "ohasis_interim",
   "px_confirm"
)
dbDisconnect(db_conn)
rm(db_conn)

# corrections
eharp$corr <- list()
invisible(lapply(sheet_names("1hDEUXo0c4dTTuIgvxhWo_sMvpcelCbXWQy9ePjnEMN0"), function(sheet) {
   .GlobalEnv$eharp$corr[[sheet]] <- read_sheet("1hDEUXo0c4dTTuIgvxhWo_sMvpcelCbXWQy9ePjnEMN0", sheet)
}))

noid <- eharp$data$records %>%
   distinct(PATIENT_ID, OH_ID, FACI_ID)

newid <- noid %>%
   filter(is.na(OH_ID))

db_conn <- ohasis$conn("db")
for (i in seq_len(nrow(newid)))
   newid[i, "OH_ID"] <- oh_px_id(db_conn, as.character(newid[i, "FACI_ID"]))

dbDisconnect(db_conn)