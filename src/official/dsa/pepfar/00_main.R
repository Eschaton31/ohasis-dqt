flow_register()
pepfar$steps$`01_load_reqs`$.init(yr = 2023, mo = 3, report = "HFR", envir = pepfar)
pepfar$steps$`02_prepare_tx`$.init(pepfar)
pepfar$steps$`03_prepare_prep`$.init(pepfar)
pepfar$steps$`04_prepare_reach`$.init(pepfar)
pepfar$steps$`05_aggregate_data`$.init(pepfar)
pepfar$steps$`06_conso_flat`$.init(pepfar)

##  MER ------------------------------------------------------------------------
oh_dir       <- file.path("O:/My Drive/Data Sharing/EpiC")
file_initial <- file.path(oh_dir, "RecencyTesting-PreProcess.xlsx")
file_final   <- file.path(oh_dir, "RecencyTesting-PostProcess.xlsx")
file_faci    <- file.path(oh_dir, "OHASIS-FacilityIDs.xlsx")
file_json    <- file.path(oh_dir, "DataStatus.json")

oh_ts <- format(
   as.POSIXct(
      paste0(
         strsplit(ohasis$timestamp, "\\.")[[1]][1], "-",
         strsplit(ohasis$timestamp, "\\.")[[1]][2], "-",
         strsplit(ohasis$timestamp, "\\.")[[1]][3], " ",
         StrLeft(strsplit(ohasis$timestamp, "\\.")[[1]][4], 2), ":",
         substr(strsplit(ohasis$timestamp, "\\.")[[1]][4], 3, 4), ":",
         StrRight(strsplit(ohasis$timestamp, "\\.")[[1]][4], 2)
      )
   ),
   "%Y-%m-%d %H:%M:%S"
)

epic                               <- new.env()
epic$data$json                     <- jsonlite::read_json(file_json)
epic$data$json$`Linelist-TX`       <- list(
   upload_date  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
   version_date = oh_ts,
   coverage     = pepfar$coverage[1:5]
)
epic$data$json$`Linelist-PrEP`     <- list(
   upload_date  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
   version_date = oh_ts,
   coverage     = pepfar$coverage[1:5]
)
epic$data$json$`Linelist-HTS-PREV` <- list(
   upload_date  = format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
   version_date = oh_ts,
   coverage     = pepfar$coverage[1:5]
)

pepfar$ip$EpiC$linelist$tx %>%
   remove_pii() %>%
   write_xlsx(file.path(oh_dir, glue("Linelist-TX.xlsx")))

pepfar$ip$EpiC$linelist$prep %>%
   remove_pii() %>%
   write_xlsx(file.path(oh_dir, glue("Linelist-PrEP.xlsx")))

pepfar$ip$EpiC$linelist$reach %>%
   remove_pii() %>%
   write_xlsx(file.path(oh_dir, glue("Linelist-HTS-PREV.xlsx")))

jsonlite::write_json(epic$data$json, file_json, pretty = TRUE, auto_unbox = TRUE)
