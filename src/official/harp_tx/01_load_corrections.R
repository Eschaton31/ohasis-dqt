##  Get the files used for correcting data -------------------------------------

# list of current vars for code cleanup
currEnv <- ls()[ls() != "currEnv"]

# drive path
primary_files <- drive_ls(paste0(nhsss$harp_tx$gdrive$path$primary, ".all/"))
report_files  <- drive_ls(paste0(nhsss$harp_tx$gdrive$path$primary, ohasis$ym, "/Cleaning/"))

# list of correction files
.log_info("Getting list of correction datasets.")

.log_info("Downloading sheets for all reporting periods.")
for (i in seq_len(nrow(primary_files))) {
   corr_id     <- primary_files[i,]$id
   corr_name   <- primary_files[i,]$name
   corr_sheets <- sheet_names(corr_id)

   if (length(corr_sheets) > 1) {
      nhsss$harp_tx$corr[[corr_name]] <- list()
      for (sheet in corr_sheets)
         nhsss$harp_tx$corr[[corr_name]][[sheet]] <- read_sheet(corr_id, sheet)
   } else {
      nhsss$harp_tx$corr[[corr_name]] <- read_sheet(corr_id)
   }
}

# create monthly folder if not exists
.log_info("Downloading sheets for this reporting period.")

if (nrow(report_files) > 0) {
   for (i in seq_len(nrow(report_files))) {
      corr_id                         <- report_files[i,]$id
      corr_name                       <- report_files[i,]$name
      nhsss$harp_tx$corr[[corr_name]] <- read_sheet(corr_id)
   }
}

.log_success("Done!")

# clean-up created objects
rm(list = setdiff(ls(), currEnv))
